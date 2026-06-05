#include "lightkv/server.h"
#include "lightkv/db_impl.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <cstring>
#include <cstdio>
#include <string>
#include <unordered_map>
#include <vector>
#include <sstream>
#include <chrono>
#include <atomic>
#include <algorithm>
#include <thread>
#include <ctime>
#include <functional>
#include <cmath>
#include <set>
#include <queue>
#include <mutex>
#include <condition_variable>

#ifdef __APPLE__
#include <sys/event.h>
#else
#include <sys/epoll.h>
#endif

namespace lightkv {

enum class ConnType { kTCP, kHTTP };

struct Connection {
    int fd;
    ConnType type;
    std::string recv_buf;
    std::string send_buf;
    bool closed;
    bool authenticated;  // true if no auth required or AUTH succeeded

    // Multi-threaded support
    std::mutex resp_mutex;
    std::queue<std::string> pending_responses;  // responses from worker threads
    bool has_pending_work = false;  // true if a command is being processed by worker
};

// ─── RESP Protocol Helpers ───
static std::string resp_ok() { return "+OK\r\n"; }
static std::string resp_nil() { return "$-1\r\n"; }
static std::string resp_integer(int64_t n) { return ":" + std::to_string(n) + "\r\n"; }
static std::string resp_error(const std::string& msg) { return "-" + msg + "\r\n"; }
static std::string resp_bulk_string(const std::string& s) {
    return "$" + std::to_string(s.size()) + "\r\n" + s + "\r\n";
}
static std::string resp_empty_array() { return "*0\r\n"; }
static std::string resp_array(const std::vector<std::string>& arr) {
    std::string r = "*" + std::to_string(arr.size()) + "\r\n";
    for (auto& s : arr) r += resp_bulk_string(s);
    return r;
}

// ─── TTL Metadata key prefix ───
static const char TTL_MAGIC = '\x01';
static const char TTL_SEP = '\x00';
static std::string ttl_key(const std::string& key) {
    std::string r;
    r += TTL_MAGIC;
    r += "_ttl_";
    r += TTL_SEP;
    r += key;
    return r;
}
static int64_t now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}
static int64_t now_sec() {
    return std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

// ─── Type Prefix Helpers ───
static const char HASH_PREFIX[] = "\x02_hash_";
static const char LIST_PREFIX[] = "\x03_list_";
static const char SET_PREFIX[]  = "\x04_set_";
static const char ZSET_PREFIX[] = "\x05_zset_";

static std::string hash_field_key(const std::string& name, const std::string& field) {
    return std::string(HASH_PREFIX) + name + ":" + field;
}
static std::string hash_meta_key(const std::string& name) {
    return std::string(HASH_PREFIX) + name + ":__meta__";
}

static std::string list_idx_key(const std::string& name, int64_t idx) {
    return std::string(LIST_PREFIX) + name + ":idx:" + std::to_string(idx);
}
static std::string list_meta_key(const std::string& name) {
    return std::string(LIST_PREFIX) + name + ":__meta__";
}

static std::string set_blob_key(const std::string& name) {
    return std::string(SET_PREFIX) + name;
}

// ZSet key helpers: use score-padded key for range scan ordering
// zset:{name}:member → score (for ZSCORE)
// zset:{name}:score:{padded_score}:{member} → "" (for ZRANGE by score)
// zset:{name}:__meta__ → count
static std::string zset_member_key(const std::string& name, const std::string& member) {
    return std::string(ZSET_PREFIX) + name + ":member:" + member;
}
static std::string zset_score_key(const std::string& name, double score, const std::string& member) {
    // Pad score to 20 chars with leading zeros for lexicographic ordering
    // Use format: sign + 15 digits + . + 4 digits
    char buf[32];
    if (score < 0) {
        snprintf(buf, sizeof(buf), "-%020.4f", -score);
    } else {
        snprintf(buf, sizeof(buf), "+%020.4f", score);
    }
    return std::string(ZSET_PREFIX) + name + ":score:" + buf + ":" + member;
}
static std::string zset_meta_key(const std::string& name) {
    return std::string(ZSET_PREFIX) + name + ":__meta__";
}
static std::string zset_score_prefix(const std::string& name) {
    return std::string(ZSET_PREFIX) + name + ":score:";
}

// Parse set blob: format is "len\0elem1\0elem2\0..."
static std::vector<std::string> parse_set_blob(const std::string& blob) {
    std::vector<std::string> result;
    if (blob.empty()) return result;
    size_t pos = 0;
    while (pos < blob.size()) {
        size_t end = blob.find('\0', pos);
        if (end == std::string::npos) {
            if (pos < blob.size()) result.push_back(blob.substr(pos));
            break;
        }
        result.push_back(blob.substr(pos, end - pos));
        pos = end + 1;
    }
    return result;
}

static std::string encode_set_blob(const std::vector<std::string>& elems) {
    std::string blob;
    for (auto& e : elems) {
        blob += e;
        blob += '\0';
    }
    return blob;
}

// Check if a key is an internal type key
static bool is_internal_key(const std::string& key) {
    if (key.empty()) return false;
    if (key[0] == TTL_MAGIC) return true;
    if (key.size() >= 7 && key.substr(0, 7) == std::string(HASH_PREFIX)) return true;
    if (key.size() >= 7 && key.substr(0, 7) == std::string(LIST_PREFIX)) return true;
    if (key.size() >= 6 && key.substr(0, 6) == std::string(SET_PREFIX)) return true;
    if (key.size() >= 7 && key.substr(0, 7) == std::string(ZSET_PREFIX)) return true;
    return false;
}

// ─── Thread Pool ───
struct WorkerTask {
    int conn_fd;
    std::vector<std::string> args;
    std::function<std::string(const std::vector<std::string>&)> handler;
};

class ThreadPool {
public:
    ThreadPool(int num_threads, std::function<void(int, std::string)> on_complete)
        : on_complete_(std::move(on_complete)), stop_(false), wakeup_fd_(-1) {
#ifdef __APPLE__
        int pipefd[2];
        if (::pipe(pipefd) == 0) {
            wakeup_fd_ = pipefd[1];  // write end
            wakeup_read_fd_ = pipefd[0];  // read end
            // Make both ends non-blocking
            int flags_w = ::fcntl(wakeup_fd_, F_GETFL, 0);
            ::fcntl(wakeup_fd_, F_SETFL, flags_w | O_NONBLOCK);
            int flags_r = ::fcntl(wakeup_read_fd_, F_GETFL, 0);
            ::fcntl(wakeup_read_fd_, F_SETFL, flags_r | O_NONBLOCK);
        }
#else
        wakeup_fd_ = ::eventfd(0, EFD_NONBLOCK);
        wakeup_read_fd_ = wakeup_fd_;
#endif
        for (int i = 0; i < num_threads; ++i) {
            workers_.emplace_back([this]() { worker_loop(); });
        }
    }

    ~ThreadPool() {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            stop_ = true;
            cv_.notify_all();
        }
        for (auto& t : workers_) t.join();
        if (wakeup_fd_ >= 0) ::close(wakeup_fd_);
#ifdef __APPLE__
        if (wakeup_read_fd_ >= 0) ::close(wakeup_read_fd_);
#endif
    }

    int wakeup_read_fd() const { return wakeup_read_fd_; }

    void submit(WorkerTask task) {
        std::unique_lock<std::mutex> lock(mutex_);
        queue_.push(std::move(task));
        cv_.notify_one();
    }

    void wakeup_event_loop() {
        if (wakeup_fd_ >= 0) {
            char buf = 1;
            ::write(wakeup_fd_, &buf, 1);
        }
    }

    void clear_wakeup() {
        if (wakeup_read_fd_ >= 0) {
            char buf[64];
            // Drain the wakeup pipe
            while (::read(wakeup_read_fd_, buf, sizeof(buf)) > 0) {}
        }
    }

private:
    void worker_loop() {
        while (true) {
            WorkerTask task;
            {
                std::unique_lock<std::mutex> lock(mutex_);
                cv_.wait(lock, [this]() { return stop_ || !queue_.empty(); });
                if (stop_ && queue_.empty()) return;
                task = std::move(queue_.front());
                queue_.pop();
            }

            // Execute command
            std::string resp = task.handler(task.args);

            // Notify event loop
            on_complete_(task.conn_fd, std::move(resp));

            // Wake up the event loop so it can drain responses
            wakeup_event_loop();
        }
    }

    std::vector<std::thread> workers_;
    std::queue<WorkerTask> queue_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::function<void(int, std::string)> on_complete_;
    bool stop_;
    int wakeup_fd_;      // write end (pipe or eventfd)
    int wakeup_read_fd_; // read end (for event loop to monitor)
};

class Server::Impl {
public:
    Impl(DB* db, const ServerOptions& opts)
        : db_(db), opts_(opts), running_(false),
          tcp_fd_(-1), http_fd_(-1), event_fd_(-1) {
        start_time_ = std::chrono::steady_clock::now();
        InitCommandTable();
    }

    bool auth_required() const { return !opts_.requirepass.empty(); }

    ~Impl() {
        Stop();
        if (pool_) {
            pool_.reset();
        }
    }

    void Run() {
        running_.store(true);

        // Initialize thread pool if worker_threads > 0
        if (opts_.worker_threads > 0) {
            pool_ = std::make_unique<ThreadPool>(opts_.worker_threads,
                [this](int fd, std::string resp) { on_worker_complete(fd, std::move(resp)); });
            fprintf(stderr, "[LightKV] Worker pool started with %d threads\n", opts_.worker_threads);
        }

#ifdef __APPLE__
        event_fd_ = kqueue();
#else
        event_fd_ = ::epoll_create1(0);
#endif
        if (event_fd_ < 0) { perror("event_fd"); return; }

        if (opts_.enable_tcp) {
            tcp_fd_ = create_server_socket(opts_.tcp_host, opts_.tcp_port);
            if (tcp_fd_ >= 0) {
                add_event(tcp_fd_, true, false);
                fprintf(stderr, "[LightKV] TCP listening on %s:%d\n",
                        opts_.tcp_host.c_str(), opts_.tcp_port);
            }
        }

        if (opts_.enable_http) {
            http_fd_ = create_server_socket(opts_.http_host, opts_.http_port);
            if (http_fd_ >= 0) {
                add_event(http_fd_, true, false);
                fprintf(stderr, "[LightKV] HTTP listening on %s:%d\n",
                        opts_.http_host.c_str(), opts_.http_port);
            }
        }

        if (tcp_fd_ < 0 && http_fd_ < 0) return;

        // Register wakeup fd for read events
        int wakeup_read = pool_ ? pool_->wakeup_read_fd() : -1;
        if (wakeup_read >= 0) {
            add_event(wakeup_read, true, false);
        }

        while (running_.load()) {
            // Drain worker responses before waiting for events
            drain_responses();

#ifdef __APPLE__
            struct kevent events[opts_.max_connections];
            struct timespec ts;
            ts.tv_sec = opts_.epoll_timeout_ms / 1000;
            ts.tv_nsec = (opts_.epoll_timeout_ms % 1000) * 1000000;
            int n = kevent(event_fd_, nullptr, 0, events, opts_.max_connections, &ts);
#else
            struct epoll_event events[opts_.max_connections];
            int n = ::epoll_wait(event_fd_, events, opts_.max_connections, opts_.epoll_timeout_ms);
#endif
            if (n < 0) { if (errno == EINTR) continue; perror("event_wait"); break; }
            if (n == 0) continue;

            for (int i = 0; i < n; ++i) {
#ifdef __APPLE__
                int fd = static_cast<int>(events[i].ident);
                bool readable = events[i].filter == EVFILT_READ;
                bool writable = events[i].filter == EVFILT_WRITE;
#else
                int fd = events[i].data.fd;
                bool readable = events[i].events & EPOLLIN;
                bool writable = events[i].events & EPOLLOUT;
#endif
                // Check if this is the wakeup fd
                if (pool_ && fd == wakeup_read && readable) {
                    pool_->clear_wakeup();
                    continue;
                }
                if (fd == tcp_fd_ && readable) {
                    accept_connection(fd, ConnType::kTCP);
                } else if (fd == http_fd_ && readable) {
                    accept_connection(fd, ConnType::kHTTP);
                } else if (fd != tcp_fd_ && fd != http_fd_) {
                    handle_client(fd, readable, writable);
                }
            }

            // After processing events, drain any pending worker responses
            if (pool_) drain_responses();
        }

        close_all_connections();
        if (tcp_fd_ >= 0) ::close(tcp_fd_);
        if (http_fd_ >= 0) ::close(http_fd_);
        if (event_fd_ >= 0) ::close(event_fd_);
        tcp_fd_ = http_fd_ = event_fd_ = -1;
    }

    void Stop() { running_.store(false); }

private:
    using CommandHandler = std::string (Impl::*)(const std::vector<std::string>&);
    std::unordered_map<std::string, CommandHandler> cmd_table_;

    void InitCommandTable() {
        cmd_table_["PING"]     = &Impl::handle_ping;
        cmd_table_["QUIT"]     = &Impl::handle_quit;
        cmd_table_["CONFIG"]   = &Impl::handle_config;
        cmd_table_["SET"]      = &Impl::handle_set;
        cmd_table_["GET"]      = &Impl::handle_get;
        cmd_table_["DEL"]      = &Impl::handle_del;
        cmd_table_["DELRANGE"] = &Impl::handle_delrange;
        cmd_table_["STATS"]    = &Impl::handle_stats;
        cmd_table_["DBSIZE"]   = &Impl::handle_dbsize;

        // String extension commands
        cmd_table_["INCR"]      = &Impl::handle_incr;
        cmd_table_["DECR"]      = &Impl::handle_decr;
        cmd_table_["INCRBY"]    = &Impl::handle_incrby;
        cmd_table_["DECRBY"]    = &Impl::handle_decrby;
        cmd_table_["INCRBYFLOAT"] = &Impl::handle_incrbyfloat;
        cmd_table_["MSET"]      = &Impl::handle_mset;
        cmd_table_["MGET"]      = &Impl::handle_mget;
        cmd_table_["SETEX"]     = &Impl::handle_setex;
        cmd_table_["PSETEX"]    = &Impl::handle_psetex;
        cmd_table_["SETNX"]     = &Impl::handle_setnx;
        cmd_table_["GETSET"]    = &Impl::handle_getset;
        cmd_table_["APPEND"]    = &Impl::handle_append;
        cmd_table_["STRLEN"]    = &Impl::handle_strlen;
        cmd_table_["GETRANGE"]  = &Impl::handle_getrange;

        // General commands
        cmd_table_["EXISTS"]    = &Impl::handle_exists;
        cmd_table_["EXPIRE"]    = &Impl::handle_expire;
        cmd_table_["PEXPIRE"]   = &Impl::handle_pexpire;
        cmd_table_["EXPIRETIME"] = &Impl::handle_expiretime;
        cmd_table_["TTL"]       = &Impl::handle_ttl;
        cmd_table_["PTTL"]      = &Impl::handle_pttl;
        cmd_table_["PERSIST"]   = &Impl::handle_persist;
        cmd_table_["TYPE"]      = &Impl::handle_type;
        cmd_table_["RENAME"]    = &Impl::handle_rename;
        cmd_table_["RENAMENX"]  = &Impl::handle_renamenx;
        cmd_table_["KEYS"]      = &Impl::handle_keys;
        cmd_table_["SCAN"]      = &Impl::handle_scan;
        cmd_table_["RANDOMKEY"] = &Impl::handle_randomkey;

        // P1: Hash commands
        cmd_table_["HSET"]      = &Impl::handle_hset;
        cmd_table_["HGET"]      = &Impl::handle_hget;
        cmd_table_["HMSET"]     = &Impl::handle_hmset;
        cmd_table_["HMGET"]     = &Impl::handle_hmget;
        cmd_table_["HGETALL"]   = &Impl::handle_hgetall;
        cmd_table_["HDEL"]      = &Impl::handle_hdel;
        cmd_table_["HEXISTS"]   = &Impl::handle_hexists;
        cmd_table_["HLEN"]      = &Impl::handle_hlen;
        cmd_table_["HKEYS"]     = &Impl::handle_hkeys;
        cmd_table_["HVALS"]     = &Impl::handle_hvals;
        cmd_table_["HINCRBY"]   = &Impl::handle_hincrby;
        cmd_table_["HSTRLEN"]   = &Impl::handle_hstrlen;

        // P1: List commands
        cmd_table_["LPUSH"]     = &Impl::handle_lpush;
        cmd_table_["RPUSH"]     = &Impl::handle_rpush;
        cmd_table_["LPOP"]      = &Impl::handle_lpop;
        cmd_table_["RPOP"]      = &Impl::handle_rpop;
        cmd_table_["LRANGE"]    = &Impl::handle_lrange;
        cmd_table_["LLEN"]      = &Impl::handle_llen;
        cmd_table_["LINDEX"]    = &Impl::handle_lindex;
        cmd_table_["LSET"]      = &Impl::handle_lset;
        cmd_table_["LTRIM"]     = &Impl::handle_ltrim;
        cmd_table_["LREM"]      = &Impl::handle_lrem;

        // P1: Set commands
        cmd_table_["SADD"]      = &Impl::handle_sadd;
        cmd_table_["SREM"]      = &Impl::handle_srem;
        cmd_table_["SMEMBERS"]  = &Impl::handle_smembers;
        cmd_table_["SISMEMBER"] = &Impl::handle_sismember;
        cmd_table_["SCARD"]     = &Impl::handle_scard;
        cmd_table_["SPOP"]      = &Impl::handle_spop;
        cmd_table_["SRANDMEMBER"] = &Impl::handle_srandmember;
        cmd_table_["SMOVE"]     = &Impl::handle_smove;

        // P2: Bitmap commands
        cmd_table_["SETBIT"]    = &Impl::handle_setbit;
        cmd_table_["GETBIT"]    = &Impl::handle_getbit;
        cmd_table_["BITCOUNT"]  = &Impl::handle_bitcount;
        cmd_table_["BITPOS"]    = &Impl::handle_bitpos;

        // P2: HyperLogLog commands
        cmd_table_["PFADD"]     = &Impl::handle_pfadd;
        cmd_table_["PFCOUNT"]   = &Impl::handle_pfcount;
        cmd_table_["PFMERGE"]   = &Impl::handle_pfmerge;

        // P2: ZSet commands
        cmd_table_["ZADD"]      = &Impl::handle_zadd;
        cmd_table_["ZREM"]      = &Impl::handle_zrem;
        cmd_table_["ZSCORE"]    = &Impl::handle_zscore;
        cmd_table_["ZRANGE"]    = &Impl::handle_zrange;
        cmd_table_["ZCARD"]     = &Impl::handle_zcard;
        cmd_table_["ZCOUNT"]    = &Impl::handle_zcount;
        cmd_table_["ZRANGEBYSCORE"] = &Impl::handle_zrangebyscore;
        cmd_table_["ZREVRANGE"] = &Impl::handle_zrevrange;

        // P2: Geo commands
        cmd_table_["GEOADD"]    = &Impl::handle_geoadd;
        cmd_table_["GEOPOS"]    = &Impl::handle_geopos;
        cmd_table_["GEODIST"]   = &Impl::handle_geodist;
    }

    // ─── TTL Management ───

    // Check and expire a key. Returns true if key was expired.
    bool expire_if_needed(const std::string& key) {
        std::string expiry;
        ReadOptions ro;
        Status s = db_->Get(ro, ttl_key(key), &expiry);
        if (!s.ok()) return false;
        int64_t expiry_ms = std::stoll(expiry);
        if (now_ms() >= expiry_ms) {
            // Key expired: delete it and the ttl metadata
            WriteOptions wo;
            db_->Delete(wo, key);
            db_->Delete(wo, ttl_key(key));
            return true;
        }
        return false;
    }

    int64_t get_ttl_ms(const std::string& key) {
        std::string expiry;
        ReadOptions ro;
        Status s = db_->Get(ro, ttl_key(key), &expiry);
        if (!s.ok()) return -2; // No expiry
        if (expire_if_needed(key)) return -2; // Already expired
        int64_t expiry_ms = std::stoll(expiry);
        int64_t remaining = expiry_ms - now_ms();
        return remaining < 0 ? -2 : remaining;
    }

    // ─── RESP Parsing ───

    bool parse_resp_array(const std::string& buf, size_t& out_len, size_t& out_end) {
        if (buf.empty() || buf[0] != '*') return false;
        size_t cr = buf.find("\r\n");
        if (cr == std::string::npos) return false;
        int count = std::stoi(buf.substr(1, cr - 1));
        out_len = static_cast<size_t>(count);
        out_end = cr + 2;
        return true;
    }

    bool parse_resp_bulk(const std::string& buf, size_t pos, std::string& out, size_t& next_pos) {
        if (pos >= buf.size() || buf[pos] != '$') return false;
        size_t cr = buf.find("\r\n", pos);
        if (cr == std::string::npos) return false;
        int len = std::stoi(buf.substr(pos + 1, cr - pos - 1));
        if (len < 0) { out.clear(); next_pos = cr + 2; return true; }
        size_t data_start = cr + 2;
        size_t data_end = data_start + len + 2;
        if (data_end > buf.size()) return false;
        out = buf.substr(data_start, len);
        next_pos = data_end;
        return true;
    }

    // ─── TCP Protocol ───

    void process_tcp(Connection& conn) {
        while (true) {
            size_t array_end, array_len;
            if (!parse_resp_array(conn.recv_buf, array_len, array_end)) break;

            size_t pos = array_end;
            std::vector<std::string> args;
            bool complete = true;
            for (size_t i = 0; i < array_len; ++i) {
                std::string arg;
                size_t next;
                if (!parse_resp_bulk(conn.recv_buf, pos, arg, next)) {
                    complete = false;
                    break;
                }
                args.push_back(arg);
                pos = next;
            }
            if (!complete) break;

            conn.recv_buf.erase(0, pos);

            if (pool_) {
                // Dispatch to worker pool
                dispatch_to_worker(conn, args);
            } else {
                // Single-threaded mode: execute directly
                std::string resp = handle_tcp_command(conn, args);
                if (!resp.empty()) queue_response(conn, resp);
            }
        }
    }

    void dispatch_to_worker(Connection& conn, const std::vector<std::string>& args) {
        if (args.empty()) {
            queue_response(conn, resp_error("ERR empty command"));
            return;
        }

        std::string cmd = args[0];
        for (auto& c : cmd) c = static_cast<char>(toupper(c));

        // Allow AUTH and PING even when not authenticated
        if (cmd != "AUTH" && cmd != "PING" && !conn.authenticated) {
            queue_response(conn, resp_error("NOAUTH Authentication required"));
            return;
        }

        // Handle AUTH specially (must execute in event loop thread)
        if (cmd == "AUTH") {
            std::string resp = handle_auth(conn, args);
            queue_response(conn, resp);
            return;
        }

        auto it = cmd_table_.find(cmd);
        if (it != cmd_table_.end()) {
            // Capture handler and submit to pool
            auto handler = [this, handler = it->second](const std::vector<std::string>& a) -> std::string {
                return (this->*handler)(a);
            };
            WorkerTask task;
            task.conn_fd = conn.fd;
            task.args = args;
            task.handler = std::move(handler);
            pool_->submit(std::move(task));
            conn.has_pending_work = true;
        } else {
            queue_response(conn, resp_error("ERR unknown command '" + args[0] + "'"));
        }
    }

    std::string handle_tcp_command(Connection& conn, const std::vector<std::string>& args) {
        if (args.empty()) return resp_error("ERR empty command");

        std::string cmd = args[0];
        for (auto& c : cmd) c = static_cast<char>(toupper(c));

        // Allow AUTH and PING even when not authenticated
        if (cmd != "AUTH" && cmd != "PING" && !conn.authenticated) {
            return resp_error("NOAUTH Authentication required");
        }

        // Handle AUTH specially (needs Connection reference)
        if (cmd == "AUTH") {
            return handle_auth(conn, args);
        }

        auto it = cmd_table_.find(cmd);
        if (it != cmd_table_.end()) {
            return (this->*(it->second))(args);
        }

        return resp_error("ERR unknown command '" + args[0] + "'");
    }

    // ─── Command Handlers ───

    std::string handle_ping(const std::vector<std::string>&) {
        return "+PONG\r\n";
    }

    std::string handle_quit(const std::vector<std::string>&) {
        return "+OK\r\n";
    }

    std::string handle_auth(Connection& conn, const std::vector<std::string>& args) {
        if (args.size() != 2)
            return resp_error("ERR wrong number of arguments for 'auth' command");

        if (!auth_required()) {
            return resp_error("ERR Client sent AUTH, but no password is set");
        }

        if (args[1] == opts_.requirepass) {
            conn.authenticated = true;
            return resp_ok();
        }

        return resp_error("ERR invalid password");
    }

    std::string handle_config(const std::vector<std::string>& args) {
        if (args.size() < 2)
            return resp_error("ERR wrong number of arguments for 'config' command");

        std::string subcmd = args[1];
        for (auto& c : subcmd) c = static_cast<char>(toupper(c));

        if (subcmd == "GET") {
            if (args.size() != 3)
                return resp_error("ERR wrong number of arguments for 'config get' command");
            std::string param = args[2];
            for (auto& c : param) c = static_cast<char>(toupper(c));

            if (param == "REQUIREPASS") {
                std::vector<std::string> result;
                result.push_back("requirepass");
                result.push_back(auth_required() ? "(protected)" : "");
                return resp_array(result);
            }
            return resp_empty_array();
        }

        if (subcmd == "SET") {
            if (args.size() != 4)
                return resp_error("ERR wrong number of arguments for 'config set' command");
            std::string param = args[2];
            for (auto& c : param) c = static_cast<char>(toupper(c));

            if (param == "REQUIREPASS") {
                opts_.requirepass = args[3];
                return resp_ok();
            }
            return resp_error("ERR Unsupported CONFIG parameter: " + args[2]);
        }

        return resp_error("ERR Unknown CONFIG subcommand: " + args[1]);
    }

    std::string handle_set(const std::vector<std::string>& args) {
        if (args.size() < 3) return resp_error("ERR wrong number of arguments for 'set' command");
        WriteOptions wo;
        Status s = db_->Put(wo, args[1], args[2]);

        // Handle optional PX/EX arguments for TTL
        if (s.ok() && args.size() >= 4) {
            for (size_t i = 3; i + 1 < args.size(); i += 2) {
                std::string opt = args[i];
                for (auto& c : opt) c = toupper(c);
                if (opt == "EX" || opt == "EXAT") {
                    int64_t sec = std::stoll(args[i + 1]);
                    int64_t expiry_ms = (opt == "EX") ? now_ms() + sec * 1000 : sec * 1000;
                    s = db_->Put(wo, ttl_key(args[1]), std::to_string(expiry_ms));
                } else if (opt == "PX" || opt == "PXAT") {
                    int64_t ms = std::stoll(args[i + 1]);
                    int64_t expiry_ms = (opt == "PX") ? now_ms() + ms : ms;
                    s = db_->Put(wo, ttl_key(args[1]), std::to_string(expiry_ms));
                }
            }
        }

        return s.ok() ? resp_ok() : resp_error(s.ToString());
    }

    std::string handle_get(const std::vector<std::string>& args) {
        if (args.size() < 2) return resp_error("ERR wrong number of arguments for 'get' command");
        expire_if_needed(args[1]);
        std::string value;
        ReadOptions ro;
        Status s = db_->Get(ro, args[1], &value);
        if (s.ok()) return resp_bulk_string(value);
        if (s.IsNotFound()) return resp_nil();
        return resp_error(s.ToString());
    }

    std::string handle_del(const std::vector<std::string>& args) {
        if (args.size() < 2) return resp_error("ERR wrong number of arguments for 'del' command");
        int64_t count = 0;
        WriteOptions wo;
        for (size_t i = 1; i < args.size(); ++i) {
            expire_if_needed(args[i]);
            Status s = db_->Delete(wo, args[i]);
            if (s.ok()) { count++; }
            // Also delete TTL metadata
            db_->Delete(wo, ttl_key(args[i]));
        }
        return resp_integer(count);
    }

    std::string handle_delrange(const std::vector<std::string>& args) {
        if (args.size() < 3) return resp_error("ERR wrong number of arguments for 'delrange' command");
        WriteOptions wo;
        Status s = db_->DeleteRange(wo, args[1], args[2]);
        return s.ok() ? resp_integer(1) : resp_error(s.ToString());
    }

    std::string handle_stats(const std::vector<std::string>&) {
        auto stats = static_cast<DBImpl*>(db_)->GetStats();
        std::vector<std::string> arr;
        arr.push_back("total_writes");       arr.push_back(std::to_string(stats.total_writes));
        arr.push_back("total_reads");        arr.push_back(std::to_string(stats.total_reads));
        arr.push_back("total_deletes");      arr.push_back(std::to_string(stats.total_deletes));
        arr.push_back("total_flushes");      arr.push_back(std::to_string(stats.total_flushes));
        arr.push_back("total_compactions");  arr.push_back(std::to_string(stats.total_compactions));
        arr.push_back("memtable_size");      arr.push_back(std::to_string(stats.memtable_size));
        arr.push_back("pending_deletes");    arr.push_back(std::to_string(stats.pending_deletes));
        return resp_array(arr);
    }

    std::string handle_dbsize(const std::vector<std::string>&) {
        // Approximate by scanning
        return resp_integer(0);
    }

    // ─── String Extension Commands ───

    std::string handle_incr(const std::vector<std::string>& args) {
        if (args.size() < 2) return resp_error("ERR wrong number of arguments for 'incr' command");
        int64_t new_val;
        WriteOptions wo;
        Status s = db_->Increment(wo, args[1], 1, &new_val);
        return s.ok() ? resp_integer(new_val) : resp_error(s.ToString());
    }

    std::string handle_decr(const std::vector<std::string>& args) {
        if (args.size() < 2) return resp_error("ERR wrong number of arguments for 'decr' command");
        int64_t new_val;
        WriteOptions wo;
        Status s = db_->Increment(wo, args[1], -1, &new_val);
        return s.ok() ? resp_integer(new_val) : resp_error(s.ToString());
    }

    std::string handle_incrby(const std::vector<std::string>& args) {
        if (args.size() < 3) return resp_error("ERR wrong number of arguments for 'incrby' command");
        int64_t delta = std::stoll(args[2]);
        int64_t new_val;
        WriteOptions wo;
        Status s = db_->Increment(wo, args[1], delta, &new_val);
        return s.ok() ? resp_integer(new_val) : resp_error(s.ToString());
    }

    std::string handle_decrby(const std::vector<std::string>& args) {
        if (args.size() < 3) return resp_error("ERR wrong number of arguments for 'decrby' command");
        int64_t delta = std::stoll(args[2]);
        int64_t new_val;
        WriteOptions wo;
        Status s = db_->Increment(wo, args[1], -delta, &new_val);
        return s.ok() ? resp_integer(new_val) : resp_error(s.ToString());
    }

    std::string handle_incrbyfloat(const std::vector<std::string>& args) {
        if (args.size() < 3) return resp_error("ERR wrong number of arguments for 'incrbyfloat' command");
        double delta = std::stod(args[2]);
        std::string value;
        ReadOptions ro;
        Status s = db_->Get(ro, args[1], &value);
        double current = 0.0;
        if (s.ok()) {
            current = std::stod(value);
        } else if (!s.IsNotFound()) {
            return resp_error(s.ToString());
        }
        double new_val = current + delta;
        WriteOptions wo;
        s = db_->Put(wo, args[1], std::to_string(new_val));
        return s.ok() ? resp_bulk_string(std::to_string(new_val)) : resp_error(s.ToString());
    }

    std::string handle_mset(const std::vector<std::string>& args) {
        if (args.size() < 3 || (args.size() - 1) % 2 != 0)
            return resp_error("ERR wrong number of arguments for 'mset' command");
        WriteOptions wo;
        for (size_t i = 1; i + 1 < args.size(); i += 2) {
            Status s = db_->Put(wo, args[i], args[i + 1]);
            if (!s.ok()) return resp_error(s.ToString());
        }
        return resp_ok();
    }

    std::string handle_mget(const std::vector<std::string>& args) {
        if (args.size() < 2) return resp_error("ERR wrong number of arguments for 'mget' command");
        std::vector<std::string> result;
        ReadOptions ro;
        for (size_t i = 1; i < args.size(); ++i) {
            expire_if_needed(args[i]);
            std::string value;
            Status s = db_->Get(ro, args[i], &value);
            if (s.ok()) {
                result.push_back(value);
            } else {
                result.push_back(""); // Will be encoded as nil below
            }
        }
        // Build array with nil for missing keys
        std::string r = "*" + std::to_string(result.size()) + "\r\n";
        for (size_t i = 0; i < result.size(); ++i) {
            std::string& val = result[i];
            if (val.empty()) {
                // Check if key actually exists or is empty string
                std::string check;
                Status cs = db_->Get(ro, args[i + 1], &check);
                if (cs.IsNotFound()) {
                    r += resp_nil();
                } else {
                    r += resp_bulk_string(val);
                }
            } else {
                r += resp_bulk_string(val);
            }
        }
        return r;
    }

    std::string handle_setex(const std::vector<std::string>& args) {
        if (args.size() < 4) return resp_error("ERR wrong number of arguments for 'setex' command");
        int64_t seconds = std::stoll(args[2]);
        WriteOptions wo;
        Status s = db_->Put(wo, args[1], args[3]);
        if (!s.ok()) return resp_error(s.ToString());
        int64_t expiry_ms = now_ms() + seconds * 1000;
        s = db_->Put(wo, ttl_key(args[1]), std::to_string(expiry_ms));
        return s.ok() ? resp_ok() : resp_error(s.ToString());
    }

    std::string handle_psetex(const std::vector<std::string>& args) {
        if (args.size() < 4) return resp_error("ERR wrong number of arguments for 'psetex' command");
        int64_t ms = std::stoll(args[2]);
        WriteOptions wo;
        Status s = db_->Put(wo, args[1], args[3]);
        if (!s.ok()) return resp_error(s.ToString());
        int64_t expiry_ms = now_ms() + ms;
        s = db_->Put(wo, ttl_key(args[1]), std::to_string(expiry_ms));
        return s.ok() ? resp_ok() : resp_error(s.ToString());
    }

    std::string handle_setnx(const std::vector<std::string>& args) {
        if (args.size() < 3) return resp_error("ERR wrong number of arguments for 'setnx' command");
        ReadOptions ro;
        std::string existing;
        Status s = db_->Get(ro, args[1], &existing);
        if (s.ok()) return resp_integer(0);
        WriteOptions wo;
        s = db_->Put(wo, args[1], args[2]);
        return s.ok() ? resp_integer(1) : resp_error(s.ToString());
    }

    std::string handle_getset(const std::vector<std::string>& args) {
        if (args.size() < 3) return resp_error("ERR wrong number of arguments for 'getset' command");
        expire_if_needed(args[1]);
        std::string old_value;
        ReadOptions ro;
        Status s = db_->Get(ro, args[1], &old_value);
        WriteOptions wo;
        Status ps = db_->Put(wo, args[1], args[2]);
        if (!ps.ok()) return resp_error(ps.ToString());
        if (s.ok()) return resp_bulk_string(old_value);
        return resp_nil();
    }

    std::string handle_append(const std::vector<std::string>& args) {
        if (args.size() < 3) return resp_error("ERR wrong number of arguments for 'append' command");
        expire_if_needed(args[1]);
        std::string value;
        ReadOptions ro;
        Status s = db_->Get(ro, args[1], &value);
        value += args[2];
        WriteOptions wo;
        s = db_->Put(wo, args[1], value);
        return s.ok() ? resp_integer(value.size()) : resp_error(s.ToString());
    }

    std::string handle_strlen(const std::vector<std::string>& args) {
        if (args.size() < 2) return resp_error("ERR wrong number of arguments for 'strlen' command");
        expire_if_needed(args[1]);
        std::string value;
        ReadOptions ro;
        Status s = db_->Get(ro, args[1], &value);
        if (s.ok()) return resp_integer(value.size());
        if (s.IsNotFound()) return resp_integer(0);
        return resp_error(s.ToString());
    }

    std::string handle_getrange(const std::vector<std::string>& args) {
        if (args.size() < 4) return resp_error("ERR wrong number of arguments for 'getrange' command");
        expire_if_needed(args[1]);
        std::string value;
        ReadOptions ro;
        Status s = db_->Get(ro, args[1], &value);
        if (s.IsNotFound()) return resp_bulk_string("");
        if (!s.ok()) return resp_error(s.ToString());

        int start = std::stoi(args[2]);
        int end = std::stoi(args[3]);
        int len = static_cast<int>(value.size());

        // Handle negative indices
        if (start < 0) start = std::max(0, len + start);
        if (end < 0) end = len + end;
        end = std::min(end, len - 1);

        if (start > end || start >= len) return resp_bulk_string("");

        std::string sub = value.substr(start, end - start + 1);
        return resp_bulk_string(sub);
    }

    // ─── General Commands ───

    std::string handle_exists(const std::vector<std::string>& args) {
        if (args.size() < 2) return resp_error("ERR wrong number of arguments for 'exists' command");
        int64_t count = 0;
        ReadOptions ro;
        for (size_t i = 1; i < args.size(); ++i) {
            expire_if_needed(args[i]);
            if (db_->Exists(ro, args[i])) count++;
        }
        return resp_integer(count);
    }

    std::string handle_expire(const std::vector<std::string>& args) {
        if (args.size() < 3) return resp_error("ERR wrong number of arguments for 'expire' command");
        ReadOptions ro;
        if (!db_->Exists(ro, args[1])) return resp_integer(0);
        int64_t seconds = std::stoll(args[2]);
        int64_t expiry_ms = now_ms() + seconds * 1000;
        WriteOptions wo;
        Status s = db_->Put(wo, ttl_key(args[1]), std::to_string(expiry_ms));
        return s.ok() ? resp_integer(1) : resp_error(s.ToString());
    }

    std::string handle_pexpire(const std::vector<std::string>& args) {
        if (args.size() < 3) return resp_error("ERR wrong number of arguments for 'pexpire' command");
        ReadOptions ro;
        if (!db_->Exists(ro, args[1])) return resp_integer(0);
        int64_t ms = std::stoll(args[2]);
        int64_t expiry_ms = now_ms() + ms;
        WriteOptions wo;
        Status s = db_->Put(wo, ttl_key(args[1]), std::to_string(expiry_ms));
        return s.ok() ? resp_integer(1) : resp_error(s.ToString());
    }

    std::string handle_expiretime(const std::vector<std::string>& args) {
        if (args.size() < 2) return resp_error("ERR wrong number of arguments for 'expiretime' command");
        std::string expiry;
        ReadOptions ro;
        Status s = db_->Get(ro, ttl_key(args[1]), &expiry);
        if (!s.ok()) return resp_integer(-1);
        int64_t expiry_ms = std::stoll(expiry);
        return resp_integer(expiry_ms / 1000);
    }

    std::string handle_ttl(const std::vector<std::string>& args) {
        if (args.size() < 2) return resp_error("ERR wrong number of arguments for 'ttl' command");
        int64_t remaining_ms = get_ttl_ms(args[1]);
        if (remaining_ms < 0) return resp_integer(remaining_ms);
        return resp_integer(remaining_ms / 1000);
    }

    std::string handle_pttl(const std::vector<std::string>& args) {
        if (args.size() < 2) return resp_error("ERR wrong number of arguments for 'pttl' command");
        return resp_integer(get_ttl_ms(args[1]));
    }

    std::string handle_persist(const std::vector<std::string>& args) {
        if (args.size() < 2) return resp_error("ERR wrong number of arguments for 'persist' command");
        // Check if key exists first
        ReadOptions ro;
        if (!db_->Exists(ro, args[1])) return resp_integer(0);
        // Check if TTL metadata exists
        std::string expiry;
        Status s = db_->Get(ro, ttl_key(args[1]), &expiry);
        if (!s.ok()) return resp_integer(0); // Has no TTL
        WriteOptions wo;
        db_->Delete(wo, ttl_key(args[1]));
        return resp_integer(1);
    }

    std::string handle_type(const std::vector<std::string>& args) {
        if (args.size() < 2) return resp_error("ERR wrong number of arguments for 'type' command");
        expire_if_needed(args[1]);
        ReadOptions ro;
        // Check if it's a Hash
        std::string hmeta;
        if (db_->Get(ro, hash_meta_key(args[1]), &hmeta).ok()) return "+hash\r\n";
        // Check if it's a List
        std::string lmeta;
        if (db_->Get(ro, list_meta_key(args[1]), &lmeta).ok()) return "+list\r\n";
        // Check if it's a Set
        std::string sblob;
        if (db_->Get(ro, set_blob_key(args[1]), &sblob).ok()) return "+set\r\n";
        // Check if it's a ZSet
        std::string zmeta;
        if (db_->Get(ro, zset_meta_key(args[1]), &zmeta).ok()) return "+zset\r\n";
        // Check if it's a String
        if (db_->Exists(ro, args[1])) return "+string\r\n";
        return "+none\r\n";
    }

    std::string handle_rename(const std::vector<std::string>& args) {
        if (args.size() < 3) return resp_error("ERR wrong number of arguments for 'rename' command");
        WriteOptions wo;
        Status s = db_->Rename(wo, args[1], args[2]);
        if (!s.ok()) return resp_error(s.ToString());

        // Also rename TTL metadata
        std::string expiry;
        ReadOptions ro;
        if (db_->Get(ro, ttl_key(args[1]), &expiry).ok()) {
            db_->Put(wo, ttl_key(args[2]), expiry);
            db_->Delete(wo, ttl_key(args[1]));
        }
        return resp_ok();
    }

    std::string handle_renamenx(const std::vector<std::string>& args) {
        if (args.size() < 3) return resp_error("ERR wrong number of arguments for 'renamenx' command");
        ReadOptions ro;
        if (db_->Exists(ro, args[2])) return resp_integer(0);
        WriteOptions wo;
        Status s = db_->Rename(wo, args[1], args[2]);
        if (!s.ok()) return resp_error(s.ToString());
        // Also rename TTL metadata
        std::string expiry;
        if (db_->Get(ro, ttl_key(args[1]), &expiry).ok()) {
            db_->Put(wo, ttl_key(args[2]), expiry);
            db_->Delete(wo, ttl_key(args[1]));
        }
        return resp_integer(1);
    }

    std::string handle_keys(const std::vector<std::string>& args) {
        if (args.size() < 2) return resp_error("ERR wrong number of arguments for 'keys' command");
        std::string pattern = args[1];
        std::vector<std::string> result;
        ReadOptions ro;
        std::vector<std::pair<std::string, std::string>> scan_results;
        Status s = db_->Scan(ro, "", 100000, &scan_results);
        if (!s.ok()) return resp_error(s.ToString());

        for (const auto& pr : scan_results) {
            const std::string& key = pr.first;
            // Skip internal TTL keys
            if (is_internal_key(key)) continue;
            expire_if_needed(key);

            if (pattern == "*") {
                result.push_back(key);
            } else if (pattern.size() > 0 && pattern.back() == '*') {
                std::string prefix = pattern.substr(0, pattern.size() - 1);
                if (key.compare(0, prefix.size(), prefix) == 0) {
                    result.push_back(key);
                }
            } else if (key == pattern) {
                result.push_back(key);
            }
        }
        return resp_array(result);
    }

    std::string handle_scan(const std::vector<std::string>& args) {
        // SCAN cursor [MATCH pattern] [COUNT count]
        size_t idx = 1;
        if (idx >= args.size()) return resp_error("ERR wrong number of arguments for 'scan' command");
        uint64_t cursor = std::stoull(args[idx++]);

        std::string pattern = "*";
        int count = 10;

        while (idx < args.size()) {
            std::string opt = args[idx++];
            for (auto& c : opt) c = toupper(c);
            if (opt == "MATCH" && idx < args.size()) {
                pattern = args[idx++];
            } else if (opt == "COUNT" && idx < args.size()) {
                count = std::stoi(args[idx++]);
            } else {
                break;
            }
        }

        std::vector<std::pair<std::string, std::string>> all_results;
        ReadOptions ro;
        db_->Scan(ro, "", 100000, &all_results);

        // Skip internal keys and filter by pattern
        std::vector<std::string> matched;
        for (size_t i = cursor; i < all_results.size() && static_cast<int>(matched.size()) < count; ++i) {
            const std::string& key = all_results[i].first;
            if (is_internal_key(key)) continue;
            if (pattern == "*") {
                matched.push_back(key);
            } else if (pattern.size() > 0 && pattern.back() == '*') {
                std::string prefix = pattern.substr(0, pattern.size() - 1);
                if (key.compare(0, prefix.size(), prefix) == 0) matched.push_back(key);
            } else if (key == pattern) {
                matched.push_back(key);
            }
        }

        uint64_t next_cursor = cursor + matched.size();
        if (next_cursor >= all_results.size()) next_cursor = 0;

        std::string r = "*2\r\n";
        r += resp_bulk_string(std::to_string(next_cursor));
        r += resp_array(matched);
        return r;
    }

    std::string handle_randomkey(const std::vector<std::string>&) {
        std::vector<std::pair<std::string, std::string>> results;
        ReadOptions ro;
        db_->Scan(ro, "", 1000, &results);
        std::vector<std::string> keys;
        for (const auto& pr : results) {
            if (is_internal_key(pr.first)) continue;
            keys.push_back(pr.first);
        }
        if (keys.empty()) return resp_nil();
        return resp_bulk_string(keys[keys.size() / 2]);
    }

    // ═══════════════════════════════════════════════════════════════
    // P1: Hash Commands (KV encoding: hash:{name}:{field})
    // ═══════════════════════════════════════════════════════════════

    std::string handle_hset(const std::vector<std::string>& args) {
        if (args.size() < 4) return resp_error("ERR wrong number of arguments for 'hset' command");
        WriteOptions wo;
        int64_t count = 0;
        std::string meta;
        ReadOptions ro;
        db_->Get(ro, hash_meta_key(args[1]), &meta);
        int64_t field_count = meta.empty() ? 0 : std::stoll(meta);

        for (size_t i = 2; i + 1 < args.size(); i += 2) {
            std::string fkey = hash_field_key(args[1], args[i]);
            std::string existing;
            if (!db_->Get(ro, fkey, &existing).ok()) count++;
            db_->Put(wo, fkey, args[i + 1]);
        }
        db_->Put(wo, hash_meta_key(args[1]), std::to_string(field_count + count));
        return resp_integer(count);
    }

    std::string handle_hget(const std::vector<std::string>& args) {
        if (args.size() < 3) return resp_error("ERR wrong number of arguments for 'hget' command");
        std::string value;
        ReadOptions ro;
        Status s = db_->Get(ro, hash_field_key(args[1], args[2]), &value);
        if (s.ok()) return resp_bulk_string(value);
        return resp_nil();
    }

    std::string handle_hmset(const std::vector<std::string>& args) {
        if (args.size() < 4 || (args.size() - 2) % 2 != 0)
            return resp_error("ERR wrong number of arguments for 'hmset' command");
        WriteOptions wo;
        for (size_t i = 2; i + 1 < args.size(); i += 2) {
            db_->Put(wo, hash_field_key(args[1], args[i]), args[i + 1]);
        }
        return resp_ok();
    }

    std::string handle_hmget(const std::vector<std::string>& args) {
        if (args.size() < 3) return resp_error("ERR wrong number of arguments for 'hmget' command");
        std::vector<std::string> result;
        ReadOptions ro;
        for (size_t i = 2; i < args.size(); ++i) {
            std::string value;
            Status s = db_->Get(ro, hash_field_key(args[1], args[i]), &value);
            if (s.ok()) result.push_back(value);
            else result.push_back(""); // nil placeholder
        }
        std::string r = "*" + std::to_string(result.size()) + "\r\n";
        for (size_t i = 0; i < result.size(); ++i) {
            if (result[i].empty()) {
                std::string check;
                Status cs = db_->Get(ro, hash_field_key(args[1], args[i + 2]), &check);
                if (cs.IsNotFound()) r += resp_nil();
                else r += resp_bulk_string(result[i]);
            } else {
                r += resp_bulk_string(result[i]);
            }
        }
        return r;
    }

    std::string handle_hgetall(const std::vector<std::string>& args) {
        if (args.size() < 2) return resp_error("ERR wrong number of arguments for 'hgetall' command");
        std::string prefix = hash_field_key(args[1], "");
        std::string meta_k = hash_meta_key(args[1]);
        std::vector<std::pair<std::string, std::string>> results;
        ReadOptions ro;
        db_->Scan(ro, prefix, 100000, &results);

        std::vector<std::string> arr;
        for (auto& pr : results) {
            if (pr.first == meta_k) continue;
            std::string full_key = pr.first;
            size_t field_start = prefix.size();
            if (full_key.size() > field_start) {
                arr.push_back(full_key.substr(field_start));
                arr.push_back(pr.second);
            }
        }
        return resp_array(arr);
    }

    std::string handle_hdel(const std::vector<std::string>& args) {
        if (args.size() < 3) return resp_error("ERR wrong number of arguments for 'hdel' command");
        int64_t count = 0;
        WriteOptions wo;
        ReadOptions ro;
        for (size_t i = 2; i < args.size(); ++i) {
            std::string fkey = hash_field_key(args[1], args[i]);
            std::string existing;
            if (db_->Get(ro, fkey, &existing).ok()) {
                db_->Delete(wo, fkey);
                count++;
            }
        }
        // Update meta count
        if (count > 0) {
            std::string meta;
            db_->Get(ro, hash_meta_key(args[1]), &meta);
            int64_t field_count = meta.empty() ? 0 : std::stoll(meta);
            int64_t new_count = field_count - count;
            if (new_count <= 0) {
                db_->Delete(wo, hash_meta_key(args[1]));
            } else {
                db_->Put(wo, hash_meta_key(args[1]), std::to_string(new_count));
            }
        }
        return resp_integer(count);
    }

    std::string handle_hexists(const std::vector<std::string>& args) {
        if (args.size() < 3) return resp_error("ERR wrong number of arguments for 'hexists' command");
        ReadOptions ro;
        return resp_integer(db_->Exists(ro, hash_field_key(args[1], args[2])) ? 1 : 0);
    }

    std::string handle_hlen(const std::vector<std::string>& args) {
        if (args.size() < 2) return resp_error("ERR wrong number of arguments for 'hlen' command");
        std::string meta;
        ReadOptions ro;
        Status s = db_->Get(ro, hash_meta_key(args[1]), &meta);
        if (s.ok()) return resp_integer(std::stoll(meta));
        // Fallback: count fields by scan (exclude meta key)
        std::string prefix = hash_field_key(args[1], "");
        std::string meta_k = hash_meta_key(args[1]);
        std::vector<std::pair<std::string, std::string>> results;
        db_->Scan(ro, prefix, 100000, &results);
        int64_t count = 0;
        for (auto& pr : results) {
            if (pr.first != meta_k) count++;
        }
        return resp_integer(count);
    }

    std::string handle_hkeys(const std::vector<std::string>& args) {
        if (args.size() < 2) return resp_error("ERR wrong number of arguments for 'hkeys' command");
        std::string prefix = hash_field_key(args[1], "");
        std::string meta_k = hash_meta_key(args[1]);
        std::vector<std::pair<std::string, std::string>> results;
        ReadOptions ro;
        db_->Scan(ro, prefix, 100000, &results);
        std::vector<std::string> keys;
        for (auto& pr : results) {
            if (pr.first == meta_k) continue;
            std::string full_key = pr.first;
            size_t field_start = prefix.size();
            if (full_key.size() > field_start) keys.push_back(full_key.substr(field_start));
        }
        return resp_array(keys);
    }

    std::string handle_hvals(const std::vector<std::string>& args) {
        if (args.size() < 2) return resp_error("ERR wrong number of arguments for 'hvals' command");
        std::string prefix = hash_field_key(args[1], "");
        std::string meta_k = hash_meta_key(args[1]);
        std::vector<std::pair<std::string, std::string>> results;
        ReadOptions ro;
        db_->Scan(ro, prefix, 100000, &results);
        std::vector<std::string> vals;
        for (auto& pr : results) {
            if (pr.first == meta_k) continue;
            vals.push_back(pr.second);
        }
        return resp_array(vals);
    }

    std::string handle_hincrby(const std::vector<std::string>& args) {
        if (args.size() < 4) return resp_error("ERR wrong number of arguments for 'hincrby' command");
        int64_t delta;
        try {
            delta = std::stoll(args[3]);
        } catch (...) {
            return resp_error("ERR value is not an integer or out of range");
        }
        std::string fkey = hash_field_key(args[1], args[2]);
        std::string value;
        ReadOptions ro;
        Status s = db_->Get(ro, fkey, &value);
        int64_t current = 0;
        if (s.ok()) {
            try {
                current = std::stoll(value);
            } catch (...) {
                return resp_error("ERR hash value is not an integer");
            }
        } else if (!s.IsNotFound()) {
            return resp_error(s.ToString());
        }
        int64_t new_val = current + delta;
        WriteOptions wo;
        s = db_->Put(wo, fkey, std::to_string(new_val));
        return s.ok() ? resp_integer(new_val) : resp_error(s.ToString());
    }

    std::string handle_hstrlen(const std::vector<std::string>& args) {
        if (args.size() < 3) return resp_error("ERR wrong number of arguments for 'hstrlen' command");
        std::string value;
        ReadOptions ro;
        Status s = db_->Get(ro, hash_field_key(args[1], args[2]), &value);
        if (s.ok()) return resp_integer(value.size());
        if (s.IsNotFound()) return resp_integer(0);
        return resp_error(s.ToString());
    }

    // ═══════════════════════════════════════════════════════════════
    // P1: List Commands (Index + KV: list:{name}:idx:{index})
    // ═══════════════════════════════════════════════════════════════

    // List meta format: "head\ntail\nlen"
    struct ListMeta {
        int64_t head = 0;
        int64_t tail = 0;
        int64_t len = 0;
        std::string encode() const {
            return std::to_string(head) + "\n" + std::to_string(tail) + "\n" + std::to_string(len);
        }
        static ListMeta decode(const std::string& s) {
            ListMeta m;
            size_t p1 = s.find('\n');
            size_t p2 = s.find('\n', p1 + 1);
            if (p1 != std::string::npos && p2 != std::string::npos) {
                m.head = std::stoll(s.substr(0, p1));
                m.tail = std::stoll(s.substr(p1 + 1, p2 - p1 - 1));
                m.len = std::stoll(s.substr(p2 + 1));
            }
            return m;
        }
    };

    std::string handle_lpush(const std::vector<std::string>& args) {
        if (args.size() < 3) return resp_error("ERR wrong number of arguments for 'lpush' command");
        WriteOptions wo;
        ReadOptions ro;
        std::string meta_str;
        ListMeta meta;
        if (db_->Get(ro, list_meta_key(args[1]), &meta_str).ok()) {
            meta = ListMeta::decode(meta_str);
        }

        for (size_t i = 2; i < args.size(); ++i) {
            meta.head--;
            db_->Put(wo, list_idx_key(args[1], meta.head), args[i]);
            meta.len++;
        }
        db_->Put(wo, list_meta_key(args[1]), meta.encode());
        return resp_integer(meta.len);
    }

    std::string handle_rpush(const std::vector<std::string>& args) {
        if (args.size() < 3) return resp_error("ERR wrong number of arguments for 'rpush' command");
        WriteOptions wo;
        ReadOptions ro;
        std::string meta_str;
        ListMeta meta;
        if (db_->Get(ro, list_meta_key(args[1]), &meta_str).ok()) {
            meta = ListMeta::decode(meta_str);
        }

        for (size_t i = 2; i < args.size(); ++i) {
            db_->Put(wo, list_idx_key(args[1], meta.tail), args[i]);
            meta.tail++;
            meta.len++;
        }
        db_->Put(wo, list_meta_key(args[1]), meta.encode());
        return resp_integer(meta.len);
    }

    std::string handle_lpop(const std::vector<std::string>& args) {
        if (args.size() < 2) return resp_error("ERR wrong number of arguments for 'lpop' command");
        ReadOptions ro;
        std::string meta_str;
        if (!db_->Get(ro, list_meta_key(args[1]), &meta_str).ok()) return resp_nil();
        ListMeta meta = ListMeta::decode(meta_str);
        if (meta.len <= 0) return resp_nil();

        std::string value;
        db_->Get(ro, list_idx_key(args[1], meta.head), &value);
        WriteOptions wo;
        db_->Delete(wo, list_idx_key(args[1], meta.head));
        meta.head++;
        meta.len--;
        if (meta.len == 0) {
            meta.head = 0;
            meta.tail = 0;
            db_->Delete(wo, list_meta_key(args[1]));
        } else {
            db_->Put(wo, list_meta_key(args[1]), meta.encode());
        }
        return resp_bulk_string(value);
    }

    std::string handle_rpop(const std::vector<std::string>& args) {
        if (args.size() < 2) return resp_error("ERR wrong number of arguments for 'rpop' command");
        ReadOptions ro;
        std::string meta_str;
        if (!db_->Get(ro, list_meta_key(args[1]), &meta_str).ok()) return resp_nil();
        ListMeta meta = ListMeta::decode(meta_str);
        if (meta.len <= 0) return resp_nil();

        std::string value;
        db_->Get(ro, list_idx_key(args[1], meta.tail - 1), &value);
        WriteOptions wo;
        db_->Delete(wo, list_idx_key(args[1], meta.tail - 1));
        meta.tail--;
        meta.len--;
        if (meta.len == 0) {
            meta.head = 0;
            meta.tail = 0;
            db_->Delete(wo, list_meta_key(args[1]));
        } else {
            db_->Put(wo, list_meta_key(args[1]), meta.encode());
        }
        return resp_bulk_string(value);
    }

    std::string handle_lrange(const std::vector<std::string>& args) {
        if (args.size() < 4) return resp_error("ERR wrong number of arguments for 'lrange' command");
        ReadOptions ro;
        std::string meta_str;
        if (!db_->Get(ro, list_meta_key(args[1]), &meta_str).ok()) return resp_empty_array();
        ListMeta meta = ListMeta::decode(meta_str);
        if (meta.len <= 0) return resp_empty_array();

        int64_t start = std::stoll(args[2]);
        int64_t stop = std::stoll(args[3]);

        // Handle negative indices
        if (start < 0) start = std::max<int64_t>(0, meta.len + start);
        if (stop < 0) stop = meta.len + stop;
        stop = std::min(stop, meta.len - 1);

        std::vector<std::string> result;
        for (int64_t i = start; i <= stop; ++i) {
            std::string value;
            if (db_->Get(ro, list_idx_key(args[1], meta.head + i), &value).ok()) {
                result.push_back(value);
            }
        }
        return resp_array(result);
    }

    std::string handle_llen(const std::vector<std::string>& args) {
        if (args.size() < 2) return resp_error("ERR wrong number of arguments for 'llen' command");
        ReadOptions ro;
        std::string meta_str;
        if (!db_->Get(ro, list_meta_key(args[1]), &meta_str).ok()) return resp_integer(0);
        ListMeta meta = ListMeta::decode(meta_str);
        return resp_integer(meta.len);
    }

    std::string handle_lindex(const std::vector<std::string>& args) {
        if (args.size() < 3) return resp_error("ERR wrong number of arguments for 'lindex' command");
        ReadOptions ro;
        std::string meta_str;
        if (!db_->Get(ro, list_meta_key(args[1]), &meta_str).ok()) return resp_nil();
        ListMeta meta = ListMeta::decode(meta_str);

        int64_t idx = std::stoll(args[2]);
        if (idx < 0) idx = meta.len + idx;
        if (idx < 0 || idx >= meta.len) return resp_nil();

        std::string value;
        if (db_->Get(ro, list_idx_key(args[1], meta.head + idx), &value).ok()) {
            return resp_bulk_string(value);
        }
        return resp_nil();
    }

    std::string handle_lset(const std::vector<std::string>& args) {
        if (args.size() < 4) return resp_error("ERR wrong number of arguments for 'lset' command");
        ReadOptions ro;
        std::string meta_str;
        if (!db_->Get(ro, list_meta_key(args[1]), &meta_str).ok())
            return resp_error("ERR no such list");
        ListMeta meta = ListMeta::decode(meta_str);

        int64_t idx = std::stoll(args[2]);
        if (idx < 0) idx = meta.len + idx;
        if (idx < 0 || idx >= meta.len) return resp_error("ERR index out of range");

        WriteOptions wo;
        db_->Put(wo, list_idx_key(args[1], meta.head + idx), args[3]);
        return resp_ok();
    }

    std::string handle_ltrim(const std::vector<std::string>& args) {
        if (args.size() < 4) return resp_error("ERR wrong number of arguments for 'ltrim' command");
        ReadOptions ro;
        std::string meta_str;
        if (!db_->Get(ro, list_meta_key(args[1]), &meta_str).ok()) return resp_ok();
        ListMeta meta = ListMeta::decode(meta_str);
        if (meta.len <= 0) return resp_ok();

        int64_t start = std::stoll(args[2]);
        int64_t stop = std::stoll(args[3]);
        if (start < 0) start = std::max<int64_t>(0, meta.len + start);
        if (stop < 0) stop = meta.len + stop;
        stop = std::min(stop, meta.len - 1);

        WriteOptions wo;
        // Delete elements outside [start, stop]
        for (int64_t i = 0; i < meta.len; ++i) {
            if (i < start || i > stop) {
                db_->Delete(wo, list_idx_key(args[1], meta.head + i));
            }
        }
        // Update meta
        int64_t new_len = std::max<int64_t>(0, stop - start + 1);
        if (new_len <= 0) {
            db_->Delete(wo, list_meta_key(args[1]));
        } else {
            ListMeta new_meta;
            new_meta.head = meta.head + start;
            new_meta.tail = new_meta.head + new_len;
            new_meta.len = new_len;
            db_->Put(wo, list_meta_key(args[1]), new_meta.encode());
        }
        return resp_ok();
    }

    std::string handle_lrem(const std::vector<std::string>& args) {
        if (args.size() < 4) return resp_error("ERR wrong number of arguments for 'lrem' command");
        int64_t count = std::stoll(args[2]);
        std::string elem = args[3];

        ReadOptions ro;
        std::string meta_str;
        if (!db_->Get(ro, list_meta_key(args[1]), &meta_str).ok()) return resp_integer(0);
        ListMeta meta = ListMeta::decode(meta_str);
        if (meta.len <= 0) return resp_integer(0);

        // Load all elements
        std::vector<std::pair<int64_t, std::string>> elements;
        for (int64_t i = 0; i < meta.len; ++i) {
            std::string val;
            if (db_->Get(ro, list_idx_key(args[1], meta.head + i), &val).ok()) {
                elements.emplace_back(meta.head + i, val);
            }
        }

        WriteOptions wo;
        int64_t removed = 0;

        if (count == 0) {
            // Remove all occurrences
            for (auto& [idx, val] : elements) {
                if (val == elem) {
                    db_->Delete(wo, list_idx_key(args[1], idx));
                    removed++;
                }
            }
        } else if (count > 0) {
            // Remove first count occurrences
            for (auto& [idx, val] : elements) {
                if (val == elem && removed < count) {
                    db_->Delete(wo, list_idx_key(args[1], idx));
                    removed++;
                }
            }
        } else {
            // Remove last |count| occurrences (reverse)
            int64_t limit = -count;
            for (auto it = elements.rbegin(); it != elements.rend() && removed < limit; ++it) {
                if (it->second == elem) {
                    db_->Delete(wo, list_idx_key(args[1], it->first));
                    removed++;
                }
            }
        }

        if (removed > 0) {
            meta.len -= removed;
            if (meta.len <= 0) {
                db_->Delete(wo, list_meta_key(args[1]));
            } else {
                db_->Put(wo, list_meta_key(args[1]), meta.encode());
            }
        }
        return resp_integer(removed);
    }

    // ═══════════════════════════════════════════════════════════════
    // P1: Set Commands (Sorted blob encoding)
    // ═══════════════════════════════════════════════════════════════

    std::string handle_sadd(const std::vector<std::string>& args) {
        if (args.size() < 3) return resp_error("ERR wrong number of arguments for 'sadd' command");
        ReadOptions ro;
        WriteOptions wo;
        std::string blob;
        std::vector<std::string> elems;
        if (db_->Get(ro, set_blob_key(args[1]), &blob).ok()) {
            elems = parse_set_blob(blob);
        }

        // Convert to set for fast lookup
        std::set<std::string> elem_set(elems.begin(), elems.end());
        int64_t added = 0;
        for (size_t i = 2; i < args.size(); ++i) {
            if (elem_set.insert(args[i]).second) added++;
        }

        // Rebuild sorted blob
        elems.assign(elem_set.begin(), elem_set.end());
        db_->Put(wo, set_blob_key(args[1]), encode_set_blob(elems));
        return resp_integer(added);
    }

    std::string handle_srem(const std::vector<std::string>& args) {
        if (args.size() < 3) return resp_error("ERR wrong number of arguments for 'srem' command");
        ReadOptions ro;
        WriteOptions wo;
        std::string blob;
        if (!db_->Get(ro, set_blob_key(args[1]), &blob).ok()) return resp_integer(0);
        std::vector<std::string> elems = parse_set_blob(blob);

        std::set<std::string> elem_set(elems.begin(), elems.end());
        int64_t removed = 0;
        for (size_t i = 2; i < args.size(); ++i) {
            if (elem_set.erase(args[i])) removed++;
        }

        if (elem_set.empty()) {
            db_->Delete(wo, set_blob_key(args[1]));
        } else {
            elems.assign(elem_set.begin(), elem_set.end());
            db_->Put(wo, set_blob_key(args[1]), encode_set_blob(elems));
        }
        return resp_integer(removed);
    }

    std::string handle_smembers(const std::vector<std::string>& args) {
        if (args.size() < 2) return resp_error("ERR wrong number of arguments for 'smembers' command");
        ReadOptions ro;
        std::string blob;
        if (!db_->Get(ro, set_blob_key(args[1]), &blob).ok()) return resp_empty_array();
        std::vector<std::string> elems = parse_set_blob(blob);
        return resp_array(elems);
    }

    std::string handle_sismember(const std::vector<std::string>& args) {
        if (args.size() < 3) return resp_error("ERR wrong number of arguments for 'sismember' command");
        ReadOptions ro;
        std::string blob;
        if (!db_->Get(ro, set_blob_key(args[1]), &blob).ok()) return resp_integer(0);
        std::vector<std::string> elems = parse_set_blob(blob);
        std::set<std::string> elem_set(elems.begin(), elems.end());
        return resp_integer(elem_set.count(args[2]) ? 1 : 0);
    }

    std::string handle_scard(const std::vector<std::string>& args) {
        if (args.size() < 2) return resp_error("ERR wrong number of arguments for 'scard' command");
        ReadOptions ro;
        std::string blob;
        if (!db_->Get(ro, set_blob_key(args[1]), &blob).ok()) return resp_integer(0);
        std::vector<std::string> elems = parse_set_blob(blob);
        return resp_integer(static_cast<int64_t>(elems.size()));
    }

    std::string handle_spop(const std::vector<std::string>& args) {
        if (args.size() < 2) return resp_error("ERR wrong number of arguments for 'spop' command");
        ReadOptions ro;
        WriteOptions wo;
        std::string blob;
        if (!db_->Get(ro, set_blob_key(args[1]), &blob).ok()) return resp_nil();
        std::vector<std::string> elems = parse_set_blob(blob);
        if (elems.empty()) return resp_nil();

        // Pop last element (deterministic)
        std::string popped = elems.back();
        elems.pop_back();

        if (elems.empty()) {
            db_->Delete(wo, set_blob_key(args[1]));
        } else {
            db_->Put(wo, set_blob_key(args[1]), encode_set_blob(elems));
        }
        return resp_bulk_string(popped);
    }

    std::string handle_srandmember(const std::vector<std::string>& args) {
        if (args.size() < 2) return resp_error("ERR wrong number of arguments for 'srandmember' command");
        ReadOptions ro;
        std::string blob;
        if (!db_->Get(ro, set_blob_key(args[1]), &blob).ok()) return resp_nil();
        std::vector<std::string> elems = parse_set_blob(blob);
        if (elems.empty()) return resp_nil();
        // Deterministic: return middle element
        return resp_bulk_string(elems[elems.size() / 2]);
    }

    std::string handle_smove(const std::vector<std::string>& args) {
        if (args.size() < 4) return resp_error("ERR wrong number of arguments for 'smove' command");
        ReadOptions ro;
        WriteOptions wo;

        // Check source set
        std::string src_blob;
        if (!db_->Get(ro, set_blob_key(args[1]), &src_blob).ok()) return resp_integer(0);
        std::vector<std::string> src_elems = parse_set_blob(src_blob);
        std::set<std::string> src_set(src_elems.begin(), src_elems.end());

        if (!src_set.count(args[3])) return resp_integer(0);

        // Remove from source
        src_set.erase(args[3]);
        if (src_set.empty()) {
            db_->Delete(wo, set_blob_key(args[1]));
        } else {
            src_elems.assign(src_set.begin(), src_set.end());
            db_->Put(wo, set_blob_key(args[1]), encode_set_blob(src_elems));
        }

        // Add to destination
        std::string dst_blob;
        std::vector<std::string> dst_elems;
        if (db_->Get(ro, set_blob_key(args[2]), &dst_blob).ok()) {
            dst_elems = parse_set_blob(dst_blob);
        }
        std::set<std::string> dst_set(dst_elems.begin(), dst_elems.end());
        dst_set.insert(args[3]);
        dst_elems.assign(dst_set.begin(), dst_set.end());
        db_->Put(wo, set_blob_key(args[2]), encode_set_blob(dst_elems));

        return resp_integer(1);
    }

    // ═══════════════════════════════════════════════════════════════
    // P2: Bitmap Commands (bit-level operations on String values)
    // ═══════════════════════════════════════════════════════════════

    std::string handle_setbit(const std::vector<std::string>& args) {
        if (args.size() < 4) return resp_error("ERR wrong number of arguments for 'setbit' command");
        int64_t offset;
        int64_t value;
        try {
            offset = std::stoll(args[2]);
            value = std::stoll(args[3]);
        } catch (...) {
            return resp_error("ERR bit offset is not an integer or out of range");
        }
        if (offset < 0) return resp_error("ERR bit offset is not an integer or out of range");
        if (value != 0 && value != 1) return resp_error("ERR bit is not an integer or out of range");

        ReadOptions ro;
        WriteOptions wo;
        std::string blob;
        Status s = db_->Get(ro, args[1], &blob);
        if (!s.ok() && !s.IsNotFound()) return resp_error(s.ToString());

        int64_t byte_idx = offset / 8;
        int64_t bit_idx = 7 - (offset % 8); // Redis uses MSB first

        // Ensure blob is large enough
        if (static_cast<int64_t>(blob.size()) <= byte_idx) {
            blob.resize(static_cast<size_t>(byte_idx + 1), 0);
        }

        int64_t old_bit = (static_cast<uint8_t>(blob[static_cast<size_t>(byte_idx)]) >> bit_idx) & 1;
        if (value) {
            blob[static_cast<size_t>(byte_idx)] |= (1 << bit_idx);
        } else {
            blob[static_cast<size_t>(byte_idx)] &= ~(1 << bit_idx);
        }

        s = db_->Put(wo, args[1], blob);
        return s.ok() ? resp_integer(old_bit) : resp_error(s.ToString());
    }

    std::string handle_getbit(const std::vector<std::string>& args) {
        if (args.size() < 3) return resp_error("ERR wrong number of arguments for 'getbit' command");
        int64_t offset;
        try {
            offset = std::stoll(args[2]);
        } catch (...) {
            return resp_error("ERR bit offset is not an integer or out of range");
        }
        if (offset < 0) return resp_error("ERR bit offset is not an integer or out of range");

        ReadOptions ro;
        std::string blob;
        Status s = db_->Get(ro, args[1], &blob);
        if (!s.ok()) return resp_integer(0); // Non-existent key → 0

        int64_t byte_idx = offset / 8;
        int64_t bit_idx = 7 - (offset % 8);

        if (byte_idx >= static_cast<int64_t>(blob.size())) return resp_integer(0);

        int64_t bit = (static_cast<uint8_t>(blob[static_cast<size_t>(byte_idx)]) >> bit_idx) & 1;
        return resp_integer(bit);
    }

    std::string handle_bitcount(const std::vector<std::string>& args) {
        if (args.size() < 2) return resp_error("ERR wrong number of arguments for 'bitcount' command");
        ReadOptions ro;
        std::string blob;
        Status s = db_->Get(ro, args[1], &blob);
        if (!s.ok()) return resp_integer(0);

        int64_t start = 0;
        int64_t end = static_cast<int64_t>(blob.size()) - 1;
        if (args.size() >= 4) {
            try {
                start = std::stoll(args[2]);
                end = std::stoll(args[3]);
            } catch (...) {
                return resp_error("ERR value is not an integer or out of range");
            }
            // Handle negative indices
            if (start < 0) start = static_cast<int64_t>(blob.size()) + start;
            if (end < 0) end = static_cast<int64_t>(blob.size()) + end;
            if (start < 0) start = 0;
            if (end >= static_cast<int64_t>(blob.size())) end = static_cast<int64_t>(blob.size()) - 1;
            if (start > end) return resp_integer(0);
        }

        int64_t count = 0;
        for (int64_t i = start; i <= end; ++i) {
            uint8_t byte = static_cast<uint8_t>(blob[static_cast<size_t>(i)]);
            while (byte) {
                count += byte & 1;
                byte >>= 1;
            }
        }
        return resp_integer(count);
    }

    std::string handle_bitpos(const std::vector<std::string>& args) {
        if (args.size() < 3) return resp_error("ERR wrong number of arguments for 'bitpos' command");
        int64_t target_bit;
        try {
            target_bit = std::stoll(args[2]);
        } catch (...) {
            return resp_error("ERR bit is not an integer or out of range");
        }
        if (target_bit != 0 && target_bit != 1) return resp_error("ERR bit is not an integer or out of range");

        ReadOptions ro;
        std::string blob;
        Status s = db_->Get(ro, args[1], &blob);
        if (!s.ok() && target_bit == 0) return resp_integer(0); // Empty string, looking for 0
        if (!s.ok()) return resp_integer(-1); // Empty string, looking for 1

        int64_t start = 0;
        int64_t end = static_cast<int64_t>(blob.size()) - 1;
        if (args.size() >= 4) {
            try {
                start = std::stoll(args[3]);
            } catch (...) {
                return resp_error("ERR value is not an integer or out of range");
            }
            if (start < 0) start = static_cast<int64_t>(blob.size()) + start;
            if (start < 0) start = 0;
            if (args.size() >= 5) {
                try {
                    end = std::stoll(args[4]);
                } catch (...) {
                    return resp_error("ERR value is not an integer or out of range");
                }
                if (end < 0) end = static_cast<int64_t>(blob.size()) + end;
            }
            if (end >= static_cast<int64_t>(blob.size())) end = static_cast<int64_t>(blob.size()) - 1;
            if (start > end) return resp_integer(-1);
        }

        for (int64_t i = start; i <= end; ++i) {
            uint8_t byte = static_cast<uint8_t>(blob[static_cast<size_t>(i)]);
            for (int b = 7; b >= 0; --b) {
                int64_t bit = (byte >> b) & 1;
                if (bit == target_bit) {
                    return resp_integer(i * 8 + (7 - b));
                }
            }
        }

        // If looking for 0 and all bits in range are 1
        if (target_bit == 0) {
            return resp_integer((end + 1) * 8);
        }
        return resp_integer(-1);
    }

    // ═══════════════════════════════════════════════════════════════
    // P2: HyperLogLog Commands (probabilistic cardinality estimation)
    // Uses 2^14 = 16384 registers, each 6 bits → 12288 bytes
    // ═══════════════════════════════════════════════════════════════

    static constexpr int HLL_P = 14;
    static constexpr int HLL_REGISTERS = 1 << HLL_P; // 16384
    static constexpr int HLL_BITS = 6;
    static constexpr int HLL_SIZE = (HLL_REGISTERS * HLL_BITS + 7) / 8; // 12288

    // MurmurHash2 64A (simplified)
    static uint64_t hll_hash(const std::string& key) {
        uint64_t seed = 0xdeadbeef;
        const uint8_t* data = reinterpret_cast<const uint8_t*>(key.data());
        size_t len = key.size();
        uint64_t h = seed ^ len;
        const uint64_t m = 0xc6a4a7935bd1e995ULL;
        const int r = 47;

        const uint8_t* end = data + (len & ~7ULL);
        while (data != end) {
            uint64_t k;
            memcpy(&k, data, 8);
            data += 8;
            k *= m;
            k ^= k >> r;
            k *= m;
            h ^= k;
            h *= m;
        }

        uint64_t k = 0;
        size_t remaining = len & 7;
        if (remaining > 0) {
            memcpy(&k, data, remaining);
            h ^= k * m;
            h *= m;
        }

        h ^= h >> r;
        h *= m;
        h ^= h >> r;
        return h;
    }

    // Get register value (6 bits)
    static uint8_t hll_get_reg(const std::string& blob, int idx) {
        size_t bit_pos = static_cast<size_t>(idx) * HLL_BITS;
        size_t byte_pos = bit_pos / 8;
        size_t bit_off = bit_pos % 8;
        uint16_t val = static_cast<uint8_t>(blob[byte_pos]) >> bit_off;
        if (bit_off + HLL_BITS > 8 && byte_pos + 1 < blob.size()) {
            val |= static_cast<uint16_t>(static_cast<uint8_t>(blob[byte_pos + 1])) << (8 - bit_off);
        }
        return static_cast<uint8_t>(val & 0x3F);
    }

    // Set register value (6 bits)
    static void hll_set_reg(std::string& blob, int idx, uint8_t val) {
        size_t bit_pos = static_cast<size_t>(idx) * HLL_BITS;
        size_t byte_pos = bit_pos / 8;
        size_t bit_off = bit_pos % 8;
        uint16_t mask = 0x3F << bit_off;
        blob[byte_pos] = (blob[byte_pos] & ~mask) | (val << bit_off);
        if (bit_off + HLL_BITS > 8 && byte_pos + 1 < blob.size()) {
            uint16_t mask2 = 0x3F >> (8 - bit_off);
            blob[byte_pos + 1] = (blob[byte_pos + 1] & ~mask2) | (val >> (8 - bit_off));
        }
    }

    // Count leading zeros + 1 in lower 50 bits
    static uint8_t count_leading_zeros_plus_one(uint64_t hash) {
        uint64_t remaining = hash << HLL_P; // Lower 50 bits for counting
        uint8_t count = 1;
        while ((remaining & (1ULL << 63)) == 0 && count <= HLL_BITS) {
            count++;
            remaining <<= 1;
        }
        return count;
    }

    // Estimate cardinality from HLL blob
    static int64_t hll_count(const std::string& blob) {
        double sum = 0.0;
        for (int i = 0; i < HLL_REGISTERS; ++i) {
            uint8_t reg = hll_get_reg(blob, i);
            sum += 1.0 / (1ULL << reg);
        }
        double alpha = 0.7213 / (1.0 + 1.079 / HLL_REGISTERS);
        double estimate = alpha * HLL_REGISTERS * HLL_REGISTERS / sum;

        // Small range correction
        if (estimate <= 2.5 * HLL_REGISTERS) {
            int zeros = 0;
            for (int i = 0; i < HLL_REGISTERS; ++i) {
                if (hll_get_reg(blob, i) == 0) zeros++;
            }
            if (zeros > 0) {
                estimate = HLL_REGISTERS * std::log(static_cast<double>(HLL_REGISTERS) / zeros);
            }
        }
        return static_cast<int64_t>(estimate + 0.5);
    }

    std::string handle_pfadd(const std::vector<std::string>& args) {
        if (args.size() < 3) return resp_error("ERR wrong number of arguments for 'pfadd' command");

        ReadOptions ro;
        WriteOptions wo;
        std::string blob;
        Status s = db_->Get(ro, args[1], &blob);
        if (!s.ok() && !s.IsNotFound()) return resp_error(s.ToString());

        if (blob.empty()) {
            blob.resize(HLL_SIZE, 0);
        } else if (static_cast<int64_t>(blob.size()) != HLL_SIZE) {
            return resp_error("ERR WRONGTYPE Key is not a valid HyperLogLog string");
        }

        bool updated = false;
        for (size_t i = 2; i < args.size(); ++i) {
            uint64_t hash = hll_hash(args[i]);
            int idx = hash & (HLL_REGISTERS - 1); // Lower 14 bits for register index
            uint8_t rank = count_leading_zeros_plus_one(hash);
            uint8_t current = hll_get_reg(blob, idx);
            if (rank > current) {
                hll_set_reg(blob, idx, rank);
                updated = true;
            }
        }

        s = db_->Put(wo, args[1], blob);
        return s.ok() ? resp_integer(updated ? 1 : 0) : resp_error(s.ToString());
    }

    std::string handle_pfcount(const std::vector<std::string>& args) {
        if (args.size() < 2) return resp_error("ERR wrong number of arguments for 'pfcount' command");

        if (args.size() == 2) {
            ReadOptions ro;
            std::string blob;
            Status s = db_->Get(ro, args[1], &blob);
            if (!s.ok()) return resp_integer(0);
            if (static_cast<int64_t>(blob.size()) != HLL_SIZE) {
                return resp_error("ERR WRONGTYPE Key is not a valid HyperLogLog string");
            }
            return resp_integer(hll_count(blob));
        }

        // Multiple keys: merge and count
        std::string merged(HLL_SIZE, 0);
        bool any = false;
        ReadOptions ro;
        for (size_t i = 1; i < args.size(); ++i) {
            std::string blob;
            Status s = db_->Get(ro, args[i], &blob);
            if (!s.ok()) continue;
            if (static_cast<int64_t>(blob.size()) != HLL_SIZE) {
                return resp_error("ERR WRONGTYPE Key is not a valid HyperLogLog string");
            }
            any = true;
            for (int j = 0; j < HLL_REGISTERS; ++j) {
                uint8_t reg = hll_get_reg(blob, j);
                uint8_t current = hll_get_reg(merged, j);
                if (reg > current) {
                    hll_set_reg(merged, j, reg);
                }
            }
        }
        return resp_integer(any ? hll_count(merged) : 0);
    }

    std::string handle_pfmerge(const std::vector<std::string>& args) {
        if (args.size() < 3) return resp_error("ERR wrong number of arguments for 'pfmerge' command");

        std::string merged(HLL_SIZE, 0);
        bool any = false;
        ReadOptions ro;
        WriteOptions wo;
        for (size_t i = 2; i < args.size(); ++i) {
            std::string blob;
            Status s = db_->Get(ro, args[i], &blob);
            if (!s.ok()) continue;
            if (static_cast<int64_t>(blob.size()) != HLL_SIZE) {
                return resp_error("ERR WRONGTYPE Key is not a valid HyperLogLog string");
            }
            any = true;
            for (int j = 0; j < HLL_REGISTERS; ++j) {
                uint8_t reg = hll_get_reg(blob, j);
                uint8_t current = hll_get_reg(merged, j);
                if (reg > current) {
                    hll_set_reg(merged, j, reg);
                }
            }
        }

        Status s = db_->Put(wo, args[1], merged);
        return s.ok() ? resp_ok() : resp_error(s.ToString());
    }

    // ═══════════════════════════════════════════════════════════════
    // P2: ZSet Commands (sorted set by score)
    // Storage: zset:{name}:member:{member} → score
    //          zset:{name}:score:{padded_score}:{member} → ""
    //          zset:{name}:__meta__ → count
    // ═══════════════════════════════════════════════════════════════

    std::string handle_zadd(const std::vector<std::string>& args) {
        if (args.size() < 4 || (args.size() - 2) % 2 != 0)
            return resp_error("ERR wrong number of arguments for 'zadd' command");
        WriteOptions wo;
        ReadOptions ro;
        int64_t added = 0;

        std::string meta;
        db_->Get(ro, zset_meta_key(args[1]), &meta);
        int64_t count = meta.empty() ? 0 : std::stoll(meta);

        for (size_t i = 2; i + 1 < args.size(); i += 2) {
            double score;
            try {
                score = std::stod(args[i]);
            } catch (...) {
                return resp_error("ERR value is not a valid float");
            }
            const std::string& member = args[i + 1];

            std::string mk = zset_member_key(args[1], member);
            std::string old_score_str;
            bool exists = db_->Get(ro, mk, &old_score_str).ok();

            if (exists) {
                double old_score = std::stod(old_score_str);
                if (old_score == score) continue; // Same score, no change
                // Remove old score key
                db_->Delete(wo, zset_score_key(args[1], old_score, member));
            } else {
                added++;
                count++;
            }

            db_->Put(wo, mk, std::to_string(score));
            db_->Put(wo, zset_score_key(args[1], score, member), "");
        }

        db_->Put(wo, zset_meta_key(args[1]), std::to_string(count));
        return resp_integer(added);
    }

    std::string handle_zrem(const std::vector<std::string>& args) {
        if (args.size() < 3) return resp_error("ERR wrong number of arguments for 'zrem' command");
        WriteOptions wo;
        ReadOptions ro;
        int64_t removed = 0;

        std::string meta;
        db_->Get(ro, zset_meta_key(args[1]), &meta);
        int64_t count = meta.empty() ? 0 : std::stoll(meta);

        for (size_t i = 2; i < args.size(); ++i) {
            std::string mk = zset_member_key(args[1], args[i]);
            std::string score_str;
            if (db_->Get(ro, mk, &score_str).ok()) {
                double score = std::stod(score_str);
                db_->Delete(wo, mk);
                db_->Delete(wo, zset_score_key(args[1], score, args[i]));
                removed++;
                count--;
            }
        }

        if (count <= 0) {
            db_->Delete(wo, zset_meta_key(args[1]));
        } else {
            db_->Put(wo, zset_meta_key(args[1]), std::to_string(count));
        }
        return resp_integer(removed);
    }

    std::string handle_zscore(const std::vector<std::string>& args) {
        if (args.size() < 3) return resp_error("ERR wrong number of arguments for 'zscore' command");
        ReadOptions ro;
        std::string score_str;
        Status s = db_->Get(ro, zset_member_key(args[1], args[2]), &score_str);
        if (s.ok()) return resp_bulk_string(score_str);
        return resp_nil();
    }

    std::string handle_zcard(const std::vector<std::string>& args) {
        if (args.size() < 2) return resp_error("ERR wrong number of arguments for 'zcard' command");
        ReadOptions ro;
        std::string meta;
        Status s = db_->Get(ro, zset_meta_key(args[1]), &meta);
        if (s.ok()) return resp_integer(std::stoll(meta));
        return resp_integer(0);
    }

    // Scan all score keys for a zset and return sorted (by score, then member)
    std::vector<std::pair<double, std::string>> zscan_all(const std::string& name) {
        std::vector<std::pair<std::string, std::string>> results;
        ReadOptions ro;
        db_->Scan(ro, zset_score_prefix(name), 1000000, &results);

        std::vector<std::pair<double, std::string>> sorted;
        for (auto& kv : results) {
            // Key format: \x05_zset_{name}:score:{padded_score}:{member}
            std::string& key = kv.first;
            size_t sp = key.find(":score:");
            if (sp == std::string::npos) continue;
            size_t score_start = sp + 7; // after ":score:"
            size_t member_sep = key.find(':', score_start);
            if (member_sep == std::string::npos) continue;

            std::string score_str = key.substr(score_start, member_sep - score_start);
            std::string member = key.substr(member_sep + 1);
            try {
                double score = std::stod(score_str);
                sorted.emplace_back(score, member);
            } catch (...) {
                continue;
            }
        }
        // Sort by score, then by member (lexicographic)
        std::sort(sorted.begin(), sorted.end(), [](const auto& a, const auto& b) {
            if (a.first != b.first) return a.first < b.first;
            return a.second < b.second;
        });
        return sorted;
    }

    std::string handle_zrange(const std::vector<std::string>& args) {
        if (args.size() < 4) return resp_error("ERR wrong number of arguments for 'zrange' command");
        int64_t start, stop;
        try {
            start = std::stoll(args[2]);
            stop = std::stoll(args[3]);
        } catch (...) {
            return resp_error("ERR value is not an integer or out of range");
        }

        bool withscores = false;
        for (size_t i = 4; i < args.size(); ++i) {
            std::string a = args[i];
            for (auto& c : a) c = toupper(c);
            if (a == "WITHSCORES") withscores = true;
        }

        auto all = zscan_all(args[1]);
        int64_t n = static_cast<int64_t>(all.size());
        if (n == 0) return resp_array({});

        // Handle negative indices
        if (start < 0) start = n + start;
        if (stop < 0) stop = n + stop;
        if (start < 0) start = 0;
        if (stop >= n) stop = n - 1;
        if (start > stop) return resp_array({});

        std::vector<std::string> result;
        for (int64_t i = start; i <= stop; ++i) {
            result.push_back(all[static_cast<size_t>(i)].second);
            if (withscores) {
                result.push_back(std::to_string(all[static_cast<size_t>(i)].first));
            }
        }
        return resp_array(result);
    }

    std::string handle_zrevrange(const std::vector<std::string>& args) {
        if (args.size() < 4) return resp_error("ERR wrong number of arguments for 'zrevrange' command");
        int64_t start, stop;
        try {
            start = std::stoll(args[2]);
            stop = std::stoll(args[3]);
        } catch (...) {
            return resp_error("ERR value is not an integer or out of range");
        }

        bool withscores = false;
        for (size_t i = 4; i < args.size(); ++i) {
            std::string a = args[i];
            for (auto& c : a) c = toupper(c);
            if (a == "WITHSCORES") withscores = true;
        }

        auto all = zscan_all(args[1]);
        int64_t n = static_cast<int64_t>(all.size());
        if (n == 0) return resp_array({});

        // ZREVRANGE: indices are in reverse order (0 = highest score)
        // Convert to forward indices
        if (start < 0) start = n + start;
        if (stop < 0) stop = n + stop;
        if (start < 0) start = 0;
        if (stop >= n) stop = n - 1;
        if (start > stop) return resp_array({});

        std::vector<std::string> result;
        // Reverse rank i maps to forward index (n-1-i)
        // We want reverse ranks start..stop, which are forward indices (n-1-start)..(n-1-stop)
        // Iterate from highest reverse rank to lowest (forward index descending)
        for (int64_t i = start; i <= stop; ++i) {
            int64_t idx = n - 1 - i;
            result.push_back(all[static_cast<size_t>(idx)].second);
            if (withscores) {
                result.push_back(std::to_string(all[static_cast<size_t>(idx)].first));
            }
        }
        return resp_array(result);
    }

    std::string handle_zcount(const std::vector<std::string>& args) {
        if (args.size() < 4) return resp_error("ERR wrong number of arguments for 'zcount' command");

        auto parse_bound = [](const std::string& s) -> std::pair<double, bool> {
            bool exclusive = false;
            std::string num = s;
            if (!num.empty() && (num[0] == '(')) {
                exclusive = true;
                num = num.substr(1);
            }
            double val = std::stod(num);
            return {val, exclusive};
        };

        double min_val, max_val;
        bool min_excl, max_excl;
        try {
            auto min_p = parse_bound(args[2]);
            min_val = min_p.first;
            min_excl = min_p.second;
            auto max_p = parse_bound(args[3]);
            max_val = max_p.first;
            max_excl = max_p.second;
        } catch (...) {
            return resp_error("ERR value is not a valid float");
        }

        auto all = zscan_all(args[1]);
        int64_t count = 0;
        for (auto& p : all) {
            bool in_min = min_excl ? (p.first > min_val) : (p.first >= min_val);
            bool in_max = max_excl ? (p.first < max_val) : (p.first <= max_val);
            if (in_min && in_max) count++;
        }
        return resp_integer(count);
    }

    std::string handle_zrangebyscore(const std::vector<std::string>& args) {
        if (args.size() < 4) return resp_error("ERR wrong number of arguments for 'zrangebyscore' command");

        auto parse_bound = [](const std::string& s) -> std::pair<double, bool> {
            bool exclusive = false;
            std::string num = s;
            if (!num.empty() && (num[0] == '(')) {
                exclusive = true;
                num = num.substr(1);
            }
            double val = std::stod(num);
            return {val, exclusive};
        };

        double min_val, max_val;
        bool min_excl, max_excl;
        try {
            auto min_p = parse_bound(args[2]);
            min_val = min_p.first;
            min_excl = min_p.second;
            auto max_p = parse_bound(args[3]);
            max_val = max_p.first;
            max_excl = max_p.second;
        } catch (...) {
            return resp_error("ERR value is not a valid float");
        }

        int64_t offset = 0;
        int64_t count_limit = -1;
        bool withscores = false;
        for (size_t i = 4; i < args.size(); ++i) {
            std::string a = args[i];
            std::string au = a;
            for (auto& c : au) c = toupper(c);
            if (au == "WITHSCORES") {
                withscores = true;
            } else if (au == "LIMIT" && i + 2 < args.size()) {
                offset = std::stoll(args[i + 1]);
                count_limit = std::stoll(args[i + 2]);
                i += 2;
            }
        }

        auto all = zscan_all(args[1]);
        std::vector<std::string> result;
        int64_t matched = 0;
        for (auto& p : all) {
            bool in_min = min_excl ? (p.first > min_val) : (p.first >= min_val);
            bool in_max = max_excl ? (p.first < max_val) : (p.first <= max_val);
            if (in_min && in_max) {
                if (matched >= offset && (count_limit < 0 || matched - offset < count_limit)) {
                    result.push_back(p.second);
                    if (withscores) result.push_back(std::to_string(p.first));
                }
                matched++;
            }
        }
        return resp_array(result);
    }

    // ─── Geo Utilities ───

    // Geohash encode: convert (longitude, latitude) to a 52-bit integer
    // longitude: [-180, 180], latitude: [-85.05112878, 85.05112878]
    static uint64_t geohash_encode(double longitude, double latitude) {
        double min_lon = -180.0, max_lon = 180.0;
        double min_lat = -85.05112878, max_lat = 85.05112878;
        uint64_t hash = 0;
        for (int i = 0; i < 26; ++i) {
            double mid_lon = (min_lon + max_lon) / 2.0;
            double mid_lat = (min_lat + max_lat) / 2.0;
            hash <<= 1;
            if (longitude >= mid_lon) {
                hash |= 1;
                min_lon = mid_lon;
            } else {
                max_lon = mid_lon;
            }
            hash <<= 1;
            if (latitude >= mid_lat) {
                hash |= 1;
                min_lat = mid_lat;
            } else {
                max_lat = mid_lat;
            }
        }
        return hash;
    }

    // Geohash decode: convert 52-bit integer back to (longitude, latitude)
    static std::pair<double, double> geohash_decode(uint64_t hash) {
        double min_lon = -180.0, max_lon = 180.0;
        double min_lat = -85.05112878, max_lat = 85.05112878;
        for (int i = 25; i >= 0; --i) {
            double mid_lon = (min_lon + max_lon) / 2.0;
            double mid_lat = (min_lat + max_lat) / 2.0;
            // Latitude bit
            if ((hash >> (i * 2)) & 1) {
                min_lat = mid_lat;
            } else {
                max_lat = mid_lat;
            }
            // Longitude bit
            if ((hash >> (i * 2 + 1)) & 1) {
                min_lon = mid_lon;
            } else {
                max_lon = mid_lon;
            }
        }
        return {(min_lon + max_lon) / 2.0, (min_lat + max_lat) / 2.0};
    }

    // Haversine formula: calculate distance between two points in meters
    static double haversine_distance(double lon1, double lat1, double lon2, double lat2) {
        constexpr double R = 6372797.5; // Earth radius in meters
        double dlat = (lat2 - lat1) * M_PI / 180.0;
        double dlon = (lon2 - lon1) * M_PI / 180.0;
        double a = sin(dlat / 2) * sin(dlat / 2) +
                   cos(lat1 * M_PI / 180.0) * cos(lat2 * M_PI / 180.0) *
                   sin(dlon / 2) * sin(dlon / 2);
        double c = 2 * atan2(sqrt(a), sqrt(1 - a));
        return R * c;
    }

    static std::string geo_key(const std::string& name, const std::string& member) {
        return std::string(ZSET_PREFIX) + name + ":member:" + member;
    }

    static std::string geo_score_key(const std::string& name, uint64_t hash, const std::string& member) {
        char buf[32];
        snprintf(buf, sizeof(buf), "+%020lu", (unsigned long)hash);
        return std::string(ZSET_PREFIX) + name + ":score:" + buf + ":" + member;
    }

    static std::string geo_meta_key(const std::string& name) {
        return std::string(ZSET_PREFIX) + name + ":__meta__";
    }

    // ─── Geo Command Handlers ───

    std::string handle_geoadd(const std::vector<std::string>& args) {
        if (args.size() < 4 || (args.size() - 2) % 3 != 0)
            return resp_error("ERR wrong number of arguments for 'geoadd' command");

        WriteOptions wo;
        ReadOptions ro;
        int64_t added = 0;

        std::string meta;
        db_->Get(ro, geo_meta_key(args[1]), &meta);
        int64_t count = meta.empty() ? 0 : std::stoll(meta);

        for (size_t i = 2; i + 1 < args.size(); i += 3) {
            double longitude, latitude;
            try {
                longitude = std::stod(args[i]);
                latitude = std::stod(args[i + 1]);
            } catch (...) {
                return resp_error("ERR value is not a valid float");
            }
            if (longitude < -180 || longitude > 180)
                return resp_error("ERR invalid longitude");
            if (latitude < -85.05112878 || latitude > 85.05112878)
                return resp_error("ERR invalid latitude");

            const std::string& member = args[i + 2];
            uint64_t hash = geohash_encode(longitude, latitude);

            std::string mk = geo_key(args[1], member);
            std::string old_val;
            bool exists = db_->Get(ro, mk, &old_val).ok();

            if (exists) {
                uint64_t old_hash = std::stoull(old_val);
                if (old_hash == hash) continue;
                db_->Delete(wo, geo_score_key(args[1], old_hash, member));
            } else {
                added++;
                count++;
            }

            db_->Put(wo, mk, std::to_string(hash));
            db_->Put(wo, geo_score_key(args[1], hash, member), "");
        }

        db_->Put(wo, geo_meta_key(args[1]), std::to_string(count));
        return resp_integer(added);
    }

    std::string handle_geopos(const std::vector<std::string>& args) {
        if (args.size() < 2)
            return resp_error("ERR wrong number of arguments for 'geopos' command");

        ReadOptions ro;
        std::vector<std::string> result;

        for (size_t i = 2; i < args.size(); ++i) {
            std::string val;
            if (db_->Get(ro, geo_key(args[1], args[i]), &val).ok()) {
                uint64_t hash = std::stoull(val);
                auto pos = geohash_decode(hash);
                result.push_back(std::to_string(pos.first));
                result.push_back(std::to_string(pos.second));
            } else {
                result.push_back("");
                result.push_back("");
            }
        }

        return resp_array(result);
    }

    std::string handle_geodist(const std::vector<std::string>& args) {
        if (args.size() < 4 || args.size() > 5)
            return resp_error("ERR wrong number of arguments for 'geodist' command");

        ReadOptions ro;
        std::string val1, val2;
        bool ok1 = db_->Get(ro, geo_key(args[1], args[2]), &val1).ok();
        bool ok2 = db_->Get(ro, geo_key(args[1], args[3]), &val2).ok();

        if (!ok1 || !ok2) return resp_nil();

        uint64_t hash1 = std::stoull(val1);
        uint64_t hash2 = std::stoull(val2);
        auto pos1 = geohash_decode(hash1);
        auto pos2 = geohash_decode(hash2);

        double dist = haversine_distance(pos1.first, pos1.second, pos2.first, pos2.second);

        std::string unit = "m";
        if (args.size() == 5) {
            unit = args[4];
            for (auto& c : unit) c = tolower(c);
        }

        if (unit == "m") {
            char buf[32];
            snprintf(buf, sizeof(buf), "%.2f", dist);
            return resp_bulk_string(buf);
        } else if (unit == "km") {
            char buf[32];
            snprintf(buf, sizeof(buf), "%.4f", dist / 1000.0);
            return resp_bulk_string(buf);
        } else if (unit == "mi") {
            char buf[32];
            snprintf(buf, sizeof(buf), "%.4f", dist / 1609.344);
            return resp_bulk_string(buf);
        } else if (unit == "ft") {
            char buf[32];
            snprintf(buf, sizeof(buf), "%.2f", dist * 3.28084);
            return resp_bulk_string(buf);
        } else {
            return resp_error("ERR unsupported unit");
        }
    }

    // ─── Network Helpers ───

    int create_server_socket(const std::string& host, uint16_t port) {
        int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) { perror("socket"); return -1; }
        int opt = 1;
        ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
        struct sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        addr.sin_addr.s_addr = inet_addr(host.c_str());
        if (::bind(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) { perror("bind"); ::close(fd); return -1; }
        if (::listen(fd, 128) < 0) { perror("listen"); ::close(fd); return -1; }
        int flags = ::fcntl(fd, F_GETFL, 0);
        ::fcntl(fd, F_SETFL, flags | O_NONBLOCK);
        return fd;
    }

    void add_event(int fd, bool read, bool write) {
#ifdef __APPLE__
        struct kevent ev[2]; int n = 0;
        if (read)  EV_SET(&ev[n++], fd, EVFILT_READ,  EV_ADD, 0, 0, nullptr);
        if (write) EV_SET(&ev[n++], fd, EVFILT_WRITE, EV_ADD, 0, 0, nullptr);
        if (n > 0) kevent(event_fd_, ev, n, nullptr, 0, nullptr);
#else
        uint32_t events = 0;
        if (read) events |= EPOLLIN;
        if (write) events |= EPOLLOUT;
        struct epoll_event ev{}; ev.events = events; ev.data.fd = fd;
        ::epoll_ctl(event_fd_, EPOLL_CTL_ADD, fd, &ev);
#endif
    }

    void mod_event(int fd, bool read, bool write) {
#ifdef __APPLE__
        struct kevent ev[4]; int n = 0;
        if (read)  EV_SET(&ev[n++], fd, EVFILT_READ,  EV_ADD | EV_ENABLE, 0, 0, nullptr);
        else       EV_SET(&ev[n++], fd, EVFILT_READ,  EV_DELETE, 0, 0, nullptr);
        if (write) EV_SET(&ev[n++], fd, EVFILT_WRITE, EV_ADD | EV_ENABLE, 0, 0, nullptr);
        else       EV_SET(&ev[n++], fd, EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
        if (n > 0) kevent(event_fd_, ev, n, nullptr, 0, nullptr);
#else
        uint32_t events = 0;
        if (read) events |= EPOLLIN;
        if (write) events |= EPOLLOUT;
        struct epoll_event ev{}; ev.events = events; ev.data.fd = fd;
        ::epoll_ctl(event_fd_, EPOLL_CTL_MOD, fd, &ev);
#endif
    }

    void del_event(int fd) {
#ifdef __APPLE__
        struct kevent ev[2]; int n = 0;
        EV_SET(&ev[n++], fd, EVFILT_READ,  EV_DELETE, 0, 0, nullptr);
        EV_SET(&ev[n++], fd, EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
        kevent(event_fd_, ev, n, nullptr, 0, nullptr);
#else
        ::epoll_ctl(event_fd_, EPOLL_CTL_DEL, fd, nullptr);
#endif
    }

    void accept_connection(int listen_fd, ConnType type) {
        struct sockaddr_in addr{}; socklen_t len = sizeof(addr);
        int fd = ::accept(listen_fd, (struct sockaddr*)&addr, &len);
        if (fd < 0) return;
        int flags = ::fcntl(fd, F_GETFL, 0);
        ::fcntl(fd, F_SETFL, flags | O_NONBLOCK);
        auto& conn = connections_[fd];
        conn.fd = fd;
        conn.type = type;
        conn.closed = false;
        conn.authenticated = !auth_required();
        conn.has_pending_work = false;
        add_event(fd, true, false);
    }

    void handle_client(int fd, bool readable, bool writable) {
        auto it = connections_.find(fd);
        if (it == connections_.end()) return;
        auto& conn = it->second;
        if (readable) {
            char buf[4096];
            ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
            if (n <= 0) { close_connection(fd); return; }
            conn.recv_buf.append(buf, n);
            if (conn.type == ConnType::kTCP) process_tcp(conn);
            else process_http(conn);
        }
        if (writable && !conn.send_buf.empty()) {
            ssize_t n = ::send(fd, conn.send_buf.data(), conn.send_buf.size(), MSG_NOSIGNAL);
            if (n <= 0) { close_connection(fd); return; }
            conn.send_buf.erase(0, n);
            if (conn.send_buf.empty()) mod_event(fd, true, false);
        }
    }

    void close_connection(int fd) {
        del_event(fd);
        ::close(fd);
        connections_.erase(fd);
    }

    void close_all_connections() {
        for (auto& [fd, _] : connections_) {
            del_event(fd);
            ::close(fd);
        }
        connections_.clear();
    }

    void queue_response(Connection& conn, const std::string& resp) {
        conn.send_buf += resp;
        mod_event(conn.fd, true, true);
    }

    // Worker thread completion callback (called from worker thread)
    void on_worker_complete(int fd, std::string resp) {
        std::lock_guard<std::mutex> lock(resp_mutex_);
        auto it = connections_.find(fd);
        if (it != connections_.end()) {
            it->second.pending_responses.push(std::move(resp));
        }
    }

    // Drain pending responses from worker threads (called from event loop thread)
    void drain_responses() {
        if (!pool_) return;
        std::lock_guard<std::mutex> lock(resp_mutex_);
        for (auto& [fd, conn] : connections_) {
            if (!conn.pending_responses.empty()) {
                while (!conn.pending_responses.empty()) {
                    conn.send_buf += conn.pending_responses.front();
                    conn.pending_responses.pop();
                }
                if (!conn.send_buf.empty()) {
                    mod_event(fd, true, true);
                }
            }
        }
    }

    // ─── HTTP Protocol ───

    void process_http(Connection& conn) {
        size_t pos = conn.recv_buf.find("\r\n\r\n");
        if (pos == std::string::npos) return;
        std::string request = conn.recv_buf.substr(0, pos);
        conn.recv_buf.erase(0, pos + 4);
        std::string resp = handle_http_request(request);
        queue_response(conn, resp);
    }

    std::string handle_http_request(const std::string& request) {
        std::istringstream iss(request);
        std::string method, path, http_version;
        iss >> method >> path >> http_version;
        if (method.empty() || path.empty()) return http_response(400, "Bad Request", "text/plain", "Bad Request");
        if (method == "GET") {
            if (path == "/health")  return handle_health();
            if (path == "/metrics") return handle_metrics();
            if (path == "/status")  return handle_status();
            return http_response(404, "Not Found", "text/plain", "Not Found");
        }
        if (method == "POST" && path == "/backup") return handle_backup();
        return http_response(405, "Method Not Allowed", "text/plain", "Method Not Allowed");
    }

    std::string http_response(int code, const std::string& status, const std::string& content_type, const std::string& body) {
        std::string resp;
        resp += "HTTP/1.1 " + std::to_string(code) + " " + status + "\r\n";
        resp += "Content-Type: " + content_type + "\r\n";
        resp += "Content-Length: " + std::to_string(body.size()) + "\r\n";
        resp += "Connection: close\r\n\r\n";
        resp += body;
        return resp;
    }

    std::string handle_health() {
        auto now = std::chrono::steady_clock::now();
        auto uptime = std::chrono::duration_cast<std::chrono::seconds>(now - start_time_).count();
        return http_response(200, "OK", "application/json",
            "{\"status\":\"ok\",\"uptime_seconds\":" + std::to_string(uptime) + "}");
    }

    std::string handle_metrics() {
        auto stats = static_cast<DBImpl*>(db_)->GetStats();
        auto now = std::chrono::steady_clock::now();
        auto uptime = std::chrono::duration_cast<std::chrono::seconds>(now - start_time_).count();
        std::string body;
        body += "# HELP lightkv_total_writes Total writes\n# TYPE lightkv_total_writes counter\nlightkv_total_writes " + std::to_string(stats.total_writes) + "\n";
        body += "# HELP lightkv_total_reads Total reads\n# TYPE lightkv_total_reads counter\nlightkv_total_reads " + std::to_string(stats.total_reads) + "\n";
        body += "# HELP lightkv_total_deletes Total deletes\n# TYPE lightkv_total_deletes counter\nlightkv_total_deletes " + std::to_string(stats.total_deletes) + "\n";
        body += "# HELP lightkv_total_flushes Total flushes\n# TYPE lightkv_total_flushes counter\nlightkv_total_flushes " + std::to_string(stats.total_flushes) + "\n";
        body += "# HELP lightkv_total_compactions Total compactions\n# TYPE lightkv_total_compactions counter\nlightkv_total_compactions " + std::to_string(stats.total_compactions) + "\n";
        body += "# HELP lightkv_memtable_size Memtable size\n# TYPE lightkv_memtable_size gauge\nlightkv_memtable_size " + std::to_string(stats.memtable_size) + "\n";
        body += "# HELP lightkv_pending_deletes Pending deletes\n# TYPE lightkv_pending_deletes gauge\nlightkv_pending_deletes " + std::to_string(stats.pending_deletes) + "\n";
        body += "# HELP lightkv_uptime_seconds Uptime\n# TYPE lightkv_uptime_seconds gauge\nlightkv_uptime_seconds " + std::to_string(uptime) + "\n";
        for (int i = 0; i < 7; ++i)
            body += "lightkv_level_size{level=\"" + std::to_string(i) + "\"} " + std::to_string(stats.level_sizes[i]) + "\n";
        return http_response(200, "OK", "text/plain", body);
    }

    std::string handle_status() {
        auto stats = static_cast<DBImpl*>(db_)->GetStats();
        auto now = std::chrono::steady_clock::now();
        auto uptime = std::chrono::duration_cast<std::chrono::seconds>(now - start_time_).count();
        std::string body = "{";
        body += "\"uptime_seconds\":" + std::to_string(uptime) + ",";
        body += "\"total_writes\":" + std::to_string(stats.total_writes) + ",";
        body += "\"total_reads\":" + std::to_string(stats.total_reads) + ",";
        body += "\"total_deletes\":" + std::to_string(stats.total_deletes) + ",";
        body += "\"total_flushes\":" + std::to_string(stats.total_flushes) + ",";
        body += "\"total_compactions\":" + std::to_string(stats.total_compactions) + ",";
        body += "\"memtable_size\":" + std::to_string(stats.memtable_size) + ",";
        body += "\"pending_deletes\":" + std::to_string(stats.pending_deletes) + ",";
        body += "\"uptime_seconds\":" + std::to_string(uptime);
        body += "}";
        return http_response(200, "OK", "application/json", body);
    }

    std::string handle_backup() {
        auto stats = static_cast<DBImpl*>(db_)->GetStats();
        auto now = std::chrono::steady_clock::now();
        auto uptime = std::chrono::duration_cast<std::chrono::seconds>(now - start_time_).count();
        std::string body = "{\"status\":\"ok\",\"message\":\"Backup API ready, call Backup() directly\"}";
        return http_response(200, "OK", "application/json", body);
    }

    DB* db_;
    ServerOptions opts_;
    std::atomic<bool> running_;
    int tcp_fd_;
    int http_fd_;
    int event_fd_;
    std::chrono::steady_clock::time_point start_time_;
    std::unordered_map<int, Connection> connections_;
    std::unique_ptr<ThreadPool> pool_;
    std::mutex resp_mutex_;  // protects pending_responses across all connections
};

// ─── Server Public API ───

Server::Server(DB* db, const ServerOptions& opts) {
    impl_ = new Impl(db, opts);
}
Server::~Server() { delete impl_; }
void Server::Run() { impl_->Run(); }
void Server::Stop() { impl_->Stop(); }

} // namespace lightkv