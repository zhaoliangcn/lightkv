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

class Server::Impl {
public:
    Impl(DB* db, const ServerOptions& opts)
        : db_(db), opts_(opts), running_(false),
          tcp_fd_(-1), http_fd_(-1), event_fd_(-1) {
        start_time_ = std::chrono::steady_clock::now();
        InitCommandTable();
    }

    ~Impl() { Stop(); }

    void Run() {
        running_.store(true);

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

        while (running_.load()) {
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
                if (fd == tcp_fd_ && readable) {
                    accept_connection(fd, ConnType::kTCP);
                } else if (fd == http_fd_ && readable) {
                    accept_connection(fd, ConnType::kHTTP);
                } else if (fd != tcp_fd_ && fd != http_fd_) {
                    handle_client(fd, readable, writable);
                }
            }
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
            std::string resp = handle_tcp_command(args);
            if (!resp.empty()) queue_response(conn, resp);
        }
    }

    std::string handle_tcp_command(const std::vector<std::string>& args) {
        if (args.empty()) return resp_error("ERR empty command");

        std::string cmd = args[0];
        for (auto& c : cmd) c = static_cast<char>(toupper(c));

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
        // Simple glob to regex conversion (only supports * wildcard)
        // Pattern match: if pattern is "*", return all keys
        // Otherwise, treat * as wildcard, prefix match if no leading *
        std::vector<std::string> result;
        ReadOptions ro;
        std::vector<std::pair<std::string, std::string>> scan_results;
        Status s = db_->Scan(ro, "", 100000, &scan_results);
        if (!s.ok()) return resp_error(s.ToString());

        for (const auto& pr : scan_results) {
            const std::string& key = pr.first;
            // Skip internal TTL keys
            if (key.size() > 0 && key[0] == TTL_MAGIC) continue;
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
            if (key.size() > 0 && key[0] == TTL_MAGIC) continue;
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
        // Filter out internal TTL keys and pick a random one
        std::vector<std::string> keys;
        for (const auto& pr : results) {
            if (pr.first.size() > 0 && pr.first[0] == TTL_MAGIC) continue;
            keys.push_back(pr.first);
        }
        if (keys.empty()) return resp_nil();
        // Simple deterministic "random" by taking a middle element
        return resp_bulk_string(keys[keys.size() / 2]);
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
        struct kevent ev[2]; int n = 0;
        if (read)  EV_SET(&ev[n++], fd, EVFILT_READ,  EV_ADD | EV_ENABLE, 0, 0, nullptr);
        if (write) EV_SET(&ev[n++], fd, EVFILT_WRITE, EV_ADD | EV_ENABLE, 0, 0, nullptr);
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
        Connection conn; conn.fd = fd; conn.type = type; conn.closed = false;
        connections_[fd] = conn;
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

    void close_connection(int fd) { del_event(fd); ::close(fd); connections_.erase(fd); }
    void close_all_connections() { for (auto& [fd, _] : connections_) { del_event(fd); ::close(fd); } connections_.clear(); }
    void queue_response(Connection& conn, const std::string& resp) {
        conn.send_buf += resp;
        mod_event(conn.fd, true, true);
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
};

// ─── Server Public API ───

Server::Server(DB* db, const ServerOptions& opts) {
    impl_ = new Impl(db, opts);
}
Server::~Server() { delete impl_; }
void Server::Run() { impl_->Run(); }
void Server::Stop() { impl_->Stop(); }

} // namespace lightkv