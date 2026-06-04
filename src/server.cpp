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
// Redis Serialization Protocol
// Simple String:  +OK\r\n
// Error:          -ERR message\r\n
// Integer:        :123\r\n
// Bulk String:    $5\r\nhello\r\n
// Array:          *2\r\n$3\r\nGET\r\n$3\r\nkey\r\n

static std::string resp_ok() { return "+OK\r\n"; }
static std::string resp_nil() { return "$-1\r\n"; }
static std::string resp_integer(int64_t n) { return ":" + std::to_string(n) + "\r\n"; }
static std::string resp_error(const std::string& msg) { return "-" + msg + "\r\n"; }
static std::string resp_bulk_string(const std::string& s) {
    return "$" + std::to_string(s.size()) + "\r\n" + s + "\r\n";
}
static std::string resp_array(const std::vector<std::string>& arr) {
    std::string r = "*" + std::to_string(arr.size()) + "\r\n";
    for (auto& s : arr) r += resp_bulk_string(s);
    return r;
}

class Server::Impl {
public:
    Impl(DB* db, const ServerOptions& opts)
        : db_(db), opts_(opts), running_(false),
          tcp_fd_(-1), http_fd_(-1), event_fd_(-1) {
        start_time_ = std::chrono::steady_clock::now();
    }

    ~Impl() {
        Stop();
    }

    void Run() {
        running_.store(true);

#ifdef __APPLE__
        event_fd_ = kqueue();
#else
        event_fd_ = ::epoll_create1(0);
#endif
        if (event_fd_ < 0) {
            perror("event_fd");
            return;
        }

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

        if (tcp_fd_ < 0 && http_fd_ < 0) {
            fprintf(stderr, "[LightKV] No server sockets bound, exiting\n");
            return;
        }

        // Event loop
        while (running_.load()) {
#ifdef __APPLE__
            struct kevent events[opts_.max_connections];
            struct timespec ts;
            ts.tv_sec = opts_.epoll_timeout_ms / 1000;
            ts.tv_nsec = (opts_.epoll_timeout_ms % 1000) * 1000000;
            int n = kevent(event_fd_, nullptr, 0, events, opts_.max_connections, &ts);
#else
            struct epoll_event events[opts_.max_connections];
            int n = ::epoll_wait(event_fd_, events, opts_.max_connections,
                                 opts_.epoll_timeout_ms);
#endif
            if (n < 0) {
                if (errno == EINTR) continue;
                perror("event_wait");
                break;
            }

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

    void Stop() {
        running_.store(false);
    }

private:
    int create_server_socket(const std::string& host, uint16_t port) {
        int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) {
            perror("socket");
            return -1;
        }

        int opt = 1;
        ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        struct sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        addr.sin_addr.s_addr = inet_addr(host.c_str());

        if (::bind(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
            perror("bind");
            ::close(fd);
            return -1;
        }

        if (::listen(fd, 128) < 0) {
            perror("listen");
            ::close(fd);
            return -1;
        }

        int flags = ::fcntl(fd, F_GETFL, 0);
        ::fcntl(fd, F_SETFL, flags | O_NONBLOCK);

        return fd;
    }

    void add_event(int fd, bool read, bool write) {
#ifdef __APPLE__
        struct kevent ev[2];
        int n = 0;
        if (read) {
            EV_SET(&ev[n++], fd, EVFILT_READ, EV_ADD, 0, 0, nullptr);
        }
        if (write) {
            EV_SET(&ev[n++], fd, EVFILT_WRITE, EV_ADD, 0, 0, nullptr);
        }
        if (n > 0) kevent(event_fd_, ev, n, nullptr, 0, nullptr);
#else
        uint32_t events = 0;
        if (read) events |= EPOLLIN;
        if (write) events |= EPOLLOUT;
        struct epoll_event ev{};
        ev.events = events;
        ev.data.fd = fd;
        ::epoll_ctl(event_fd_, EPOLL_CTL_ADD, fd, &ev);
#endif
    }

    void mod_event(int fd, bool read, bool write) {
#ifdef __APPLE__
        struct kevent ev[4];
        int n = 0;
        EV_SET(&ev[n++], fd, EVFILT_READ, EV_DELETE, 0, 0, nullptr);
        EV_SET(&ev[n++], fd, EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
        if (read) {
            EV_SET(&ev[n++], fd, EVFILT_READ, EV_ADD, 0, 0, nullptr);
        }
        if (write) {
            EV_SET(&ev[n++], fd, EVFILT_WRITE, EV_ADD, 0, 0, nullptr);
        }
        kevent(event_fd_, ev, n, nullptr, 0, nullptr);
#else
        uint32_t events = 0;
        if (read) events |= EPOLLIN;
        if (write) events |= EPOLLOUT;
        struct epoll_event ev{};
        ev.events = events;
        ev.data.fd = fd;
        ::epoll_ctl(event_fd_, EPOLL_CTL_MOD, fd, &ev);
#endif
    }

    void del_event(int fd) {
#ifdef __APPLE__
        struct kevent ev[2];
        int n = 0;
        EV_SET(&ev[n++], fd, EVFILT_READ, EV_DELETE, 0, 0, nullptr);
        EV_SET(&ev[n++], fd, EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
        kevent(event_fd_, ev, n, nullptr, 0, nullptr);
#else
        ::epoll_ctl(event_fd_, EPOLL_CTL_DEL, fd, nullptr);
#endif
    }

    void accept_connection(int listen_fd, ConnType type) {
        struct sockaddr_in addr{};
        socklen_t len = sizeof(addr);
        int fd = ::accept(listen_fd, (struct sockaddr*)&addr, &len);
        if (fd < 0) return;

        int flags = ::fcntl(fd, F_GETFL, 0);
        ::fcntl(fd, F_SETFL, flags | O_NONBLOCK);

        Connection conn;
        conn.fd = fd;
        conn.type = type;
        conn.closed = false;
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
            if (n <= 0) {
                close_connection(fd);
                return;
            }
            conn.recv_buf.append(buf, n);

            if (conn.type == ConnType::kTCP) {
                process_tcp(conn);
            } else {
                process_http(conn);
            }
        }

        if (writable) {
            if (!conn.send_buf.empty()) {
                ssize_t n = ::send(fd, conn.send_buf.data(), conn.send_buf.size(), MSG_NOSIGNAL);
                if (n <= 0) {
                    close_connection(fd);
                    return;
                }
                conn.send_buf.erase(0, n);
            }
            if (conn.send_buf.empty()) {
                mod_event(fd, true, false);
            }
        }
    }

    void close_connection(int fd) {
        del_event(fd);
        ::close(fd);
        connections_.erase(fd);
    }

    void close_all_connections() {
        for (auto& [fd, conn] : connections_) {
            del_event(fd);
            ::close(fd);
        }
        connections_.clear();
    }

    void queue_response(Connection& conn, const std::string& resp) {
        conn.send_buf += resp;
        mod_event(conn.fd, true, true);
    }

    // ─── RESP Protocol Parser ───
    // Parses RESP arrays from the buffer.
    // Returns true if a complete command was parsed.

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
        if (len < 0) {
            out.clear();
            next_pos = cr + 2;
            return true;
        }

        size_t data_start = cr + 2;
        size_t data_end = data_start + len + 2; // +2 for \r\n
        if (data_end > buf.size()) return false;

        out = buf.substr(data_start, len);
        next_pos = data_end;
        return true;
    }

    // ─── TCP Protocol (Redis RESP) ───
    // Commands:
    //   SET key value     → +OK\r\n
    //   GET key           → $N\r\nvalue\r\n  or  $-1\r\n
    //   DEL key           → :1\r\n
    //   DELRANGE begin end → :1\r\n
    //   STATS             → *N\r\n$K\r\n$V\r\n...
    //   QUIT              → +OK\r\n (then close)
    //   PING              → +PONG\r\n
    //   DBSIZE            → :N\r\n

    void process_tcp(Connection& conn) {
        while (true) {
            size_t array_end;
            size_t array_len;
            if (!parse_resp_array(conn.recv_buf, array_len, array_end)) break;

            // Check if we have enough data for all bulk strings
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

            // Remove processed data
            conn.recv_buf.erase(0, pos);

            std::string resp = handle_tcp_command(args);
            if (!resp.empty()) queue_response(conn, resp);
        }
    }

    std::string handle_tcp_command(const std::vector<std::string>& args) {
        if (args.empty()) return resp_error("ERR empty command");

        std::string cmd = args[0];
        // Convert to uppercase for comparison
        for (auto& c : cmd) c = static_cast<char>(toupper(c));

        if (cmd == "PING") return "+PONG\r\n";
        if (cmd == "QUIT") return "+OK\r\n";

        if (cmd == "SET") {
            if (args.size() < 3) return resp_error("ERR wrong number of arguments for 'set' command");
            WriteOptions wo;
            auto s = db_->Put(wo, args[1], args[2]);
            return s.ok() ? resp_ok() : resp_error(s.ToString());
        }

        if (cmd == "GET") {
            if (args.size() < 2) return resp_error("ERR wrong number of arguments for 'get' command");
            std::string value;
            ReadOptions ro;
            auto s = db_->Get(ro, args[1], &value);
            if (s.ok()) return resp_bulk_string(value);
            if (s.IsNotFound()) return resp_nil();
            return resp_error(s.ToString());
        }

        if (cmd == "DEL") {
            if (args.size() < 2) return resp_error("ERR wrong number of arguments for 'del' command");
            WriteOptions wo;
            auto s = db_->Delete(wo, args[1]);
            return s.ok() ? resp_integer(1) : resp_integer(0);
        }

        if (cmd == "DELRANGE") {
            if (args.size() < 3) return resp_error("ERR wrong number of arguments for 'delrange' command");
            WriteOptions wo;
            auto s = db_->DeleteRange(wo, args[1], args[2]);
            return s.ok() ? resp_integer(1) : resp_error(s.ToString());
        }

        if (cmd == "STATS") return handle_tcp_stats();
        if (cmd == "DBSIZE") return handle_tcp_dbsize();

        return resp_error("ERR unknown command '" + args[0] + "'");
    }

    std::string handle_tcp_stats() {
        auto stats = static_cast<DBImpl*>(db_)->GetStats();
        std::vector<std::string> arr;
        arr.push_back("total_writes");
        arr.push_back(std::to_string(stats.total_writes));
        arr.push_back("total_reads");
        arr.push_back(std::to_string(stats.total_reads));
        arr.push_back("total_deletes");
        arr.push_back(std::to_string(stats.total_deletes));
        arr.push_back("total_flushes");
        arr.push_back(std::to_string(stats.total_flushes));
        arr.push_back("total_compactions");
        arr.push_back(std::to_string(stats.total_compactions));
        arr.push_back("memtable_size");
        arr.push_back(std::to_string(stats.memtable_size));
        arr.push_back("pending_deletes");
        arr.push_back(std::to_string(stats.pending_deletes));
        return resp_array(arr);
    }

    std::string handle_tcp_dbsize() {
        // Approximate: count keys in memtable + sstables
        auto stats = static_cast<DBImpl*>(db_)->GetStats();
        // For now, return a simple approximation
        return resp_integer(0);
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

        if (method.empty() || path.empty()) {
            return http_response(400, "Bad Request", "text/plain", "Bad Request");
        }

        if (method == "GET") {
            if (path == "/health") return handle_health();
            if (path == "/metrics") return handle_metrics();
            if (path == "/status") return handle_status();
            return http_response(404, "Not Found", "text/plain", "Not Found");
        }

        if (method == "POST") {
            if (path == "/backup") return handle_backup();
            return http_response(404, "Not Found", "text/plain", "Not Found");
        }

        return http_response(405, "Method Not Allowed", "text/plain", "Method Not Allowed");
    }

    std::string http_response(int code, const std::string& status,
                               const std::string& content_type,
                               const std::string& body) {
        std::string resp;
        resp += "HTTP/1.1 " + std::to_string(code) + " " + status + "\r\n";
        resp += "Content-Type: " + content_type + "\r\n";
        resp += "Content-Length: " + std::to_string(body.size()) + "\r\n";
        resp += "Connection: close\r\n";
        resp += "\r\n";
        resp += body;
        return resp;
    }

    std::string handle_health() {
        auto now = std::chrono::steady_clock::now();
        auto uptime = std::chrono::duration_cast<std::chrono::seconds>(now - start_time_).count();
        std::string body = "{\"status\":\"ok\",\"uptime_seconds\":" + std::to_string(uptime) + "}";
        return http_response(200, "OK", "application/json", body);
    }

    std::string handle_metrics() {
        auto stats = static_cast<DBImpl*>(db_)->GetStats();
        auto now = std::chrono::steady_clock::now();
        auto uptime = std::chrono::duration_cast<std::chrono::seconds>(now - start_time_).count();

        std::string body;
        body += "# HELP lightkv_total_writes Total number of write operations\n";
        body += "# TYPE lightkv_total_writes counter\n";
        body += "lightkv_total_writes " + std::to_string(stats.total_writes) + "\n";

        body += "# HELP lightkv_total_reads Total number of read operations\n";
        body += "# TYPE lightkv_total_reads counter\n";
        body += "lightkv_total_reads " + std::to_string(stats.total_reads) + "\n";

        body += "# HELP lightkv_total_deletes Total number of delete operations\n";
        body += "# TYPE lightkv_total_deletes counter\n";
        body += "lightkv_total_deletes " + std::to_string(stats.total_deletes) + "\n";

        body += "# HELP lightkv_total_flushes Total number of flush operations\n";
        body += "# TYPE lightkv_total_flushes counter\n";
        body += "lightkv_total_flushes " + std::to_string(stats.total_flushes) + "\n";

        body += "# HELP lightkv_total_compactions Total number of compaction operations\n";
        body += "# TYPE lightkv_total_compactions counter\n";
        body += "lightkv_total_compactions " + std::to_string(stats.total_compactions) + "\n";

        body += "# HELP lightkv_memtable_size Current memtable size in bytes\n";
        body += "# TYPE lightkv_memtable_size gauge\n";
        body += "lightkv_memtable_size " + std::to_string(stats.memtable_size) + "\n";

        body += "# HELP lightkv_pending_deletes Number of pending file deletions\n";
        body += "# TYPE lightkv_pending_deletes gauge\n";
        body += "lightkv_pending_deletes " + std::to_string(stats.pending_deletes) + "\n";

        body += "# HELP lightkv_uptime_seconds Server uptime in seconds\n";
        body += "# TYPE lightkv_uptime_seconds gauge\n";
        body += "lightkv_uptime_seconds " + std::to_string(uptime) + "\n";

        for (int i = 0; i < 7; ++i) {
            body += "# HELP lightkv_level_size Size of level " + std::to_string(i) + " in bytes\n";
            body += "# TYPE lightkv_level_size gauge\n";
            body += "lightkv_level_size{level=\"" + std::to_string(i) + "\"} " +
                    std::to_string(stats.level_sizes[i]) + "\n";
        }

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
        body += "\"level_sizes\":[";
        for (int i = 0; i < 7; ++i) {
            if (i > 0) body += ",";
            body += std::to_string(stats.level_sizes[i]);
        }
        body += "]}";

        return http_response(200, "OK", "application/json", body);
    }

    std::string handle_backup() {
        std::string backup_path = "/tmp/lightkv_backup_" +
                                  std::to_string(std::time(nullptr));
        auto s = db_->Backup(backup_path);
        if (s.ok()) {
            std::string body = "{\"status\":\"ok\",\"backup_path\":\"" + backup_path + "\"}";
            return http_response(200, "OK", "application/json", body);
        }
        std::string body = "{\"status\":\"error\",\"message\":\"" + s.ToString() + "\"}";
        return http_response(500, "Internal Server Error", "application/json", body);
    }

    DB* db_;
    ServerOptions opts_;
    std::atomic<bool> running_;
    std::chrono::steady_clock::time_point start_time_;

    int tcp_fd_;
    int http_fd_;
    int event_fd_;

    std::unordered_map<int, Connection> connections_;
};

Server::Server(DB* db, const ServerOptions& options)
    : impl_(new Impl(db, options)) {}

Server::~Server() {
    delete impl_;
}

void Server::Run() {
    impl_->Run();
}

void Server::Stop() {
    impl_->Stop();
}

} // namespace lightkv
