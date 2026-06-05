#pragma once

#include "db.h"
#include <cstdint>
#include <string>

namespace lightkv {

struct ServerOptions {
    // TCP Server options
    std::string tcp_host = "0.0.0.0";
    uint16_t tcp_port = 6379;
    bool enable_tcp = true;

    // HTTP Monitor options
    std::string http_host = "0.0.0.0";
    uint16_t http_port = 8080;
    bool enable_http = true;

    // Connection limits
    int max_connections = 1024;
    int epoll_timeout_ms = 100;

    // Worker pool
    int worker_threads = 0;  // 0 = single-threaded (default), >0 = thread pool size

    // Authentication
    std::string requirepass;  // empty = no authentication required
};

// LightKV Server: provides TCP access to the embedded DB engine
// and HTTP monitoring endpoints
class Server {
public:
    Server(DB* db, const ServerOptions& options);
    ~Server();

    // Start the server (blocks until Stop() is called)
    void Run();

    // Stop the server gracefully
    void Stop();

private:
    class Impl;
    Impl* impl_;
};

} // namespace lightkv
