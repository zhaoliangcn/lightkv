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

    // TTL active expire
    int ttl_scan_interval_ms = 1000;  // 0 = disable active expire, >0 = scan interval in ms
    int ttl_sample_count = 20;        // number of keys to sample per scan round
    float ttl_sample_ratio = 0.25f;   // if expired ratio > this, repeat scan

    // v2.0 慢查询日志（详见设计草案 8.4）
    uint64_t slowlog_threshold_ms = 100;  // 超过此阈值记录慢查询
    uint32_t slowlog_max_len = 128;       // 最多保留条数

    // Replication
    std::string master_host;          // Slave mode: Master address
    uint16_t master_port = 6379;      // Slave mode: Master port
    std::string master_auth;          // Master authentication password
    int repl_backlog_size = 1024 * 1024;  // Replication backlog size (1MB)
    int repl_ping_interval_ms = 10000;    // Heartbeat interval (10s)
    bool readonly = false;            // Read-only mode (auto-enabled for Slave)
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
