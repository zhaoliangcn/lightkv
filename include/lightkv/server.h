#pragma once

#include "db.h"
#include <cstdint>
#include <string>
#include <vector>

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
    // v2.0 Phase 3: 集群分片配置
    bool enable_cluster = false;           // false = 单机模式
    uint64_t cluster_node_id = 0;          // 本节点集群 ID
    std::string cluster_peers_config;      // peers 配置字符串（与 --raft-peers 同格式）
    uint16_t cluster_raft_port = 16379;    // Raft 内部通信端口

    // v2.0 Phase B: Raft 集成 — 写操作通过 Raft 复制
    // 由 server_main 在创建 Server 前设置，为 nullptr 时走直接 DB 路径
    void* raft_ptr = nullptr;              // 指向 lightkv::Raft 实例
};

// Raft log payload encoding for client commands.
//
// Wire format: [n_args:4][len0:4][arg0][len1:4][arg1]...
// Replicating the full argv (rather than a pre-computed KV result) keeps the
// leader and the followers on exactly the same code path.
std::string EncodeRaftCommand(const std::vector<std::string>& args);
bool DecodeRaftCommand(const std::string& data, std::vector<std::string>* args);

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

    // Execute one already-committed client command against the local state.
    //
    // Used by the Raft state machine on the apply thread: replicating the
    // command (rather than a pre-computed result) keeps leader and followers on
    // exactly the same code path. Never call this from a request handler.
    std::string ExecuteLocally(const std::vector<std::string>& args);

private:
    class Impl;
    Impl* impl_;
};

} // namespace lightkv
