#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

namespace lightkv {

enum class CompressionType {
    kNoCompression = 0,
    kLZ4Compression = 1,
};

struct Options {
    size_t memtable_size = 64 * 1024 * 1024;   // 64MB
    size_t wal_file_size = 256 * 1024 * 1024;   // 256MB
    size_t block_cache_size = 512 * 1024 * 1024; // 512MB
    size_t block_size = 4 * 1024;                // 4KB
    size_t max_level = 7;
    size_t l0_file_num_trigger = 4;              // L0 compaction trigger
    size_t level_multiplier = 10;                // size ratio between levels

    // Compaction backpressure. Past the slow threshold a writer pauses briefly
    // before giving up its turn; past the stop threshold it blocks until the
    // background compaction catches up. Without these, a write rate that
    // outruns compaction grows L0 without bound -- every read degrades and the
    // disk fills, with nothing on the write path to say so.
    // 0 = derive from l0_file_num_trigger (soft = trigger, stop = 2x trigger).
    size_t l0_slowdown_trigger = 0;
    size_t l0_stop_trigger = 0;
    // How long a blocked writer waits before re-checking. Bounded so a stuck
    // background thread surfaces as slow writes rather than a deadlock.
    uint32_t compaction_backpressure_timeout_ms = 1000;
    size_t bloom_bits_per_key = 10;
    size_t restart_interval = 16;                // prefix compression restart
    CompressionType compression = CompressionType::kNoCompression;
    std::string db_path = "./lightkv_data";
    bool create_if_missing = true;
    bool paranoid_checks = false;

    // v2.0 大 Value 分离存储（详见设计草案 7）
    size_t value_threshold = 64 * 1024;          // 超过此大小的 value 写入 vlog
    size_t vlog_file_size_limit = 512 * 1024 * 1024;  // vlog 文件大小上限
    bool vlog_gc_enabled = true;                 // 是否启用 vlog 垃圾回收（Phase 2 实装）

    // v2.0 Compaction 限速 + 优先级（详见设计草案 4.2.2）
    uint64_t compaction_rate_limit = 0;          // 0 = 不限速，单位 bytes/sec
    int compaction_priority = 0;                 // 0 = 默认优先级，正值优先，负值延后

    // v2.0 Phase 3: Raft 共识 + 集群分片（详见设计草案 11）
    bool enable_raft = false;                    // false = 单机模式, true = 分布式 Raft 模式
    uint64_t raft_node_id = 0;                   // 本节点 ID（0 = 未配置）
    std::string raft_host = "0.0.0.0";           // Raft 内部通信监听地址
    uint16_t raft_port = 16379;                  // Raft 内部通信端口（默认比 RESP 端口 +10000）

    // 集群节点列表格式："id1:host1:port1:is_voter,id2:host2:port2:is_voter"
    // 示例: "1:192.168.1.10:16379:1,2:192.168.1.11:16379:1,3:192.168.1.12:16379:1"
    std::string raft_peers_config;

    // Raft 超时参数
    int raft_election_timeout_min_ms = 150;
    int raft_election_timeout_max_ms = 300;
    int raft_heartbeat_interval_ms = 50;
};

struct ReadOptions {
    bool verify_checksums = false;
    bool fill_cache = true;

    // 0 means "latest". A non-zero value requests a point-in-time read: the
    // newest version of each key whose sequence number is <= snapshot_seq.
    //
    // The bound is INCLUSIVE. DBImpl::LastSeq() returns the next sequence number
    // to be assigned, so to pin "everything committed so far" use
    // `LastSeq() - 1`.
    //
    // SSTables persist a sequence number per entry, so this works for data that
    // has already been flushed. Note that compaction may still reclaim versions
    // that no live snapshot needs, so a very old snapshot can observe fewer
    // historical versions than were originally written.
    uint64_t snapshot_seq = 0;
};

struct WriteOptions {
    bool sync = false;
};

} // namespace lightkv