#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

namespace lightkv {

struct Options {
    size_t memtable_size = 64 * 1024 * 1024;   // 64MB
    size_t wal_file_size = 256 * 1024 * 1024;   // 256MB
    size_t block_cache_size = 512 * 1024 * 1024; // 512MB
    size_t block_size = 4 * 1024;                // 4KB
    size_t max_level = 7;
    size_t l0_file_num_trigger = 4;              // L0 compaction trigger
    size_t level_multiplier = 10;                // size ratio between levels
    size_t bloom_bits_per_key = 10;
    size_t restart_interval = 16;                // prefix compression restart
    bool enable_compression = false;
    std::string db_path = "./lightkv_data";
    bool create_if_missing = true;
    bool paranoid_checks = false;
};

struct ReadOptions {
    bool verify_checksums = false;
    bool fill_cache = true;
    uint64_t snapshot_seq = 0;  // 0 means latest
};

struct WriteOptions {
    bool sync = false;
};

} // namespace lightkv