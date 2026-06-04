#pragma once

#include "slice.h"
#include "status.h"
#include "options.h"
#include <memory>
#include <string>
#include <vector>
#include <cstdint>

namespace lightkv {

class DB {
public:
    DB() = default;
    virtual ~DB() = default;

    DB(const DB&) = delete;
    DB& operator=(const DB&) = delete;

    virtual Status Put(const WriteOptions& options, const Slice& key, const Slice& value) = 0;

    virtual Status Delete(const WriteOptions& options, const Slice& key) = 0;

    virtual Status DeleteRange(const WriteOptions& options, const Slice& begin_key, const Slice& end_key) = 0;

    virtual Status Get(const ReadOptions& options, const Slice& key, std::string* value) = 0;

    virtual bool Exists(const ReadOptions& options, const Slice& key) = 0;

    // Scan keys with given prefix, limited to `limit` results.
    // Results are returned as (key, value) pairs.
    virtual Status Scan(const ReadOptions& options, const Slice& prefix, int limit,
                        std::vector<std::pair<std::string, std::string>>* results) = 0;

    // Atomic increment: new_value = current + delta. If key doesn't exist, treat as 0.
    virtual Status Increment(const WriteOptions& options, const Slice& key, int64_t delta, int64_t* new_value) = 0;

    // Rename key from src to dst. If dst exists, overwrite it.
    virtual Status Rename(const WriteOptions& options, const Slice& src, const Slice& dst) = 0;

    virtual Status Backup(const std::string& backup_path) = 0;

    static Status Open(const Options& options, DB** dbptr);
};

} // namespace lightkv