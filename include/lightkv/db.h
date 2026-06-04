#pragma once

#include "slice.h"
#include "status.h"
#include "options.h"
#include <memory>
#include <string>

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

    static Status Open(const Options& options, DB** dbptr);
};

} // namespace lightkv