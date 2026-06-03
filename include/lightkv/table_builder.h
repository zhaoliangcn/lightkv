#pragma once

#include "slice.h"
#include "block_handle.h"
#include "block_builder.h"
#include "bloom_filter.h"
#include "options.h"
#include <string>
#include <memory>
#include <cstdint>

namespace lightkv {

class FileWriter {
public:
    explicit FileWriter(const std::string& filename);
    ~FileWriter();

    bool Open();
    void Append(const Slice& data);
    void Flush();
    void Sync();
    void Close();
    uint64_t Offset() const { return offset_; }

private:
    std::string filename_;
    int fd_;
    uint64_t offset_;
};

class TableBuilder {
public:
    TableBuilder(const Options& options, const std::string& filename);

    void Add(const Slice& key, const Slice& value);

    void Finish();

    uint64_t NumEntries() const { return num_entries_; }
    uint64_t FileSize() const { return writer_->Offset(); }

    void Abandon();

private:
    void FlushDataBlock();

    Options options_;
    std::unique_ptr<FileWriter> writer_;
    BlockBuilder data_block_;
    BlockBuilder index_block_;
    BloomFilter bloom_filter_;
    std::string last_key_;
    uint64_t num_entries_;
    bool finished_;

    struct DataBlockInfo {
        BlockHandle handle;
        std::string last_key;
    };
    std::vector<DataBlockInfo> data_blocks_;
};

} // namespace lightkv