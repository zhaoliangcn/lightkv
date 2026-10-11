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
    const std::string& filename() const { return filename_; }

private:
    std::string filename_;
    int fd_;
    uint64_t offset_;
};

class TableBuilder {
public:
    TableBuilder(const Options& options, const std::string& filename);

    // Append one entry. `key` is the USER key; the builder appends the sequence
    // number and value type to form the internal key that is actually stored.
    //
    // Entries must be added in ascending internal-key order, i.e. sorted by
    // user_key and, for equal user keys, by descending seq. `LastKey()` returns
    // the internal key so callers can verify that ordering.
    void Add(const Slice& user_key, const Slice& value, uint64_t seq, uint8_t type);

    void Finish();

    uint64_t NumEntries() const { return num_entries_; }
    uint64_t FileSize() const { return writer_->Offset(); }

    // Internal key of the most recently added entry.
    const std::string& LastKey() const { return last_key_; }
    // User key of the first / last entry added.
    const std::string& SmallestUserKey() const { return smallest_user_key_; }
    const std::string& LargestUserKey() const { return largest_user_key_; }

    void Abandon();

    const std::string& filename() const { return writer_->filename(); }

private:
    void FlushDataBlock();

    Options options_;
    std::unique_ptr<FileWriter> writer_;
    BlockBuilder data_block_;
    BlockBuilder index_block_;
    BloomFilter bloom_filter_;
    std::string last_key_;              // internal key of the last entry
    std::string smallest_user_key_;
    std::string largest_user_key_;
    uint64_t num_entries_;
    bool finished_;

    struct DataBlockInfo {
        BlockHandle handle;
        std::string last_key;
    };
    std::vector<DataBlockInfo> data_blocks_;
};

} // namespace lightkv