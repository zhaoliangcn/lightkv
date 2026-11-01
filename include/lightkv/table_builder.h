#pragma once

#include "slice.h"
#include "block_handle.h"
#include "block_builder.h"
#include "bloom_filter.h"
#include "options.h"
#include "status.h"
#include <string>
#include <memory>
#include <cstdint>

namespace lightkv {

class FileWriter {
public:
    explicit FileWriter(const std::string& filename);
    ~FileWriter();

    bool Open();
    // Returns an error on a short or failed write. A builder that ignored these
    // used to record full block sizes in the index while only part of the block
    // reached disk, so every later read was silently corrupted.
    Status Append(const Slice& data);
    void Flush();
    // Returns the fsync result: a table that cannot be flushed must not be
    // installed in the manifest.
    Status Sync();
    void Close();
    bool is_open() const { return fd_ >= 0; }
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
    //
    // Returns an error if the table could not be written; once that happens every
    // later call is a no-op and Finish() reports the same error.
    Status Add(const Slice& user_key, const Slice& value, uint64_t seq, uint8_t type);

    // Writes the bloom filter, index, and footer, then fsyncs. Reports any write
    // failure -- callers must not install the table unless this returns OK.
    Status Finish();

    // True until a write error is recorded.
    bool ok() const { return status_.ok(); }
    const Status& status() const { return status_; }

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
    Status FlushDataBlock();

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
    Status status_;                     // first write error, sticky

    struct DataBlockInfo {
        BlockHandle handle;
        std::string last_key;
    };
    std::vector<DataBlockInfo> data_blocks_;
};

} // namespace lightkv