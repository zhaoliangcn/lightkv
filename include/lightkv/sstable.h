#pragma once

#include "slice.h"
#include "block_handle.h"
#include "bloom_filter.h"
#include "block.h"
#include "cache.h"
#include "options.h"
#include "status.h"
#include <string>
#include <memory>
#include <cstdint>
#include <vector>

namespace lightkv {

class FileReader {
public:
    explicit FileReader(const std::string& filename);
    ~FileReader();

    bool Open();
    Status Read(uint64_t offset, size_t n, Slice* result, std::string* scratch) const;
    size_t FileSize() const { return file_size_; }

private:
    std::string filename_;
    int fd_;
    size_t file_size_;
    void* mmap_base_;
};

struct TableFooter {
    uint64_t index_block_handle_offset;
    uint64_t meta_index_block_handle_offset;

    static constexpr size_t kEncodedLength = 2 * sizeof(uint64_t) + 8; // two handles + magic
    // Bumped when the on-disk block key encoding changed (v2 stores internal keys
    // carrying a sequence number; v1 stored bare user keys and cannot be read
    // correctly by this build).
    static constexpr const char* kMagic = "LIGHTKV2";

    void EncodeTo(std::string* dst) const;
    static Status DecodeFrom(const Slice& input, TableFooter* footer);
};

class SSTable {
public:
    SSTable(const Options& options, const std::string& filename, uint64_t file_id);

    Status Open();

    // Look up the newest version of `user_key` visible at `snapshot_seq`.
    //
    // Returns NotFound only when NO version is visible. A visible deletion is
    // reported as OK with *type == kTypeDeletion, so the caller can compare
    // sequence numbers across overlapping files and let a newer tombstone
    // shadow an older value.
    Status Get(const Slice& user_key, uint64_t snapshot_seq,
               std::string* value, uint64_t* seq, uint8_t* type) const;

    uint64_t file_id() const { return file_id_; }
    const std::string& filename() const { return filename_; }
    uint64_t FileSize() const { return reader_ ? reader_->FileSize() : 0; }
    // User-key ranges (trailers stripped), as recorded in the manifest.
    const std::string& SmallestKey() const { return smallest_key_; }
    const std::string& LargestKey() const { return largest_key_; }

    // `key` is a USER key: the filter is populated with user keys.
    bool MayMatch(const Slice& user_key) const;

    class Iterator {
    public:
        // Constructor for external use (holds shared_ptr to prevent destruction)
        Iterator(const std::shared_ptr<const SSTable>& table);
        // Constructor for internal use (raw pointer, caller保证SSTable存活)
        Iterator(const SSTable* table);
        ~Iterator() = default;

        bool Valid() const;
        void SeekToFirst();
        void SeekToLast();
        // Positions on the first internal key >= `internal_target`. The target
        // must be an internal key; use SeekInternalKey() to build one.
        void Seek(const Slice& internal_target);
        void Next();

        // USER key of the current entry (trailer stripped).
        Slice key() const;
        // Raw internal key, for diagnostics and for writing compaction output.
        Slice internal_key() const;
        Slice value() const;
        uint64_t seq() const;
        uint8_t type() const;

    private:
        std::shared_ptr<const SSTable> owned_table_;  // prevents destruction (may be null)
        const SSTable* table_;                         // always points to the SSTable
        int data_block_index_;

        // Current block state - kept alive for iterator
        std::string block_data_;
        std::unique_ptr<Block> block_;
        std::unique_ptr<Block::Iterator> iter_;

        void SwitchToBlock(int index);
    };

    Iterator NewIterator() const { return Iterator(this); }

private:
    Status ReadBlock(const BlockHandle& handle, BlockContents* result) const;

    Options options_;
    std::string filename_;
    uint64_t file_id_;
    std::unique_ptr<FileReader> reader_;
    BloomFilter bloom_filter_;
    std::string index_data_;
    Block index_block_;
    std::vector<BlockHandle> data_block_handles_;
    std::string smallest_key_;
    std::string largest_key_;
};

} // namespace lightkv