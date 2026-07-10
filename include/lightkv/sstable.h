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

    void EncodeTo(std::string* dst) const;
    static Status DecodeFrom(const Slice& input, TableFooter* footer);
};

class SSTable {
public:
    SSTable(const Options& options, const std::string& filename, uint64_t file_id);

    Status Open();

    Status Get(const Slice& key, std::string* value, uint64_t* seq) const;

    uint64_t file_id() const { return file_id_; }
    const std::string& filename() const { return filename_; }
    uint64_t FileSize() const { return reader_ ? reader_->FileSize() : 0; }
    std::string SmallestKey() const { return smallest_key_; }
    std::string LargestKey() const { return largest_key_; }

    bool MayMatch(const Slice& key) const;

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
        void Seek(const Slice& target);
        void Next();
        Slice key() const;
        Slice value() const;
        uint64_t seq() const { return last_seq_; }

    private:
        std::shared_ptr<const SSTable> owned_table_;  // prevents destruction (may be null)
        const SSTable* table_;                         // always points to the SSTable
        int data_block_index_;
        uint64_t last_seq_;
        
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