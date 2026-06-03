#pragma once

#include "slice.h"
#include <string>
#include <vector>
#include <cstdint>

namespace lightkv {

class Block {
public:
    Block() : data_(nullptr), size_(0), verify_checksum_(false) {}
    Block(const char* data, size_t size, bool verify_checksum = false);

    Block(const Block&);
    Block& operator=(const Block&);

    class Iterator {
    public:
        Iterator(const Block* block, const char* data);

        bool Valid() const;
        void SeekToFirst();
        void SeekToLast();
        void Seek(const Slice& target);
        void Next();
        void Prev();

        Slice key() const;
        Slice value() const;

    private:
        const Block* block_;
        const char* data_;
        const char* limit_;
        uint32_t restart_offset_;
        uint32_t restart_index_;
        uint32_t num_restarts_;
        std::string key_buffer_;

        const char* NextEntryOffset(const char* p) const;
        void ParseEntry(const char* entry);
        uint32_t GetRestartPoint(uint32_t index) const;
    };

    Iterator NewIterator() const { return Iterator(this, data_); }

    size_t size() const { return size_; }

private:
    const char* data_;
    size_t size_;
    bool verify_checksum_;
};

} // namespace lightkv