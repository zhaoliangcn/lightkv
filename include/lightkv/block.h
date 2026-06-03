#pragma once

#include "slice.h"
#include <string>
#include <vector>
#include <cstdint>

namespace lightkv {

class Block {
public:
    Block() : data_(nullptr), size_(0) {}
    Block(const char* data, size_t size);

    Block(const Block&) = default;
    Block& operator=(const Block&) = default;

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
};

} // namespace lightkv