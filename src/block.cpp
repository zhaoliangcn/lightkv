#include "lightkv/block.h"
#include "lightkv/encoding.h"
#include <algorithm>
#include <cstring>

namespace lightkv {

Block::Block(const char* data, size_t size, bool verify_checksum)
    : data_(data), size_(size), verify_checksum_(verify_checksum) {
    if (verify_checksum_ && size >= sizeof(uint32_t)) {
        uint32_t stored_crc = DecodeFixed32(data + size - sizeof(uint32_t));
        uint32_t computed_crc = Crc32c(data, size - sizeof(uint32_t));
        if (stored_crc != computed_crc) {
            // Data corruption detected - mark as empty
            size_ = 0;
        }
    }
}

Block::Block(const Block& other)
    : data_(other.data_), size_(other.size_), verify_checksum_(other.verify_checksum_) {}

Block& Block::operator=(const Block& other) {
    data_ = other.data_;
    size_ = other.size_;
    verify_checksum_ = other.verify_checksum_;
    return *this;
}

static inline const char* DecodeEntry(const char* p, const char* limit,
                                      uint32_t* shared, uint32_t* non_shared,
                                      uint32_t* value_len) {
    if (p == nullptr || p >= limit) return nullptr;
    p = GetVarint32(p, limit, shared);
    if (p == nullptr) return nullptr;
    p = GetVarint32(p, limit, non_shared);
    if (p == nullptr) return nullptr;
    return GetVarint32(p, limit, value_len);
}

Block::Iterator::Iterator(const Block* block, const char* data)
    : block_(block), data_(data) {
    uint32_t restart_count = 0;
    if (block->size_ >= 2 * sizeof(uint32_t)) {
        // With CRC32: | entries | restarts[] | num_restarts(4B) | CRC32(4B) |
        restart_count = DecodeFixed32(block->data_ + block->size_ - 2 * sizeof(uint32_t));
    }
    num_restarts_ = restart_count;
    if (restart_count > 0) {
        restart_offset_ = block->size_ - (2 + restart_count) * sizeof(uint32_t);
    } else {
        restart_offset_ = 0;
    }
    limit_ = block->data_ + restart_offset_;
    restart_index_ = 0;
}

bool Block::Iterator::Valid() const {
    return data_ >= block_->data_ && data_ < limit_;
}

uint32_t Block::Iterator::GetRestartPoint(uint32_t index) const {
    return DecodeFixed32(block_->data_ + restart_offset_ + index * sizeof(uint32_t));
}

void Block::Iterator::SeekToFirst() {
    if (num_restarts_ == 0) {
        data_ = limit_;
        return;
    }
    uint32_t offset = GetRestartPoint(0);
    data_ = block_->data_ + offset;
    ParseEntry(data_);
}

void Block::Iterator::SeekToLast() {
    if (num_restarts_ == 0) {
        data_ = limit_;
        return;
    }
    uint32_t offset = GetRestartPoint(num_restarts_ - 1);
    const char* p = block_->data_ + offset;
    while (p < limit_) {
        uint32_t shared, non_shared, value_len;
        const char* q = DecodeEntry(p, limit_, &shared, &non_shared, &value_len);
        if (!q) break;
        const char* next = q + non_shared + value_len;
        if (next >= limit_) {
            data_ = p;
            ParseEntry(p);
            return;
        }
        p = next;
    }
    data_ = p;
    ParseEntry(p);
}

void Block::Iterator::Seek(const Slice& target) {
    if (num_restarts_ == 0) {
        data_ = limit_;
        return;
    }

    uint32_t left = 0;
    uint32_t right = num_restarts_ - 1;
    while (left < right) {
        uint32_t mid = (left + right + 1) / 2;
        uint32_t region_offset = GetRestartPoint(mid);
        const char* entry = block_->data_ + region_offset;
        uint32_t shared, non_shared, value_len;
        const char* p = DecodeEntry(entry, limit_, &shared, &non_shared, &value_len);
        if (!p || p + non_shared + value_len > limit_) break;
        Slice key(p, non_shared);
        if (key.compare(target) < 0) {
            left = mid;
        } else {
            right = mid - 1;
        }
    }

    const char* p = block_->data_ + GetRestartPoint(left);
    ParseEntry(p);
    if (Slice(key_buffer_).compare(target) >= 0) {
        data_ = p;
        return;
    }

    while (Valid()) {
        const char* entry = data_;
        uint32_t shared, non_shared, value_len;
        const char* q = DecodeEntry(entry, limit_, &shared, &non_shared, &value_len);
        if (!q || q + non_shared + value_len > limit_) {
            data_ = limit_;
            return;
        }

        std::string full_key(key_buffer_.data(), shared);
        full_key.append(q, non_shared);
        key_buffer_ = std::move(full_key);

        Slice cur_key(key_buffer_);
        if (cur_key.compare(target) >= 0) {
            data_ = entry;
            ParseEntry(entry);
            return;
        }

        const char* next = q + non_shared + value_len;
        if (next >= limit_) {
            data_ = limit_;
            return;
        }
        ParseEntry(next);
        data_ = next;
    }
    data_ = limit_;
}

void Block::Iterator::Next() {
    if (!Valid()) return;
    uint32_t shared, non_shared, value_len;
    const char* p = DecodeEntry(data_, limit_, &shared, &non_shared, &value_len);
    if (!p) {
        data_ = limit_;
        return;
    }
    const char* next = p + non_shared + value_len;
    if (next >= limit_) {
        data_ = limit_;
        return;
    }
    data_ = next;
    ParseEntry(data_);
}

void Block::Iterator::Prev() {
    const char* orig = data_;
    SeekToFirst();
    if (data_ >= orig) {
        data_ = limit_;
        return;
    }
    const char* prev = data_;
    while (Valid()) {
        const char* cur = data_;
        if (cur >= orig) break;
        prev = cur;
        Next();
    }
    data_ = prev;
    ParseEntry(data_);
}

void Block::Iterator::ParseEntry(const char* entry) {
    uint32_t shared, non_shared, value_len;
    const char* p = DecodeEntry(entry, limit_, &shared, &non_shared, &value_len);
    if (!p) {
        data_ = limit_;
        return;
    }
    if (shared > key_buffer_.size()) {
        data_ = limit_;
        return;
    }
    key_buffer_.resize(shared);
    key_buffer_.append(p, non_shared);
}

Slice Block::Iterator::key() const {
    return Slice(key_buffer_);
}

Slice Block::Iterator::value() const {
    uint32_t shared, non_shared, value_len;
    const char* p = DecodeEntry(data_, limit_, &shared, &non_shared, &value_len);
    if (!p) return Slice();
    return Slice(p + non_shared, value_len);
}

} // namespace lightkv