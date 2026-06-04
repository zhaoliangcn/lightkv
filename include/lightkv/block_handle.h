#pragma once

#include "slice.h"
#include <cstdint>
#include <string>

namespace lightkv {

struct BlockHandle {
    uint64_t offset;
    uint64_t size;  // compressed size if is_compressed, otherwise original size
    bool is_compressed;

    BlockHandle() : offset(0), size(0), is_compressed(false) {}
    BlockHandle(uint64_t o, uint64_t s, bool compressed = false)
        : offset(o), size(s), is_compressed(compressed) {}

    void EncodeTo(std::string* dst) const;
    static BlockHandle DecodeFrom(const Slice& input);
};

} // namespace lightkv
