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
    // Decodes into `handle` and reports whether the input was well formed.
    //
    // This used to return a BlockHandle built from whatever the varint decoder
    // produced, including a null pointer on truncated input, so a corrupted index
    // block silently turned into bogus offsets that were then passed to Read().
    static bool DecodeFrom(const Slice& input, BlockHandle* handle);
};

} // namespace lightkv
