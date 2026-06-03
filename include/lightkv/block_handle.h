#pragma once

#include "slice.h"
#include <cstdint>
#include <string>

namespace lightkv {

struct BlockHandle {
    uint64_t offset;
    uint64_t size;

    BlockHandle() : offset(0), size(0) {}
    BlockHandle(uint64_t o, uint64_t s) : offset(o), size(s) {}

    void EncodeTo(std::string* dst) const;
    static BlockHandle DecodeFrom(const Slice& input);
};

} // namespace lightkv