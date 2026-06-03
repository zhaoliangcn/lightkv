#pragma once

#include "slice.h"
#include <vector>
#include <string>
#include <cstdint>

namespace lightkv {

class BloomFilter {
public:
    BloomFilter();
    explicit BloomFilter(int bits_per_key);
    BloomFilter(const uint32_t* data, size_t num_u32, int bits_per_key);

    void Add(const Slice& key);

    bool MayMatch(const Slice& key) const;

    const char* data() const { return reinterpret_cast<const char*>(bits_.data()); }
    size_t size() const { return bits_.size() * sizeof(uint32_t); }

private:
    uint32_t Hash(uint32_t h) const;
    int bits_per_key_;
    int k_; // number of hash functions
    std::vector<uint32_t> bits_;
};

} // namespace lightkv