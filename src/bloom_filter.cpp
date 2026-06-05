#include "lightkv/bloom_filter.h"
#include "lightkv/encoding.h"
#include <algorithm>

namespace lightkv {

BloomFilter::BloomFilter() : bits_per_key_(10), k_(7) {}

BloomFilter::BloomFilter(int bits_per_key)
    : bits_per_key_(bits_per_key) {
    k_ = static_cast<int>(bits_per_key * 0.69);
    if (k_ < 1) k_ = 1;
    if (k_ > 30) k_ = 30;
    // Pre-allocate a reasonable initial size (enough for ~4000 keys at 10 bits/key)
    // This avoids the dynamic growth problem which breaks bloom filter consistency
    size_t initial_words = 1024; // 1024 * 32 = 32768 bits
    bits_.resize(initial_words, 0);
}

BloomFilter::BloomFilter(const uint32_t* data, size_t num_u32, int bits_per_key)
    : bits_per_key_(bits_per_key),
      bits_(data, data + num_u32) {
    k_ = static_cast<int>(bits_per_key * 0.69);
    if (k_ < 1) k_ = 1;
    if (k_ > 30) k_ = 30;
}

void BloomFilter::Add(const Slice& key) {
    uint32_t h = key.Hash();
    uint32_t delta = (h >> 17) | (h << 15);
    uint32_t total_bits = static_cast<uint32_t>(bits_.size() * 32);
    for (int i = 0; i < k_; ++i) {
        uint32_t bit_pos = h % total_bits;
        bits_[bit_pos / 32] |= (1U << (bit_pos % 32));
        h += delta;
    }
}

bool BloomFilter::MayMatch(const Slice& key) const {
    if (bits_.empty()) return false;
    uint32_t h = key.Hash();
    uint32_t delta = (h >> 17) | (h << 15);
    uint32_t total_bits = static_cast<uint32_t>(bits_.size() * 32);
    for (int i = 0; i < k_; ++i) {
        uint32_t bit_pos = h % total_bits;
        if (bit_pos / 32 >= bits_.size()) return false;
        if ((bits_[bit_pos / 32] & (1U << (bit_pos % 32))) == 0) return false;
        h += delta;
    }
    return true;
}

uint32_t BloomFilter::Hash(uint32_t h) const {
    return h;
}

} // namespace lightkv