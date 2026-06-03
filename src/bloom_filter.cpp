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
    for (int i = 0; i < k_; ++i) {
        uint32_t bit_pos = h % (bits_per_key_ * 32);
        if (bit_pos / 32 >= bits_.size()) {
            bits_.resize(bit_pos / 32 + 1, 0);
        }
        bits_[bit_pos / 32] |= (1U << (bit_pos % 32));
        h += delta;
    }
}

bool BloomFilter::MayMatch(const Slice& key) const {
    if (bits_.empty()) return false;
    uint32_t h = key.Hash();
    uint32_t delta = (h >> 17) | (h << 15);
    for (int i = 0; i < k_; ++i) {
        uint32_t bit_pos = h % (bits_per_key_ * 32);
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