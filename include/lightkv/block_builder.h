#pragma once

#include "slice.h"
#include <string>
#include <vector>
#include <cstdint>

namespace lightkv {

class BlockBuilder {
public:
    explicit BlockBuilder(size_t restart_interval = 16);

    void Add(const Slice& key, const Slice& value);

    Slice Finish();

    size_t CurrentSizeEstimate() const { return buffer_.size() + restarts_.size() * sizeof(uint32_t) + sizeof(uint32_t); }

    bool empty() const { return buffer_.empty(); }

    void Reset();

private:
    std::string buffer_;
    std::vector<uint32_t> restarts_;
    size_t restart_interval_;
    std::string last_key_;
    uint32_t counter_;
    bool finished_;
};

} // namespace lightkv