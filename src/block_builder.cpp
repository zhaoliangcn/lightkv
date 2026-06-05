#include "lightkv/block_builder.h"
#include "lightkv/encoding.h"
#include <algorithm>

namespace lightkv {

BlockBuilder::BlockBuilder(size_t restart_interval)
    : restart_interval_(restart_interval), counter_(0), finished_(false) {
    // First entry is always a restart point
    restarts_.push_back(0);
}

void BlockBuilder::Add(const Slice& key, const Slice& value) {
    size_t shared = 0;
    if (counter_ < restart_interval_) {
        const size_t min_len = std::min(last_key_.size(), key.size());
        while (shared < min_len && last_key_[shared] == key[shared]) {
            ++shared;
        }
    } else {
        // New restart point - shared must be 0
        restarts_.push_back(static_cast<uint32_t>(buffer_.size()));
        counter_ = 0;
        shared = 0;
    }

    uint32_t non_shared = static_cast<uint32_t>(key.size() - shared);
    uint32_t value_len = static_cast<uint32_t>(value.size());

    PutVarint32(&buffer_, static_cast<uint32_t>(shared));
    PutVarint32(&buffer_, non_shared);
    PutVarint32(&buffer_, value_len);

    buffer_.append(key.data() + shared, non_shared);
    buffer_.append(value.data(), value.size());

    last_key_.assign(key.data(), key.size());
    ++counter_;
}

Slice BlockBuilder::Finish() {
    if (!finished_) {
        for (uint32_t restart : restarts_) {
            PutFixed32(&buffer_, restart);
        }
        PutFixed32(&buffer_, static_cast<uint32_t>(restarts_.size()));
        // Append CRC32 covering all block data (entries + restart metadata)
        uint32_t crc = Crc32c(buffer_.data(), buffer_.size());
        PutFixed32(&buffer_, crc);
        finished_ = true;
    }
    return Slice(buffer_);
}

void BlockBuilder::Reset() {
    buffer_.clear();
    restarts_.clear();
    last_key_.clear();
    counter_ = 0;
    finished_ = false;
    // First entry is always a restart point
    restarts_.push_back(0);
}

} // namespace lightkv