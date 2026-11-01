#include "lightkv/block_handle.h"
#include "lightkv/encoding.h"

namespace lightkv {

void BlockHandle::EncodeTo(std::string* dst) const {
    PutVarint64(dst, offset);
    PutVarint64(dst, size);
    dst->push_back(is_compressed ? 1 : 0);
}

bool BlockHandle::DecodeFrom(const Slice& input, BlockHandle* handle) {
    if (handle == nullptr) return false;
    const char* p = input.data();
    const char* limit = input.data() + input.size();

    // Every decode is checked: GetVarint64 returns nullptr on truncated input, and
    // comparing that nullptr against `limit` is undefined behaviour.
    p = GetVarint64(p, limit, &handle->offset);
    if (p == nullptr) return false;
    p = GetVarint64(p, limit, &handle->size);
    if (p == nullptr) return false;
    if (p < limit) {
        handle->is_compressed = (*p != 0);
        ++p;
    } else {
        handle->is_compressed = false;
    }
    return true;
}

} // namespace lightkv