#include "lightkv/block_handle.h"
#include "lightkv/encoding.h"

namespace lightkv {

void BlockHandle::EncodeTo(std::string* dst) const {
    PutVarint64(dst, offset);
    PutVarint64(dst, size);
    dst->push_back(is_compressed ? 1 : 0);
}

BlockHandle BlockHandle::DecodeFrom(const Slice& input) {
    BlockHandle handle;
    const char* p = input.data();
    const char* limit = input.data() + input.size();
    p = GetVarint64(p, limit, &handle.offset);
    p = GetVarint64(p, limit, &handle.size);
    if (p < limit) {
        handle.is_compressed = (*p != 0);
        ++p;
    }
    return handle;
}

} // namespace lightkv