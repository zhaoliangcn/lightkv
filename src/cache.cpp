#include "lightkv/cache.h"

namespace lightkv {

void BlockCache::Insert(uint64_t file_id, uint64_t block_offset, const BlockContents& contents) {
    CacheKey key{file_id, block_offset};
    cache_.Insert(key, contents, contents.data.size());
}

bool BlockCache::Lookup(uint64_t file_id, uint64_t block_offset, BlockContents* contents) {
    CacheKey key{file_id, block_offset};
    return cache_.Lookup(key, contents);
}

void BlockCache::Erase(uint64_t /* file_id */) {
    // Simplified: we don't track all keys by file_id yet.
    // Full implementation would need an index by file_id.
}

} // namespace lightkv