#include "lightkv/cache.h"

namespace lightkv {

void BlockCache::Insert(uint64_t file_id, uint64_t block_offset, const BlockContents& contents) {
    CacheKey key{file_id, block_offset};
    cache_.Insert(key, contents, contents.data.size());
    // Track file_id -> block_offset for efficient bulk eviction
    {
        std::lock_guard<std::mutex> lock(file_index_mutex_);
        file_index_[file_id].push_back(block_offset);
    }
}

bool BlockCache::Lookup(uint64_t file_id, uint64_t block_offset, BlockContents* contents) {
    CacheKey key{file_id, block_offset};
    return cache_.Lookup(key, contents);
}

void BlockCache::Erase(uint64_t file_id) {
    std::lock_guard<std::mutex> lock(file_index_mutex_);
    auto it = file_index_.find(file_id);
    if (it == file_index_.end()) return;
    for (uint64_t offset : it->second) {
        CacheKey key{file_id, offset};
        cache_.Erase(key);
    }
    file_index_.erase(it);
}

} // namespace lightkv