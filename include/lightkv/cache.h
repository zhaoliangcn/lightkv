#pragma once

#include "slice.h"
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <list>
#include <string>
#include <vector>

namespace lightkv {

struct BlockContents {
    std::string data;
};

// FNV hash for CacheKey-like usage
namespace detail {
template<typename T>
struct IdentityHash {
    std::size_t operator()(const T& v) const noexcept {
        return static_cast<std::size_t>(v);
    }
};
}

template<typename Key, typename Value, typename Hash = std::hash<Key>>
class LRUCache {
    struct Entry {
        Key key;
        Value value;
        size_t charge;
        typename std::list<Key>::iterator lru_it;
    };

    mutable std::mutex mutex_;
    size_t capacity_;
    size_t usage_;
    std::unordered_map<Key, Entry, Hash> table_;
    std::list<Key> lru_list_;

    void EvictOne() {
        if (lru_list_.empty()) return;
        const Key& key = lru_list_.back();
        auto it = table_.find(key);
        if (it != table_.end()) {
            usage_ -= it->second.charge;
            table_.erase(it);
        }
        lru_list_.pop_back();
    }

public:
    explicit LRUCache(size_t capacity) : capacity_(capacity), usage_(0) {}

    void Insert(const Key& key, Value value, size_t charge = 1) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = table_.find(key);
        if (it != table_.end()) {
            usage_ -= it->second.charge;
            lru_list_.erase(it->second.lru_it);
            table_.erase(it);
        }
        while (usage_ + charge > capacity_ && !lru_list_.empty()) {
            EvictOne();
        }
        if (charge > capacity_) return;
        lru_list_.push_front(key);
        Entry entry{key, std::move(value), charge, lru_list_.begin()};
        table_[key] = std::move(entry);
        usage_ += charge;
    }

    bool Lookup(const Key& key, Value* value) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = table_.find(key);
        if (it == table_.end()) return false;
        lru_list_.erase(it->second.lru_it);
        lru_list_.push_front(key);
        it->second.lru_it = lru_list_.begin();
        *value = it->second.value;
        return true;
    }

    void Erase(const Key& key) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = table_.find(key);
        if (it == table_.end()) return;
        usage_ -= it->second.charge;
        lru_list_.erase(it->second.lru_it);
        table_.erase(it);
    }

    void Prune() {
        std::lock_guard<std::mutex> lock(mutex_);
        while (usage_ > capacity_ && !lru_list_.empty()) {
            EvictOne();
        }
    }

    size_t TotalCharge() const { return usage_; }
};

class BlockCache {
public:
    struct CacheKey {
        uint64_t file_id;
        uint64_t block_offset;
        bool operator==(const CacheKey& other) const {
            return file_id == other.file_id && block_offset == other.block_offset;
        }
    };

    struct CacheKeyHash {
        size_t operator()(const CacheKey& k) const {
            return std::hash<uint64_t>{}(k.file_id) ^
                   (std::hash<uint64_t>{}(k.block_offset) << 1);
        }
    };

    BlockCache(size_t capacity) : cache_(capacity) {}

    void Insert(uint64_t file_id, uint64_t block_offset, const BlockContents& contents);

    bool Lookup(uint64_t file_id, uint64_t block_offset, BlockContents* contents);

    void Erase(uint64_t file_id);

private:
    LRUCache<CacheKey, BlockContents, CacheKeyHash> cache_;
    // Secondary index: file_id -> set of block_offsets for efficient bulk eviction
    std::mutex file_index_mutex_;
    std::unordered_map<uint64_t, std::vector<uint64_t>> file_index_;
};

} // namespace lightkv