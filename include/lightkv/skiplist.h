#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <random>
#include <algorithm>
#include <shared_mutex>
#include <string>
#include <string_view>

#include "slice.h"

namespace lightkv {

namespace detail {

template<typename Key, typename Value>
class Arena;

// kRanked, when true, makes every node carry a per-level span: how many nodes
// sit strictly between it and the node that pointer leads to. Summing them
// along a descent yields a key's position, which is what turns "which index
// does this key sit at" and "which key sits at this index" from linear walks
// into O(log N) descents -- the reason the sorted-set index uses it. The write
// path does not: spans cost 8 bytes per level per node, and nothing on the
// memtable path asks for a rank. The parameter is compile-time, so an unranked
// list compiles to exactly what it did before: no extra bytes, no extra stores.
template<typename Key, typename Value, bool kRanked = false>
class SkipList {
    static constexpr int kMaxHeight = 12;
    static constexpr int kBranching = 4;

    struct Node {
        Key key;
        Value value;
        uint64_t seq;
        int height;
        bool is_deleted;
        std::atomic<Node*> next[1];
        // A ranked node stores `height` spans in the same allocation, directly
        // behind next[]. Span()/SetSpan() are the only things that know about
        // them: reading them when !kRanked would walk off the allocation, so
        // both go through the constexpr guard below.

        Node* Next(int n) const {
            return next[n].load(std::memory_order_acquire);
        }

        void SetNext(int n, Node* node) {
            next[n].store(node, std::memory_order_release);
        }

        uint64_t Span(int n) const {
            if constexpr (!kRanked) return 0;
            return reinterpret_cast<const std::atomic<uint64_t>*>(next + height)[n]
                .load(std::memory_order_relaxed);
        }

        void SetSpan(int n, uint64_t v) {
            if constexpr (!kRanked) return;
            reinterpret_cast<std::atomic<uint64_t>*>(next + height)[n]
                .store(v, std::memory_order_relaxed);
        }

        static size_t NodeSize(int height) {
            size_t bytes = sizeof(Node) + sizeof(std::atomic<Node*>) * (height - 1);
            if constexpr (kRanked) bytes += sizeof(uint64_t) * height;
            return bytes;
        }
    };

    std::atomic<Node*> head_;
    std::atomic<int> max_height_;
    std::mt19937 rng_;
    Arena<Key, Value>* arena_;
    mutable std::shared_mutex rw_mutex_;

    int RandomHeight() {
        int h = 1;
        while (h < kMaxHeight && (rng_() % kBranching == 0)) {
            ++h;
        }
        return h;
    }

    Node* NewNode(const Key& key, const Value& value, uint64_t seq, int height);

    bool Equal(const Key& a, const Key& b) const { return !(a < b) && !(b < a); }

public:
    explicit SkipList(Arena<Key, Value>* arena);
    ~SkipList();

    struct Iterator {
        Node* node;
        explicit Iterator(Node* n) : node(n) {}
        bool Valid() const { return node != nullptr; }
        const Key& key() const { return node->key; }
        const Value& value() const { return node->value; }
        uint64_t seq() const { return node->seq; }
        bool IsDeleted() const { return node->is_deleted; }
        void Next() { if (node) node = node->Next(0); }
    };

    void Insert(const Key& key, const Value& value, uint64_t seq);
    void InsertDeletion(const Key& key, uint64_t seq);
    // Unlinks the node holding exactly this key and returns whether there was
    // one. The node stays in the arena (nothing here can hand memory back) but
    // it is off every level, so it is no longer iterated over, no longer keeps
    // the member alive, and its key/value are destroyed to stop the strings
    // leaking.
    //
    // Unlike InsertDeletion(), this removes the entry itself, so it is only for
    // callers that hold the only live node per key and whose own lock keeps
    // iterators from outliving the call. The MVCC write path must not use it:
    // old versions have to stay readable to snapshots.
    bool Erase(const Key& key);

    // Position of the key in ascending order, or -1. O(log N) on a ranked list.
    int64_t RankOf(const Key& key) const;
    // Node at 0-based position `k`; invalid when k >= size.
    Iterator SeekToRank(uint64_t k) const;

    bool Contains(const Key& key) const;

    Iterator Find(const Key& key) const;

    Iterator SeekGE(const Key& key) const;

    // Overloads accepting std::string_view to avoid heap allocation
    void Insert(std::string_view key, std::string_view value, uint64_t seq);
    void InsertDeletion(std::string_view key, uint64_t seq);
    bool Contains(std::string_view key) const;
    Iterator Find(std::string_view key) const;
    Iterator SeekGE(std::string_view key) const;

    Iterator SeekToFirst() const {
        std::shared_lock<std::shared_mutex> lock(rw_mutex_);
        return Iterator(head_.load()->Next(0));
    }
};

// Arena is a simple memory pool for SkipList nodes
template<typename Key, typename Value>
class Arena {
    static constexpr size_t kBlockSize = 4096;
    struct Block {
        char data[kBlockSize];
        size_t used;
        Block* next;
    };
    Block* head_;
    Block* current_;

public:
    Arena() : head_(nullptr), current_(nullptr) {}

    ~Arena() {
        Block* b = head_;
        while (b) {
            Block* next = b->next;
            // Large blocks (size > kBlockSize/2) are allocated with new char[]
            // and stored with header prepended. Small blocks are allocated with new Block().
            // We detect large blocks by checking if used > kBlockSize.
            if (b->used > kBlockSize) {
                delete[] reinterpret_cast<char*>(b);
            } else {
                delete b;
            }
            b = next;
        }
    }

    char* Allocate(size_t size) {
        if (size > kBlockSize / 2) {
            // large allocation, allocate standalone block
            char* mem = new char[sizeof(Block) + size];
            auto* b = reinterpret_cast<Block*>(mem);
            b->used = size;
            b->next = head_;
            head_ = b;
            return mem + sizeof(Block);
        }
        if (!current_ || current_->used + size > kBlockSize) {
            auto* b = new Block();
            b->used = 0;
            b->next = head_;
            head_ = b;
            current_ = b;
        }
        char* ptr = current_->data + current_->used;
        current_->used += size;
        return ptr;
    }

    size_t MemoryUsage() const {
        size_t total = 0;
        Block* b = head_;
        while (b) {
            total += sizeof(Block) + (b->used > kBlockSize ? b->used - kBlockSize : 0);
            b = b->next;
        }
        return total;
    }
};

template<typename Key, typename Value, bool kRanked>
SkipList<Key, Value, kRanked>::SkipList(Arena<Key, Value>* arena)
    : max_height_(1), rng_(std::random_device{}()), arena_(arena) {
    head_ = NewNode(Key(), Value(), 0, kMaxHeight);
    for (int i = 0; i < kMaxHeight; ++i) {
        head_.load()->SetNext(i, nullptr);
    }
}

template<typename Key, typename Value, bool kRanked>
SkipList<Key, Value, kRanked>::~SkipList() {
    // Arena owns the raw memory but does not call destructors.
    // We must manually destroy Key and Value objects (e.g., std::string)
    // before the Arena releases the memory blocks.
    Node* node = head_.load(std::memory_order_acquire)->Next(0);
    while (node) {
        Node* next = node->Next(0);
        node->key.~Key();
        node->value.~Value();
        node = next;
    }
}

template<typename Key, typename Value, bool kRanked>
typename SkipList<Key, Value, kRanked>::Node*
SkipList<Key, Value, kRanked>::NewNode(const Key& key, const Value& value, uint64_t seq, int height) {
    char* mem = arena_->Allocate(Node::NodeSize(height));
    Node* node = reinterpret_cast<Node*>(mem);
    new (&node->key) Key(key);
    new (&node->value) Value(value);
    node->seq = seq;
    node->height = height;
    node->is_deleted = false;
    for (int i = 0; i < height; ++i) {
        new (&node->next[i]) std::atomic<Node*>(nullptr);
        node->SetSpan(i, 0);
    }
    return node;
}

template<typename Key, typename Value, bool kRanked>
void SkipList<Key, Value, kRanked>::Insert(const Key& key, const Value& value, uint64_t seq) {
    std::unique_lock<std::shared_mutex> lock(rw_mutex_);
    Node* prev[kMaxHeight];
    // rank[i] is the position of prev[i] at level i, where the head counts as
    // -1 so that the first real node sits at 0. Only tracked when the list is
    // ranked; the compiler drops it entirely otherwise.
    int64_t rank[kMaxHeight];
    Node* cur = head_.load(std::memory_order_acquire);
    int cur_height = max_height_.load(std::memory_order_acquire);

    for (int i = cur_height - 1; i >= 0; --i) {
        if constexpr (kRanked) rank[i] = (i == cur_height - 1) ? -1 : rank[i + 1];
        while (Node* next = cur->Next(i)) {
            if (next->key < key) {
                if constexpr (kRanked) rank[i] += static_cast<int64_t>(cur->Span(i)) + 1;
                cur = next;
            } else {
                break;
            }
        }
        prev[i] = cur;
    }

    cur = cur->Next(0);
    if (cur && Equal(cur->key, key)) {
        // Always create a new node to preserve MVCC snapshot semantics
        // Do NOT update in-place - older versions must remain visible to snapshots
    }

    int height = RandomHeight();
    if (height > cur_height) {
        for (int i = cur_height; i < height; ++i) {
            prev[i] = head_.load(std::memory_order_acquire);
            if constexpr (kRanked) rank[i] = -1;
        }
        max_height_.store(height, std::memory_order_release);
    }

    Node* node = NewNode(key, value, seq, height);
    for (int i = 0; i < height; ++i) {
        Node* next = prev[i]->Next(i);
        uint64_t span_before = 0;
        if constexpr (kRanked) span_before = prev[i]->Span(i);
        node->SetNext(i, next);
        prev[i]->SetNext(i, node);
        if constexpr (kRanked) {
            // rank[0] is the position prev[0] holds and rank[i] the one prev[i]
            // holds, so their difference is how many nodes sit between prev[i]
            // and the insertion point.
            const uint64_t skipped = static_cast<uint64_t>(rank[0] - rank[i]);
            prev[i]->SetSpan(i, skipped);
            // The rest of the old hop -- everything after the new node, up to
            // and including its old target -- now belongs to the new node. A
            // node appended at the end of a level passes nothing.
            node->SetSpan(i, next ? span_before - skipped : 0);
        }
    }

    if constexpr (kRanked) {
        // A span is a level-0 distance, so a hop that covers the new node gets
        // one longer even when the node is too short to have a pointer of its
        // own on that level. Missing this was silently dropping nodes from every
        // position lookup while the iteration order still looked perfect.
        for (int i = height; i < cur_height; ++i) {
            if (prev[i]->Next(i)) prev[i]->SetSpan(i, prev[i]->Span(i) + 1);
        }
    }
}

template<typename Key, typename Value, bool kRanked>
void SkipList<Key, Value, kRanked>::InsertDeletion(const Key& key, uint64_t seq) {
    std::unique_lock<std::shared_mutex> lock(rw_mutex_);
    Node* prev[kMaxHeight];
    Node* cur = head_.load(std::memory_order_acquire);
    int cur_height = max_height_.load(std::memory_order_acquire);

    for (int i = cur_height - 1; i >= 0; --i) {
        while (Node* next = cur->Next(i)) {
            if (next->key < key) {
                cur = next;
            } else {
                break;
            }
        }
        prev[i] = cur;
    }

    cur = cur->Next(0);
    if (cur && Equal(cur->key, key)) {
        // Always create a new node to preserve MVCC snapshot semantics
    }

    int height = RandomHeight();
    if (height > cur_height) {
        for (int i = cur_height; i < height; ++i) {
            prev[i] = head_.load(std::memory_order_acquire);
        }
        max_height_.store(height, std::memory_order_release);
    }

    Node* node = NewNode(key, Value(), seq, height);
    node->is_deleted = true;
    for (int i = 0; i < height; ++i) {
        node->SetNext(i, prev[i]->Next(i));
        prev[i]->SetNext(i, node);
    }
}

template<typename Key, typename Value, bool kRanked>
bool SkipList<Key, Value, kRanked>::Erase(const Key& key) {
    std::unique_lock<std::shared_mutex> lock(rw_mutex_);
    Node* prev[kMaxHeight];
    Node* cur = head_.load(std::memory_order_acquire);
    int cur_height = max_height_.load(std::memory_order_acquire);

    for (int i = cur_height - 1; i >= 0; --i) {
        while (Node* next = cur->Next(i)) {
            if (next->key < key) {
                cur = next;
            } else {
                break;
            }
        }
        prev[i] = cur;
    }

    Node* target = prev[0]->Next(0);
    if (!target || !Equal(target->key, key)) return false;

    const int h = target->height;
    for (int i = 0; i < h; ++i) {
        // Keys are unique in the lists that erase -- the sorted-set index
        // removes a member before re-inserting it under a new score -- so the
        // node found at level 0 is the one this level links to as well.
        prev[i]->SetNext(i, target->Next(i));
        if constexpr (kRanked) {
            // Both hops counted only the nodes strictly between their ends, and
            // the target was an end of each, never one of them -- so the merged
            // hop passes exactly what the two of them passed, and nothing when
            // the target had no successor left.
            prev[i]->SetSpan(i, target->Next(i) ? prev[i]->Span(i) + target->Span(i) : 0);
        }
    }

    if constexpr (kRanked) {
        // Same reasoning as in Insert: the hops above the node's own height
        // covered it without having a pointer to it, and now cover it no more.
        for (int i = h; i < cur_height; ++i) {
            if (prev[i]->Next(i)) prev[i]->SetSpan(i, prev[i]->Span(i) - 1);
        }
    }

    // The arena cannot hand the memory back, but the node is off every level now
    // and the list destructor will never reach it, so its key/value have to go
    // here or their heap buffers leak.
    target->key.~Key();
    target->value.~Value();
    return true;
}

template<typename Key, typename Value, bool kRanked>
int64_t SkipList<Key, Value, kRanked>::RankOf(const Key& key) const {
    static_assert(kRanked, "RankOf needs spans; instantiate the list with kRanked=true");
    std::shared_lock<std::shared_mutex> lock(rw_mutex_);
    Node* cur = head_.load(std::memory_order_acquire);
    int cur_height = max_height_.load(std::memory_order_acquire);
    // Position of `cur`; the head sits at -1 so that the first real node is 0.
    int64_t pos = -1;

    for (int i = cur_height - 1; i >= 0; --i) {
        while (Node* next = cur->Next(i)) {
            if (next->key < key) {
                pos += static_cast<int64_t>(cur->Span(i)) + 1;
                cur = next;
            } else {
                break;
            }
        }
    }

    // `cur` is the last node below the key, so the candidate sits one further.
    Node* first = cur->Next(0);
    if (!first || !Equal(first->key, key)) return -1;
    return pos + 1;
}

template<typename Key, typename Value, bool kRanked>
typename SkipList<Key, Value, kRanked>::Iterator
SkipList<Key, Value, kRanked>::SeekToRank(uint64_t k) const {
    static_assert(kRanked, "SeekToRank needs spans; instantiate the list with kRanked=true");
    std::shared_lock<std::shared_mutex> lock(rw_mutex_);
    Node* head = head_.load(std::memory_order_acquire);
    Node* cur = head;
    int cur_height = max_height_.load(std::memory_order_acquire);
    int64_t pos = -1;

    for (int i = cur_height - 1; i >= 0; --i) {
        while (Node* next = cur->Next(i)) {
            const int64_t next_pos = pos + static_cast<int64_t>(cur->Span(i)) + 1;
            if (next_pos > static_cast<int64_t>(k)) break;  // overshoots
            pos = next_pos;
            cur = next;
        }
    }

    // Level 0 is descended last and its hops advance exactly one node, so the
    // walk stops on the node at position k -- or runs out of nodes.
    if (cur == head || pos != static_cast<int64_t>(k)) return Iterator(nullptr);
    return Iterator(cur);
}

template<typename Key, typename Value, bool kRanked>
bool SkipList<Key, Value, kRanked>::Contains(const Key& key) const {
    std::shared_lock<std::shared_mutex> lock(rw_mutex_);
    Node* cur = head_.load(std::memory_order_acquire);
    int cur_height = max_height_.load(std::memory_order_acquire);

    for (int i = cur_height - 1; i >= 0; --i) {
        while (Node* next = cur->Next(i)) {
            if (next->key < key) {
                cur = next;
            } else {
                break;
            }
        }
    }
    cur = cur->Next(0);
    return cur && Equal(cur->key, key);
}

template<typename Key, typename Value, bool kRanked>
typename SkipList<Key, Value, kRanked>::Iterator
SkipList<Key, Value, kRanked>::Find(const Key& key) const {
    std::shared_lock<std::shared_mutex> lock(rw_mutex_);
    Node* cur = head_.load(std::memory_order_acquire);
    int cur_height = max_height_.load(std::memory_order_acquire);

    for (int i = cur_height - 1; i >= 0; --i) {
        while (Node* next = cur->Next(i)) {
            if (next->key < key) {
                cur = next;
            } else {
                break;
            }
        }
    }
    cur = cur->Next(0);
    if (cur && Equal(cur->key, key)) {
        return Iterator(cur);
    }
    return Iterator(nullptr);
}

template<typename Key, typename Value, bool kRanked>
typename SkipList<Key, Value, kRanked>::Iterator
SkipList<Key, Value, kRanked>::SeekGE(const Key& key) const {
    std::shared_lock<std::shared_mutex> lock(rw_mutex_);
    Node* cur = head_.load(std::memory_order_acquire);
    int cur_height = max_height_.load(std::memory_order_acquire);

    for (int i = cur_height - 1; i >= 0; --i) {
        while (Node* next = cur->Next(i)) {
            if (next->key < key) {
                cur = next;
            } else {
                break;
            }
        }
    }
    return Iterator(cur->Next(0));
}

// string_view overloads to avoid heap allocation on hot path
template<typename Key, typename Value, bool kRanked>
void SkipList<Key, Value, kRanked>::Insert(std::string_view key, std::string_view value, uint64_t seq) {
    std::unique_lock<std::shared_mutex> lock(rw_mutex_);
    Node* prev[kMaxHeight];
    Node* cur = head_.load(std::memory_order_acquire);
    int cur_height = max_height_.load(std::memory_order_acquire);

    for (int i = cur_height - 1; i >= 0; --i) {
        while (Node* next = cur->Next(i)) {
            if (std::string_view(next->key) < key) {
                cur = next;
            } else {
                break;
            }
        }
        prev[i] = cur;
    }

    int height = RandomHeight();
    if (height > cur_height) {
        for (int i = cur_height; i < height; ++i) {
            prev[i] = head_.load(std::memory_order_acquire);
        }
        max_height_.store(height, std::memory_order_release);
    }

    // Create node with actual string storage
    std::string key_str(key);
    std::string value_str(value);
    Node* node = NewNode(key_str, value_str, seq, height);
    for (int i = 0; i < height; ++i) {
        node->SetNext(i, prev[i]->Next(i));
        prev[i]->SetNext(i, node);
    }
}

template<typename Key, typename Value, bool kRanked>
void SkipList<Key, Value, kRanked>::InsertDeletion(std::string_view key, uint64_t seq) {
    std::unique_lock<std::shared_mutex> lock(rw_mutex_);
    Node* prev[kMaxHeight];
    Node* cur = head_.load(std::memory_order_acquire);
    int cur_height = max_height_.load(std::memory_order_acquire);

    for (int i = cur_height - 1; i >= 0; --i) {
        while (Node* next = cur->Next(i)) {
            if (std::string_view(next->key) < key) {
                cur = next;
            } else {
                break;
            }
        }
        prev[i] = cur;
    }

    int height = RandomHeight();
    if (height > cur_height) {
        for (int i = cur_height; i < height; ++i) {
            prev[i] = head_.load(std::memory_order_acquire);
        }
        max_height_.store(height, std::memory_order_release);
    }

    std::string key_str(key);
    Node* node = NewNode(key_str, Value(), seq, height);
    node->is_deleted = true;
    for (int i = 0; i < height; ++i) {
        node->SetNext(i, prev[i]->Next(i));
        prev[i]->SetNext(i, node);
    }
}

template<typename Key, typename Value, bool kRanked>
bool SkipList<Key, Value, kRanked>::Contains(std::string_view key) const {
    std::shared_lock<std::shared_mutex> lock(rw_mutex_);
    Node* cur = head_.load(std::memory_order_acquire);
    int cur_height = max_height_.load(std::memory_order_acquire);

    for (int i = cur_height - 1; i >= 0; --i) {
        while (Node* next = cur->Next(i)) {
            if (std::string_view(next->key) < key) {
                cur = next;
            } else {
                break;
            }
        }
    }
    cur = cur->Next(0);
    return cur && std::string_view(cur->key) == key;
}

template<typename Key, typename Value, bool kRanked>
typename SkipList<Key, Value, kRanked>::Iterator
SkipList<Key, Value, kRanked>::Find(std::string_view key) const {
    std::shared_lock<std::shared_mutex> lock(rw_mutex_);
    Node* cur = head_.load(std::memory_order_acquire);
    int cur_height = max_height_.load(std::memory_order_acquire);

    for (int i = cur_height - 1; i >= 0; --i) {
        while (Node* next = cur->Next(i)) {
            if (std::string_view(next->key) < key) {
                cur = next;
            } else {
                break;
            }
        }
    }
    cur = cur->Next(0);
    if (cur && std::string_view(cur->key) == key) {
        return Iterator(cur);
    }
    return Iterator(nullptr);
}

template<typename Key, typename Value, bool kRanked>
typename SkipList<Key, Value, kRanked>::Iterator
SkipList<Key, Value, kRanked>::SeekGE(std::string_view key) const {
    std::shared_lock<std::shared_mutex> lock(rw_mutex_);
    Node* cur = head_.load(std::memory_order_acquire);
    int cur_height = max_height_.load(std::memory_order_acquire);

    for (int i = cur_height - 1; i >= 0; --i) {
        while (Node* next = cur->Next(i)) {
            if (std::string_view(next->key) < key) {
                cur = next;
            } else {
                break;
            }
        }
    }
    return Iterator(cur->Next(0));
}

} // namespace detail

// Public SkipList wrapper using string keys
class SkipList {
public:
    using Iterator = detail::SkipList<std::string, std::string>::Iterator;

    SkipList() : arena_(new detail::Arena<std::string, std::string>()),
                 impl_(new detail::SkipList<std::string, std::string>(arena_.get())) {}

    ~SkipList() = default;

    void Insert(const Slice& key, const Slice& value, uint64_t seq) {
        // Use string_view to avoid heap allocation during lookup
        impl_->Insert(std::string_view(key.data(), key.size()),
                      std::string_view(value.data(), value.size()), seq);
    }

    void InsertDeletion(const Slice& key, uint64_t seq) {
        impl_->InsertDeletion(std::string_view(key.data(), key.size()), seq);
    }

    bool Contains(const Slice& key) const {
        return impl_->Contains(std::string_view(key.data(), key.size()));
    }

    Iterator Find(const Slice& key) const {
        return impl_->Find(std::string_view(key.data(), key.size()));
    }

    Iterator SeekGE(const Slice& key) const {
        return impl_->SeekGE(std::string_view(key.data(), key.size()));
    }

    Iterator SeekToFirst() const {
        return impl_->SeekToFirst();
    }

    size_t MemoryUsage() const {
        return arena_->MemoryUsage();
    }

private:
    std::unique_ptr<detail::Arena<std::string, std::string>> arena_;
    std::unique_ptr<detail::SkipList<std::string, std::string>> impl_;
};

} // namespace lightkv