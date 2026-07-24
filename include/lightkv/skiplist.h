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

template<typename Key, typename Value>
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

        Node* Next(int n) const {
            return next[n].load(std::memory_order_acquire);
        }

        void SetNext(int n, Node* node) {
            next[n].store(node, std::memory_order_release);
        }

        static size_t NodeSize(int height) {
            return sizeof(Node) + sizeof(std::atomic<Node*>) * (height - 1);
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

template<typename Key, typename Value>
SkipList<Key, Value>::SkipList(Arena<Key, Value>* arena)
    : max_height_(1), rng_(std::random_device{}()), arena_(arena) {
    head_ = NewNode(Key(), Value(), 0, kMaxHeight);
    for (int i = 0; i < kMaxHeight; ++i) {
        head_.load()->SetNext(i, nullptr);
    }
}

template<typename Key, typename Value>
SkipList<Key, Value>::~SkipList() {
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

template<typename Key, typename Value>
typename SkipList<Key, Value>::Node*
SkipList<Key, Value>::NewNode(const Key& key, const Value& value, uint64_t seq, int height) {
    char* mem = arena_->Allocate(Node::NodeSize(height));
    Node* node = reinterpret_cast<Node*>(mem);
    new (&node->key) Key(key);
    new (&node->value) Value(value);
    node->seq = seq;
    node->height = height;
    node->is_deleted = false;
    for (int i = 0; i < height; ++i) {
        new (&node->next[i]) std::atomic<Node*>(nullptr);
    }
    return node;
}

template<typename Key, typename Value>
void SkipList<Key, Value>::Insert(const Key& key, const Value& value, uint64_t seq) {
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
        // Do NOT update in-place - older versions must remain visible to snapshots
    }

    int height = RandomHeight();
    if (height > cur_height) {
        for (int i = cur_height; i < height; ++i) {
            prev[i] = head_.load(std::memory_order_acquire);
        }
        max_height_.store(height, std::memory_order_release);
    }

    Node* node = NewNode(key, value, seq, height);
    for (int i = 0; i < height; ++i) {
        node->SetNext(i, prev[i]->Next(i));
        prev[i]->SetNext(i, node);
    }
}

template<typename Key, typename Value>
void SkipList<Key, Value>::InsertDeletion(const Key& key, uint64_t seq) {
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

template<typename Key, typename Value>
bool SkipList<Key, Value>::Contains(const Key& key) const {
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

template<typename Key, typename Value>
typename SkipList<Key, Value>::Iterator
SkipList<Key, Value>::Find(const Key& key) const {
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

template<typename Key, typename Value>
typename SkipList<Key, Value>::Iterator
SkipList<Key, Value>::SeekGE(const Key& key) const {
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
template<typename Key, typename Value>
void SkipList<Key, Value>::Insert(std::string_view key, std::string_view value, uint64_t seq) {
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

template<typename Key, typename Value>
void SkipList<Key, Value>::InsertDeletion(std::string_view key, uint64_t seq) {
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

template<typename Key, typename Value>
bool SkipList<Key, Value>::Contains(std::string_view key) const {
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

template<typename Key, typename Value>
typename SkipList<Key, Value>::Iterator
SkipList<Key, Value>::Find(std::string_view key) const {
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

template<typename Key, typename Value>
typename SkipList<Key, Value>::Iterator
SkipList<Key, Value>::SeekGE(std::string_view key) const {
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