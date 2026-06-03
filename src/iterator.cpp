#include "lightkv/db_impl.h"
#include "lightkv/sstable.h"
#include "lightkv/memtable.h"
#include <algorithm>
#include <memory>
#include <string>
#include <vector>

namespace lightkv {

// ============================================================
// Source iterator adapters - unify MemTable and SSTable iterators
// ============================================================

class SourceIterator {
public:
    virtual ~SourceIterator() = default;
    virtual bool Valid() const = 0;
    virtual void Next() = 0;
    virtual void SeekToFirst() = 0;
    virtual void Seek(const Slice& target) = 0;
    virtual std::string key() const = 0;
    virtual std::string value() const = 0;
    virtual uint64_t seq() const = 0;
    virtual bool IsDeleted() const = 0;
};

class SkipListSourceIterator : public SourceIterator {
public:
    explicit SkipListSourceIterator(SkipList::Iterator iter) : iter_(std::move(iter)) {}
    
    bool Valid() const override { return iter_.Valid(); }
    void Next() override { iter_.Next(); }
    void SeekToFirst() override { /* already initialized */ }
    void Seek(const Slice& target) override { /* not supported directly */ }
    
    std::string key() const override { return iter_.key(); }
    std::string value() const override { return iter_.value(); }
    uint64_t seq() const override { return iter_.seq(); }
    bool IsDeleted() const override { return iter_.IsDeleted(); }

private:
    SkipList::Iterator iter_;
};

class SSTableSourceIterator : public SourceIterator {
public:
    explicit SSTableSourceIterator(const SSTable* table) : iter_(table) {}
    
    bool Valid() const override { return iter_.Valid(); }
    void Next() override { iter_.Next(); }
    void SeekToFirst() override { iter_.SeekToFirst(); }
    void Seek(const Slice& target) override { iter_.Seek(target); }
    
    std::string key() const override { return iter_.key().ToString(); }
    std::string value() const override { return iter_.value().ToString(); }
    uint64_t seq() const override { return iter_.seq(); }
    bool IsDeleted() const override { return false; }

private:
    SSTable::Iterator iter_;
};

// ============================================================
// MergingIterator
// Merges multiple sorted source iterators into a single sorted stream.
// Uses linear scan to find minimum since number of sources is typically small.
// ============================================================

class MergingIterator {
public:
    ~MergingIterator() = default;

    void AddSource(std::unique_ptr<SourceIterator> source) {
        sources_.push_back(std::move(source));
    }

    void Clear() {
        sources_.clear();
        current_ = -1;
    }

    bool Valid() const { return current_ >= 0 && current_ < static_cast<int>(sources_.size()); }

    void SeekToFirst() {
        for (auto& src : sources_) {
            src->SeekToFirst();
        }
        FindMinimum();
        SkipDeleted();
    }

    void Seek(const Slice& target) {
        for (auto& src : sources_) {
            src->Seek(target);
        }
        FindMinimum();
        SkipDeleted();
    }

    void Next() {
        if (!Valid()) return;
        
        std::string cur_key = sources_[current_]->key();
        
        // Advance all sources that are positioned at cur_key
        for (auto& src : sources_) {
            while (src->Valid() && src->key() == cur_key) {
                src->Next();
            }
        }
        
        FindMinimum();
        SkipDeleted();
    }

    std::string key() const {
        return sources_[current_]->key();
    }

    std::string value() const {
        return sources_[current_]->value();
    }

    uint64_t seq() const {
        return sources_[current_]->seq();
    }

    bool IsDeleted() const {
        return sources_[current_]->IsDeleted();
    }

private:
    // Compare two sources: returns < 0 if a < b
    int Compare(int a, int b) const {
        int cmp = sources_[a]->key().compare(sources_[b]->key());
        if (cmp != 0) return cmp;
        // Same key: higher seq wins (newer version first)
        if (sources_[a]->seq() > sources_[b]->seq()) return -1;
        if (sources_[a]->seq() < sources_[b]->seq()) return 1;
        return 0;
    }

    void FindMinimum() {
        int best = -1;
        for (int i = 0; i < static_cast<int>(sources_.size()); ++i) {
            if (!sources_[i]->Valid()) continue;
            if (best < 0 || Compare(i, best) < 0) {
                best = i;
            }
        }
        current_ = best;
    }

    void SkipDeleted() {
        while (Valid() && IsDeleted()) {
            std::string del_key = key();
            for (auto& src : sources_) {
                while (src->Valid() && src->key() == del_key) {
                    src->Next();
                }
            }
            FindMinimum();
        }
    }

    std::vector<std::unique_ptr<SourceIterator>> sources_;
    int current_ = -1;
};

// ============================================================
// DBImpl::Iterator Implementation
// ============================================================

DBImpl::Iterator::Iterator(const DBImpl* db, uint64_t snapshot_seq)
    : db_(db), snapshot_seq_(snapshot_seq), valid_(false) {
    merger_ = std::make_unique<MergingIterator>();
    db_->active_iterators_.fetch_add(1, std::memory_order_release);
}

DBImpl::Iterator::~Iterator() {
    db_->active_iterators_.fetch_sub(1, std::memory_order_release);
    db_->CleanupDeletedFiles();  // Try cleanup on destruction
}

bool DBImpl::Iterator::Valid() const {
    return valid_ && merger_->Valid();
}

void DBImpl::Iterator::SeekToFirst() {
    merger_->Clear();
    
    // Snapshot the current state under shared lock
    std::shared_lock<std::shared_mutex> lock(db_->rw_mutex_);
    
    // Add MemTable entries
    {
        auto mem_iter = db_->mem_->SeekToFirst();
        auto src = std::make_unique<SkipListSourceIterator>(std::move(mem_iter));
        merger_->AddSource(std::move(src));
    }
    
    // Add Immutable MemTable
    if (db_->imm_) {
        auto imm_iter = db_->imm_->SeekToFirst();
        auto src = std::make_unique<SkipListSourceIterator>(std::move(imm_iter));
        merger_->AddSource(std::move(src));
    }
    
    // Add SSTables from all levels
    for (int level = 0; level < 7; ++level) {
        for (const auto& table : db_->levels_[level]) {
            auto src = std::make_unique<SSTableSourceIterator>(table.get());
            merger_->AddSource(std::move(src));
        }
    }
    
    lock.unlock();
    
    merger_->SeekToFirst();
    UpdateCurrent();
}

void DBImpl::Iterator::Seek(const Slice& target) {
    merger_->Clear();
    
    // Snapshot the current state under shared lock
    std::shared_lock<std::shared_mutex> lock(db_->rw_mutex_);
    
    // Add MemTable with Seek
    {
        auto mem_iter = db_->mem_->Seek(target);
        auto src = std::make_unique<SkipListSourceIterator>(std::move(mem_iter));
        merger_->AddSource(std::move(src));
    }
    
    // Add Immutable MemTable with Seek
    if (db_->imm_) {
        auto imm_iter = db_->imm_->Seek(target);
        auto src = std::make_unique<SkipListSourceIterator>(std::move(imm_iter));
        merger_->AddSource(std::move(src));
    }
    
    // Add SSTables from all levels
    for (int level = 0; level < 7; ++level) {
        for (const auto& table : db_->levels_[level]) {
            auto src = std::make_unique<SSTableSourceIterator>(table.get());
            merger_->AddSource(std::move(src));
        }
    }
    
    lock.unlock();
    
    merger_->Seek(target);
    UpdateCurrent();
}

void DBImpl::Iterator::Next() {
    if (!Valid()) return;
    merger_->Next();
    UpdateCurrent();
}

Slice DBImpl::Iterator::key() const {
    return Slice(key_);
}

Slice DBImpl::Iterator::value() const {
    return Slice(value_);
}

void DBImpl::Iterator::UpdateCurrent() {
    if (!merger_->Valid()) {
        valid_ = false;
        key_.clear();
        value_.clear();
        return;
    }
    
    // Check snapshot visibility
    if (merger_->seq() > snapshot_seq_) {
        // The current entry is not visible in this snapshot
        // Try to find the next visible version of this key or a later key
        std::string cur_key = merger_->key();
        
        // Advance past this invisible entry
        merger_->Next();
        
        // If same key from another source has a visible version, it will be picked up
        // by the merge. But we need to check if we should skip this entire key.
        
        UpdateCurrent();
        return;
    }
    
    valid_ = true;
    key_ = merger_->key();
    value_ = merger_->value();
}

} // namespace lightkv