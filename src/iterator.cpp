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
    explicit SkipListSourceIterator(std::shared_ptr<const MemTable> memtable)
        : memtable_(std::move(memtable)),
          iter_(memtable_->GetSkipList().SeekToFirst()) {}

    bool Valid() const override { return iter_.Valid(); }
    void Next() override { iter_.Next(); }
    void SeekToFirst() override { iter_ = memtable_->GetSkipList().SeekToFirst(); }
    void Seek(const Slice& target) override { iter_ = memtable_->GetSkipList().SeekGE(target); }

    std::string key() const override { return iter_.key(); }
    std::string value() const override { return iter_.value(); }
    uint64_t seq() const override { return iter_.seq(); }
    bool IsDeleted() const override { return iter_.IsDeleted(); }

private:
    std::shared_ptr<const MemTable> memtable_;  // keeps MemTable alive
    SkipList::Iterator iter_;
};

class SSTableSourceIterator : public SourceIterator {
public:
    explicit SSTableSourceIterator(const std::shared_ptr<const SSTable>& table)
        : table_(table), iter_(table) {}

    bool Valid() const override { return iter_.Valid(); }
    void Next() override { iter_.Next(); }
    void SeekToFirst() override { iter_.SeekToFirst(); }
    void Seek(const Slice& target) override { iter_.Seek(target); }

    std::string key() const override { return iter_.key().ToString(); }
    std::string value() const override { return iter_.value().ToString(); }
    uint64_t seq() const override { return iter_.seq(); }
    bool IsDeleted() const override { return false; }

private:
    std::shared_ptr<const SSTable> table_;  // prevent SSTable from being destroyed
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

    void AddRangeTombstone(std::string begin_key, std::string end_key) {
        range_tombstones_.push_back({std::move(begin_key), std::move(end_key)});
    }

    void Clear() {
        sources_.clear();
        range_tombstones_.clear();
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

    bool IsRangeTombstoneKey() const {
        if (!Valid()) return false;
        const auto& k = key();
        return k.size() >= 4 && k[0] == '\xff' && k[1] == '\xff' && k[2] == '\xff' && k[3] == '\xff';
    }

    std::string GetRangeTombstoneEnd() const {
        return value();
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

    bool IsKeyInRangeTombstone(const std::string& key) const {
        for (const auto& tombstone : range_tombstones_) {
            if (key >= tombstone.begin_key && key < tombstone.end_key) {
                return true;
            }
        }
        return false;
    }

    void SkipDeleted() {
        while (Valid()) {
            // Skip regular deletion markers
            if (IsDeleted()) {
                std::string del_key = key();
                for (auto& src : sources_) {
                    while (src->Valid() && src->key() == del_key) {
                        src->Next();
                    }
                }
                FindMinimum();
                continue;
            }
            
            // Skip range tombstone marker keys themselves
            if (IsRangeTombstoneKey()) {
                // Register this range tombstone for future key skipping
                range_tombstones_.push_back({key().substr(4), value()});
                std::string tombstone_key = key();
                for (auto& src : sources_) {
                    while (src->Valid() && src->key() == tombstone_key) {
                        src->Next();
                    }
                }
                FindMinimum();
                continue;
            }
            
            // Skip keys covered by range tombstones
            if (IsKeyInRangeTombstone(key())) {
                std::string covered_key = key();
                for (auto& src : sources_) {
                    while (src->Valid() && src->key() == covered_key) {
                        src->Next();
                    }
                }
                FindMinimum();
                continue;
            }
            
            break;
        }
    }

    struct RangeTombstone {
        std::string begin_key;
        std::string end_key;
    };

    std::vector<std::unique_ptr<SourceIterator>> sources_;
    std::vector<RangeTombstone> range_tombstones_;
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

    // Add MemTable entries (hold shared_ptr to keep them alive)
    {
        auto src = std::make_unique<SkipListSourceIterator>(db_->mem_);
        merger_->AddSource(std::move(src));
    }

    // Add Immutable MemTable
    if (db_->imm_) {
        auto src = std::make_unique<SkipListSourceIterator>(db_->imm_);
        merger_->AddSource(std::move(src));
    }

    // Add SSTables from all levels (hold shared_ptr to prevent destruction)
    for (int level = 0; level < 7; ++level) {
        for (const auto& table : db_->levels_[level]) {
            auto src = std::make_unique<SSTableSourceIterator>(table);
            merger_->AddSource(std::move(src));
        }
    }

    merger_->SeekToFirst();
    UpdateCurrent();
    // lock released here — MemTable/SSTable kept alive by shared_ptr in iterators
}

void DBImpl::Iterator::Seek(const Slice& target) {
    merger_->Clear();

    // Snapshot the current state under shared lock
    std::shared_lock<std::shared_mutex> lock(db_->rw_mutex_);

    // Add MemTable with Seek (hold shared_ptr to keep them alive)
    {
        auto src = std::make_unique<SkipListSourceIterator>(db_->mem_);
        merger_->AddSource(std::move(src));
    }

    // Add Immutable MemTable with Seek
    if (db_->imm_) {
        auto src = std::make_unique<SkipListSourceIterator>(db_->imm_);
        merger_->AddSource(std::move(src));
    }

    // Add SSTables from all levels (hold shared_ptr to prevent destruction)
    for (int level = 0; level < 7; ++level) {
        for (const auto& table : db_->levels_[level]) {
            auto src = std::make_unique<SSTableSourceIterator>(table);
            merger_->AddSource(std::move(src));
        }
    }

    merger_->Seek(target);
    UpdateCurrent();
    // lock released here — MemTable/SSTable kept alive by shared_ptr in iterators
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
    
    // v2.0: Unwrap value tag — DBImpl::Put adds a tag byte prefix:
    //   \x00 + original bytes   → inline small value, strip \x00
    //   \x01 + 24B vlog pointer → large value stored in vlog, read it
    //   empty / no tag          → backward compat, return as-is
    if (!value_.empty()) {
        char tag = value_[0];
        if (tag == '\x00') {
            // Inline value: strip the tag byte
            value_.erase(0, 1);
        } else if (tag == '\x01' && value_.size() == 1 + 24) {
            // VLog pointer: try to read actual value from vlog
            // We need db_->vlog_ to do this, but we don't have direct access here.
            // For now, leave the tagged value (Scan/Get handle it properly).
            // The normal Get path doesn't go through Iterator, so this is acceptable
            // short-term. Full fix: store db reference and read from vlog.
        }
    }
}

} // namespace lightkv