#include "lightkv/db_impl.h"
#include "lightkv/sstable.h"
#include "lightkv/memtable.h"
#include "lightkv/encoding.h"
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

    // Tie-break used when two sources hold the same key with the same seq (only
    // possible across SSTables produced by concurrent flushes). Higher == newer.
    virtual int64_t recency() const = 0;
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
    // The memtable is by definition the newest source.
    int64_t recency() const override { return INT64_MAX; }
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
    // Deletions are persisted as a value type in the internal key.
    bool IsDeleted() const override { return iter_.type() == kTypeDeletion; }
    // File ids increase with flush order, so a higher id means a newer file.
    // Only used to break ties when two files hold the same version.
    int64_t recency() const override {
        return table_ ? static_cast<int64_t>(table_->file_id()) : 0;
    }

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

    void AddRangeTombstone(std::string begin_key, std::string end_key, uint64_t seq) {
        range_tombstones_.push_back({std::move(begin_key), std::move(end_key), seq});
    }

    void Clear() {
        sources_.clear();
        range_tombstones_.clear();
        current_ = -1;
    }

    // Skip entries of the CURRENT key that are not visible at `snapshot_seq`,
    // staying on the same key so an older visible version can still surface.
    // Unlike Next(), this advances only the winning source, not every source.
    void SkipInvisible(uint64_t snapshot_seq) {
        while (Valid() && sources_[current_]->seq() > snapshot_seq) {
            const int idx = current_;
            sources_[idx]->Next();
            FindMinimum();
            // If the merge moved to a different key, nothing more to skip here.
            if (!Valid() || sources_[current_]->key() != sources_[idx]->key()) break;
        }
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
        // Same key and equal seq (always the case for two SSTable sources,
        // because seq is not persisted): fall back to source recency so a
        // stale copy in an older file cannot shadow the current value.
        const int64_t ra = sources_[a]->recency();
        const int64_t rb = sources_[b]->recency();
        if (ra > rb) return -1;
        if (ra < rb) return 1;
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

    bool IsKeyInRangeTombstone(const std::string& key, uint64_t version_seq) const {
        for (const auto& tombstone : range_tombstones_) {
            // Only a tombstone NEWER than this version can shadow it; a write
            // made after a DELRANGE must stay visible.
            if (tombstone.seq <= version_seq) continue;
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
                range_tombstones_.push_back({key().substr(4), value(), seq()});
                std::string tombstone_key = key();
                for (auto& src : sources_) {
                    while (src->Valid() && src->key() == tombstone_key) {
                        src->Next();
                    }
                }
                FindMinimum();
                continue;
            }
            
            // Skip keys covered by range tombstones newer than this version
            if (IsKeyInRangeTombstone(key(), seq())) {
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
        uint64_t seq = 0;   // only shadows versions older than this
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

    // In-memory range tombstones must be visible to the merge too. They only
    // exist in the MemTable, so without this a range-deleted key keeps showing up
    // (in SCAN results, and in DeleteRange's deleted-key count).
    for (const auto& t : db_->mem_->GetRangeTombstones()) {
        merger_->AddRangeTombstone(t.begin_key, t.end_key, t.seq);
    }
    if (db_->imm_) {
        for (const auto& t : db_->imm_->GetRangeTombstones()) {
            merger_->AddRangeTombstone(t.begin_key, t.end_key, t.seq);
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

    // In-memory range tombstones must be visible to the merge too (see the
    // note in SeekToFirst).
    for (const auto& t : db_->mem_->GetRangeTombstones()) {
        merger_->AddRangeTombstone(t.begin_key, t.end_key, t.seq);
    }
    if (db_->imm_) {
        for (const auto& t : db_->imm_->GetRangeTombstones()) {
            merger_->AddRangeTombstone(t.begin_key, t.end_key, t.seq);
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
    
    // Check snapshot visibility: skip versions newer than the snapshot while
    // staying on the same key, so an older visible version can still surface.
    merger_->SkipInvisible(snapshot_seq_);
    if (!merger_->Valid()) {
        valid_ = false;
        key_.clear();
        value_.clear();
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