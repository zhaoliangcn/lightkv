#include "lightkv/memtable.h"
#include <algorithm>

namespace lightkv {

MemTable::MemTable() = default;

MemTable::~MemTable() = default;

void MemTable::Insert(uint64_t seq, const Slice& key, const Slice& value) {
    table_.Insert(key, value, seq);
}

void MemTable::InsertDeletion(uint64_t seq, const Slice& key) {
    table_.InsertDeletion(key, seq);
}

bool MemTable::Get(const Slice& key, std::string* value, uint64_t snapshot_seq) const {
    // Check range tombstones first
    if (IsRangeDeleted(key)) return false;

    auto iter = table_.Find(key);
    if (!iter.Valid()) return false;
    if (iter.seq() > snapshot_seq) return false;
    if (iter.IsDeleted()) return false;
    *value = iter.value();
    return true;
}

bool MemTable::IsRangeDeleted(const Slice& key) const {
    std::lock_guard<std::mutex> lock(range_mu_);
    for (const auto& tombstone : range_tombstones_) {
        if (!(key < Slice(tombstone.begin_key)) && key < Slice(tombstone.end_key)) {
            return true;
        }
    }
    return false;
}

void MemTable::InsertRangeDeletion(uint64_t seq, const Slice& begin_key, const Slice& end_key) {
    std::lock_guard<std::mutex> lock(range_mu_);
    range_tombstones_.push_back({begin_key.ToString(), end_key.ToString(), seq});
}

size_t MemTable::ApproximateMemoryUsage() const {
    return table_.MemoryUsage();
}

bool MemTable::empty() const {
    auto iter = table_.SeekToFirst();
    return !iter.Valid();
}

} // namespace lightkv