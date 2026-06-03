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
    auto iter = table_.Find(key);
    if (!iter.Valid()) return false;
    if (iter.seq() > snapshot_seq) return false;
    if (iter.IsDeleted()) return false;
    *value = iter.value();
    return true;
}

size_t MemTable::ApproximateMemoryUsage() const {
    return table_.MemoryUsage();
}

bool MemTable::empty() const {
    auto iter = table_.SeekToFirst();
    return !iter.Valid();
}

} // namespace lightkv