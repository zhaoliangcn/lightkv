#include "lightkv/memtable.h"
#include <algorithm>

namespace lightkv {

MemTable::MemTable() = default;

MemTable::~MemTable() = default;

void MemTable::Insert(uint64_t seq, const Slice& key, const Slice& value) {
    table_.Insert(key, value, seq);
    NoteSeq(seq);
}

void MemTable::InsertDeletion(uint64_t seq, const Slice& key) {
    table_.InsertDeletion(key, seq);
    NoteSeq(seq);
}

bool MemTable::Get(const Slice& key, std::string* value, uint64_t snapshot_seq) const {
    // Locate the newest version of this key visible in the snapshot.
    auto iter = table_.Find(key);
    bool found = false;
    bool deleted = false;
    uint64_t version_seq = 0;
    while (iter.Valid() && iter.key() == key.ToString()) {
        if (iter.seq() <= snapshot_seq) {
            version_seq = iter.seq();
            deleted = iter.IsDeleted();
            found = true;
            break;
        }
        iter.Next();
    }

    if (found && deleted) return false;

    // A range tombstone only shadows versions OLDER than itself. Blindly
    // treating any covering tombstone as a delete would make a key written
    // AFTER a DELRANGE invisible, i.e. DELRANGE would permanently block the
    // key range.
    if (found && IsRangeDeleted(key, version_seq)) return false;

    if (!found) return false;

    iter = table_.Find(key);
    while (iter.Valid() && iter.key() == key.ToString()) {
        if (iter.seq() <= snapshot_seq) {
            *value = iter.value();
            return true;
        }
        iter.Next();
    }
    return false;
}

bool MemTable::IsRangeDeleted(const Slice& key, uint64_t version_seq) const {
    std::lock_guard<std::mutex> lock(range_mu_);
    for (const auto& tombstone : range_tombstones_) {
        if (tombstone.seq > version_seq &&
            !(key < Slice(tombstone.begin_key)) && key < Slice(tombstone.end_key)) {
            return true;
        }
    }
    return false;
}

void MemTable::InsertRangeDeletion(uint64_t seq, const Slice& begin_key, const Slice& end_key) {
    std::lock_guard<std::mutex> lock(range_mu_);
    range_tombstones_.push_back({begin_key.ToString(), end_key.ToString(), seq});
    NoteSeq(seq);
}

size_t MemTable::ApproximateMemoryUsage() const {
    return table_.MemoryUsage();
}

bool MemTable::empty() const {
    auto iter = table_.SeekToFirst();
    return !iter.Valid();
}

} // namespace lightkv