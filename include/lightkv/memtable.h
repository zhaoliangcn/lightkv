#pragma once

#include "slice.h"
#include "skiplist.h"
#include <cstdint>
#include <string>
#include <memory>
#include <vector>
#include <mutex>
#include <atomic>

namespace lightkv {

class MemTable {
public:
    MemTable();
    ~MemTable();

    void Insert(uint64_t seq, const Slice& key, const Slice& value);
    void InsertDeletion(uint64_t seq, const Slice& key);
    void InsertRangeDeletion(uint64_t seq, const Slice& begin_key, const Slice& end_key);

    bool Get(const Slice& key, std::string* value, uint64_t snapshot_seq) const;
    // True when a range tombstone NEWER than `version_seq` covers `key`, i.e.
    // the tombstone shadows that version. A write that happened after the
    // tombstone must stay visible.
    bool IsRangeDeleted(const Slice& key, uint64_t version_seq = 0) const;

    size_t ApproximateMemoryUsage() const;

    bool empty() const;

    // Highest sequence number inserted so far, across values, point deletions
    // and range tombstones. This is the flush boundary: once the memtable has
    // been written out as an SSTable, everything at or below MaxSeq() is durable
    // on disk and everything above it is not. Backup relies on this to pick a
    // last_seq that does not claim more than the copied files contain.
    uint64_t MaxSeq() const { return max_seq_.load(std::memory_order_acquire); }

    SkipList::Iterator SeekToFirst() { return table_.SeekToFirst(); }
    SkipList::Iterator Seek(const Slice& key) { return table_.SeekGE(key); }

    const SkipList& GetSkipList() const { return table_; }

    struct RangeTombstone {
        std::string begin_key;
        std::string end_key;
        uint64_t seq;
    };
    const std::vector<RangeTombstone>& GetRangeTombstones() const { return range_tombstones_; }

private:
    SkipList table_;
    mutable std::mutex range_mu_;
    std::vector<RangeTombstone> range_tombstones_;
    std::atomic<uint64_t> max_seq_{0};

    void NoteSeq(uint64_t seq) {
        uint64_t cur = max_seq_.load(std::memory_order_relaxed);
        while (seq > cur &&
               !max_seq_.compare_exchange_weak(cur, seq, std::memory_order_release,
                                               std::memory_order_relaxed)) {
        }
    }
};

} // namespace lightkv