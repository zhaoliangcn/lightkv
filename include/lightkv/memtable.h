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

    // What a lookup found. `Deleted` is NOT the same as `NotFound`: it means this
    // table holds the newest visible version of the key and that version is a
    // tombstone, so an older version sitting in an SSTable must stay hidden.
    // Collapsing the two is what resurrected point-deleted keys once their value
    // had been flushed.
    enum class Lookup { NotFound, Deleted, Value };

    // `out_seq` receives the sequence number of the version reported.
    Lookup Find(const Slice& key, std::string* value, uint64_t snapshot_seq,
                uint64_t* out_seq = nullptr) const;

    // Convenience wrapper: true only when a live value is visible.
    bool Get(const Slice& key, std::string* value, uint64_t snapshot_seq) const {
        return Find(key, value, snapshot_seq) == Lookup::Value;
    }
    // True when a range tombstone shadows the version `version_seq` of `key`:
    // the tombstone must be NEWER than that version (a write after the DELRANGE
    // stays visible) and VISIBLE to the reader (a DELRANGE issued after an older
    // snapshot was taken must not hide anything from that snapshot).
    bool IsRangeDeleted(const Slice& key, uint64_t version_seq = 0,
                        uint64_t snapshot_seq = UINT64_MAX) const;

    size_t ApproximateMemoryUsage() const;

    bool empty() const;

    // True when a flush would have something to write out.
    //
    // Deliberately NOT the same as `!empty()`: the skip list holds point writes
    // and point deletions, while range tombstones live in their own vector. A
    // memtable carrying only a DELRANGE is `empty()` but very much has content,
    // and dropping it would resurrect everything the range delete covered.
    bool HasContent() const;

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
    // Returns a copy on purpose. Readers (the merging iterator seeds its list
    // from here) used to take a reference to the live vector while writers were
    // pushing into it under range_mu_ -- a reallocation mid-iteration, i.e. a
    // dangling read. There are only a handful of tombstones per memtable, so the
    // copy is cheap.
    std::vector<RangeTombstone> GetRangeTombstones() const {
        std::lock_guard<std::mutex> lock(range_mu_);
        return range_tombstones_;
    }

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