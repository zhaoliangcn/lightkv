#pragma once

#include "slice.h"
#include "skiplist.h"
#include <cstdint>
#include <string>
#include <memory>
#include <vector>
#include <mutex>

namespace lightkv {

class MemTable {
public:
    MemTable();
    ~MemTable();

    void Insert(uint64_t seq, const Slice& key, const Slice& value);
    void InsertDeletion(uint64_t seq, const Slice& key);
    void InsertRangeDeletion(uint64_t seq, const Slice& begin_key, const Slice& end_key);

    bool Get(const Slice& key, std::string* value, uint64_t snapshot_seq) const;
    bool IsRangeDeleted(const Slice& key) const;

    size_t ApproximateMemoryUsage() const;

    bool empty() const;

    SkipList::Iterator SeekToFirst() { return table_.SeekToFirst(); }
    SkipList::Iterator Seek(const Slice& key) { return table_.SeekGE(key); }

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
};

} // namespace lightkv