#pragma once

#include "slice.h"
#include "skiplist.h"
#include <cstdint>
#include <string>
#include <memory>

namespace lightkv {

class MemTable {
public:
    MemTable();
    ~MemTable();

    void Insert(uint64_t seq, const Slice& key, const Slice& value);
    void InsertDeletion(uint64_t seq, const Slice& key);

    bool Get(const Slice& key, std::string* value, uint64_t snapshot_seq) const;

    size_t ApproximateMemoryUsage() const;

    bool empty() const;

    SkipList::Iterator SeekToFirst() { return table_.SeekToFirst(); }
    SkipList::Iterator Seek(const Slice& key) { return table_.SeekGE(key); }

private:
    SkipList table_;
};

} // namespace lightkv