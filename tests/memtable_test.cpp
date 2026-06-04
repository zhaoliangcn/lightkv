#include "lightkv/memtable.h"
#include <iostream>
#include <cassert>
#include <string>

void TestMemTableInsertGet() {
    lightkv::MemTable mem;

    mem.Insert(1, "hello", "world");
    mem.Insert(2, "foo", "bar");
    mem.Insert(3, "hello", "updated");

    std::string value;
    assert(mem.Get("hello", &value, 3));
    assert(value == "updated");

    assert(mem.Get("foo", &value, 3));
    assert(value == "bar");

    assert(!mem.Get("nonexistent", &value, 100));
}

void TestMemTableDeletion() {
    lightkv::MemTable mem;

    mem.Insert(1, "key", "value");
    mem.InsertDeletion(2, "key");

    std::string value;
    assert(!mem.Get("key", &value, 2));
}

void TestMemoryUsage() {
    lightkv::MemTable mem;
    for (int i = 0; i < 1000; ++i) {
        mem.Insert(static_cast<uint64_t>(i), "key" + std::to_string(i), "value" + std::to_string(i));
    }
    size_t usage = mem.ApproximateMemoryUsage();
    assert(usage > 0);
    std::cout << "MemTable memory usage for 1000 entries: " << usage << " bytes" << std::endl;
}

void TestMemTableSeek() {
    lightkv::MemTable mem;

    mem.Insert(1, "aaa", "1");
    mem.Insert(2, "ccc", "3");
    mem.Insert(3, "eee", "5");
    mem.Insert(4, "ggg", "7");

    // Seek to exact key
    auto iter = mem.Seek("ccc");
    assert(iter.Valid());
    assert(iter.key() == "ccc");

    // Seek to non-existent key (should find next)
    iter = mem.Seek("ddd");
    assert(iter.Valid());
    assert(iter.key() == "eee");

    // Seek past all keys
    iter = mem.Seek("zzz");
    assert(!iter.Valid());

    // Seek before first key
    iter = mem.Seek("aaa");
    assert(iter.Valid());
    assert(iter.key() == "aaa");
}

void TestMemTableSeekToFirst() {
    lightkv::MemTable mem;

    mem.Insert(1, "ccc", "3");
    mem.Insert(2, "aaa", "1");
    mem.Insert(3, "bbb", "2");

    auto iter = mem.SeekToFirst();
    assert(iter.Valid());
    assert(iter.key() == "aaa");
    iter.Next();
    assert(iter.Valid());
    assert(iter.key() == "bbb");
    iter.Next();
    assert(iter.Valid());
    assert(iter.key() == "ccc");
}

void TestEmptyMemTable() {
    lightkv::MemTable mem;

    assert(mem.empty());

    std::string value;
    assert(!mem.Get("key", &value, 1));

    auto iter = mem.SeekToFirst();
    assert(!iter.Valid());

    iter = mem.Seek("key");
    assert(!iter.Valid());

    assert(mem.ApproximateMemoryUsage() > 0);  // Arena overhead
}

void TestMemTableSnapshotVisibility() {
    lightkv::MemTable mem;

    mem.Insert(1, "key", "v1");
    mem.Insert(5, "key", "v5");
    mem.Insert(10, "key", "v10");

    std::string value;

    // Snapshot at seq 3 should see v1
    assert(mem.Get("key", &value, 3));
    assert(value == "v1");

    // Snapshot at seq 7 should see v5
    assert(mem.Get("key", &value, 7));
    assert(value == "v5");

    // Snapshot at seq 100 should see v10
    assert(mem.Get("key", &value, 100));
    assert(value == "v10");
}

void TestMemTableDeletionWithSnapshot() {
    lightkv::MemTable mem;

    mem.Insert(1, "key", "value");
    mem.InsertDeletion(5, "key");
    mem.Insert(10, "key", "new_value");

    std::string value;

    // Snapshot at seq 3 should see "value"
    assert(mem.Get("key", &value, 3));
    assert(value == "value");

    // Snapshot at seq 7 should not see key (deleted)
    assert(!mem.Get("key", &value, 7));

    // Snapshot at seq 100 should see "new_value"
    assert(mem.Get("key", &value, 100));
    assert(value == "new_value");
}

int main() {
    TestMemTableInsertGet();
    TestMemTableDeletion();
    TestMemoryUsage();
    TestMemTableSeek();
    TestMemTableSeekToFirst();
    TestEmptyMemTable();
    TestMemTableSnapshotVisibility();
    TestMemTableDeletionWithSnapshot();

    std::cout << "All MemTable tests passed!" << std::endl;
    return 0;
}