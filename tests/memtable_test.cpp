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

int main() {
    TestMemTableInsertGet();
    TestMemTableDeletion();
    TestMemoryUsage();

    std::cout << "All MemTable tests passed!" << std::endl;
    return 0;
}