#include "lightkv/skiplist.h"
#include <iostream>
#include <cassert>
#include <string>

void TestBasicOperations() {
    lightkv::SkipList list;

    list.Insert("key1", "value1", 1);
    list.Insert("key2", "value2", 2);
    list.Insert("key3", "value3", 3);

    assert(list.Contains("key1"));
    assert(list.Contains("key2"));
    assert(list.Contains("key3"));
    assert(!list.Contains("key4"));

    auto iter = list.Find("key2");
    assert(iter.Valid());
    assert(iter.key() == "key2");
    assert(iter.value() == "value2");
    assert(iter.seq() == 2);
    (void)iter;  // suppress unused variable warning
}

void TestOverwrite() {
    lightkv::SkipList list;

    list.Insert("key1", "value1", 1);
    list.Insert("key1", "value1_new", 2);

    auto iter = list.Find("key1");
    assert(iter.Valid());
    assert(iter.value() == "value1_new");
    assert(iter.seq() == 2);
    (void)iter;  // suppress unused variable warning
}

void TestOrderedIteration() {
    lightkv::SkipList list;

    list.Insert("b", "2", 1);
    list.Insert("a", "1", 2);
    list.Insert("c", "3", 3);

    auto iter = list.SeekToFirst();
    assert(iter.Valid());
    assert(iter.key() == "a");
    iter.Next();
    assert(iter.Valid());
    assert(iter.key() == "b");
    iter.Next();
    assert(iter.Valid());
    assert(iter.key() == "c");
    iter.Next();
    assert(!iter.Valid());
}

void TestSeekGE() {
    lightkv::SkipList list;

    list.Insert("aaa", "1", 1);
    list.Insert("bbb", "2", 2);
    list.Insert("ccc", "3", 3);
    list.Insert("ddd", "4", 4);

    // Seek to exact key
    auto iter = list.SeekGE("bbb");
    assert(iter.Valid());
    assert(iter.key() == "bbb");

    // Seek to non-existent key (should find next)
    iter = list.SeekGE("bxx");
    assert(iter.Valid());
    assert(iter.key() == "ccc");

    // Seek past all keys
    iter = list.SeekGE("zzz");
    assert(!iter.Valid());

    // Seek before first key
    iter = list.SeekGE("aaa");
    assert(iter.Valid());
    assert(iter.key() == "aaa");
}

void TestDeletion() {
    lightkv::SkipList list;

    list.Insert("key1", "value1", 1);
    list.Insert("key2", "value2", 2);
    list.InsertDeletion("key1", 3);

    // key1 should still be findable but marked as deleted
    auto iter = list.Find("key1");
    assert(iter.Valid());
    assert(iter.IsDeleted());

    // key2 should not be deleted
    iter = list.Find("key2");
    assert(iter.Valid());
    assert(!iter.IsDeleted());
}

void TestEmptyList() {
    lightkv::SkipList list;

    assert(!list.Contains("anything"));

    auto iter = list.SeekToFirst();
    assert(!iter.Valid());

    iter = list.Find("key");
    assert(!iter.Valid());

    iter = list.SeekGE("key");
    assert(!iter.Valid());
}

void TestIterationWithDeletions() {
    lightkv::SkipList list;

    list.Insert("a", "1", 1);
    list.InsertDeletion("b", 2);
    list.Insert("c", "3", 3);
    list.InsertDeletion("d", 4);
    list.Insert("e", "5", 5);

    auto iter = list.SeekToFirst();
    int count = 0;
    while (iter.Valid()) {
        ++count;
        iter.Next();
    }
    assert(count == 5);  // All entries including deletions
}

void TestLargeDataset() {
    lightkv::SkipList list;

    const int N = 10000;
    for (int i = 0; i < N; ++i) {
        list.Insert("key" + std::to_string(i), "value" + std::to_string(i), static_cast<uint64_t>(i));
    }

    // Verify all keys exist
    for (int i = 0; i < N; ++i) {
        assert(list.Contains("key" + std::to_string(i)));
    }

    // Verify iteration count
    auto iter = list.SeekToFirst();
    int count = 0;
    while (iter.Valid()) {
        ++count;
        iter.Next();
    }
    assert(count == N);
    (void)count;
}

int main() {
    TestBasicOperations();
    TestOverwrite();
    TestOrderedIteration();
    TestSeekGE();
    TestDeletion();
    TestEmptyList();
    TestIterationWithDeletions();
    TestLargeDataset();

    std::cout << "All SkipList tests passed!" << std::endl;
    return 0;
}