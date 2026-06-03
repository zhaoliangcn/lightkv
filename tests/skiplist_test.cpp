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
}

void TestOverwrite() {
    lightkv::SkipList list;

    list.Insert("key1", "value1", 1);
    list.Insert("key1", "value1_new", 2);

    auto iter = list.Find("key1");
    assert(iter.Valid());
    assert(iter.value() == "value1_new");
    assert(iter.seq() == 2);
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

int main() {
    TestBasicOperations();
    TestOverwrite();
    TestOrderedIteration();

    std::cout << "All SkipList tests passed!" << std::endl;
    return 0;
}