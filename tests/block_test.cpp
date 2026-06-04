#include "lightkv/block.h"
#include "lightkv/block_builder.h"
#include <iostream>
#include <cassert>
#include <string>

void TestBlockBuildAndRead() {
    lightkv::BlockBuilder builder(16);

    builder.Add("key1", "value1");
    builder.Add("key2", "value2");
    builder.Add("key3", "value3");

    lightkv::Slice data = builder.Finish();

    lightkv::Block block(data.data(), data.size());
    auto iter = block.NewIterator();

    iter.SeekToFirst();
    assert(iter.Valid());
    assert(iter.key() == "key1");
    assert(iter.value() == "value1");

    iter.Next();
    assert(iter.Valid());
    assert(iter.key() == "key2");
    assert(iter.value() == "value2");

    iter.Next();
    assert(iter.Valid());
    assert(iter.key() == "key3");
    assert(iter.value() == "value3");

    iter.Next();
    assert(!iter.Valid());
}

void TestBlockIteratorSeek() {
    lightkv::BlockBuilder builder(4);

    builder.Add("aaa", "1");
    builder.Add("bbb", "2");
    builder.Add("ccc", "3");
    builder.Add("ddd", "4");
    builder.Add("eee", "5");

    lightkv::Slice data = builder.Finish();

    lightkv::Block block(data.data(), data.size());
    auto iter = block.NewIterator();

    // Seek to exact key
    iter.Seek("ccc");
    assert(iter.Valid());
    assert(iter.key() == "ccc");

    // Seek to non-existent key (should find next)
    iter.Seek("ccx");
    assert(iter.Valid());
    assert(iter.key() == "ddd");

    // Seek past all keys
    iter.Seek("zzz");
    assert(!iter.Valid());

    // Seek before first key
    iter.Seek("aaa");
    assert(iter.Valid());
    assert(iter.key() == "aaa");
}

void TestBlockEmpty() {
    lightkv::BlockBuilder builder(16);
    lightkv::Slice data = builder.Finish();

    lightkv::Block block(data.data(), data.size());
    auto iter = block.NewIterator();

    iter.SeekToFirst();
    assert(!iter.Valid());

    iter.Seek("anything");
    assert(!iter.Valid());
}

void TestBlockLargeDataset() {
    lightkv::BlockBuilder builder(16);

    const int N = 500;
    for (int i = 0; i < N; ++i) {
        char key[16], val[32];
        snprintf(key, sizeof(key), "%04d", i);
        snprintf(val, sizeof(val), "val_%04d", i);
        builder.Add(key, val);
    }

    lightkv::Slice data = builder.Finish();

    lightkv::Block block(data.data(), data.size());
    auto iter = block.NewIterator();

    iter.SeekToFirst();
    int count = 0;
    while (iter.Valid()) {
        char expected_key[16], expected_val[32];
        snprintf(expected_key, sizeof(expected_key), "%04d", count);
        snprintf(expected_val, sizeof(expected_val), "val_%04d", count);
        assert(iter.key() == lightkv::Slice(expected_key));
        assert(iter.value() == lightkv::Slice(expected_val));
        iter.Next();
        ++count;
    }
    assert(count == N);

    // Seek to middle
    iter.Seek("0250");
    assert(iter.Valid());
    assert(iter.key() == "0250");
}

void TestBlockPrefixCompression() {
    lightkv::BlockBuilder builder(2);  // Small restart interval to test prefix compression

    // Keys with common prefix
    builder.Add("prefix_key1", "value1");
    builder.Add("prefix_key2", "value2");
    builder.Add("prefix_key3", "value3");
    builder.Add("different_key", "value4");
    builder.Add("prefix_key4", "value5");

    lightkv::Slice data = builder.Finish();

    lightkv::Block block(data.data(), data.size());
    auto iter = block.NewIterator();

    iter.SeekToFirst();
    int count = 0;
    while (iter.Valid()) {
        ++count;
        iter.Next();
    }
    assert(count == 5);
}

void TestBlockSingleEntry() {
    lightkv::BlockBuilder builder(16);
    builder.Add("only_key", "only_value");

    lightkv::Slice data = builder.Finish();

    lightkv::Block block(data.data(), data.size());
    auto iter = block.NewIterator();

    iter.SeekToFirst();
    assert(iter.Valid());
    assert(iter.key() == "only_key");
    assert(iter.value() == "only_value");

    iter.Next();
    assert(!iter.Valid());
}

void TestBlockPrev() {
    lightkv::BlockBuilder builder(16);

    builder.Add("aaa", "1");
    builder.Add("bbb", "2");
    builder.Add("ccc", "3");

    lightkv::Slice data = builder.Finish();

    lightkv::Block block(data.data(), data.size());
    auto iter = block.NewIterator();

    iter.SeekToLast();
    assert(iter.Valid());
    assert(iter.key() == "ccc");

    iter.Prev();
    assert(iter.Valid());
    assert(iter.key() == "bbb");

    iter.Prev();
    assert(iter.Valid());
    assert(iter.key() == "aaa");

    iter.Prev();
    assert(!iter.Valid());
}

int main() {
    TestBlockBuildAndRead();
    TestBlockIteratorSeek();
    TestBlockEmpty();
    TestBlockLargeDataset();
    TestBlockPrefixCompression();
    TestBlockSingleEntry();
    TestBlockPrev();

    std::cout << "All Block tests passed!" << std::endl;
    return 0;
}
