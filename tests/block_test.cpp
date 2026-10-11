#include "lightkv/block.h"
#include "lightkv/encoding.h"
#include "lightkv/block_builder.h"
#include <iostream>
#include <cassert>
#include <string>

void TestBlockBuildAndRead() {
    lightkv::BlockBuilder builder(16);
    uint64_t seq = 1;

    builder.Add(lightkv::Slice(lightkv::MakeInternalKey("key1", seq++, lightkv::kTypeValue)), "value1");
    builder.Add(lightkv::Slice(lightkv::MakeInternalKey("key2", seq++, lightkv::kTypeValue)), "value2");
    builder.Add(lightkv::Slice(lightkv::MakeInternalKey("key3", seq++, lightkv::kTypeValue)), "value3");

    lightkv::Slice data = builder.Finish();

    lightkv::Block block(data.data(), data.size());
    auto iter = block.NewIterator();

    iter.SeekToFirst();
    assert(iter.Valid());
    assert(ExtractUserKey(iter.key()) == lightkv::Slice("key1"));
    assert(iter.value() == "value1");

    iter.Next();
    assert(iter.Valid());
    assert(ExtractUserKey(iter.key()) == lightkv::Slice("key2"));
    assert(iter.value() == "value2");

    iter.Next();
    assert(iter.Valid());
    assert(ExtractUserKey(iter.key()) == lightkv::Slice("key3"));
    assert(iter.value() == "value3");

    iter.Next();
    assert(!iter.Valid());
}

void TestBlockIteratorSeek() {
    lightkv::BlockBuilder builder(4);
    uint64_t seq = 1;

    builder.Add(lightkv::Slice(lightkv::MakeInternalKey("aaa", seq++, lightkv::kTypeValue)), "1");
    builder.Add(lightkv::Slice(lightkv::MakeInternalKey("bbb", seq++, lightkv::kTypeValue)), "2");
    builder.Add(lightkv::Slice(lightkv::MakeInternalKey("ccc", seq++, lightkv::kTypeValue)), "3");
    builder.Add(lightkv::Slice(lightkv::MakeInternalKey("ddd", seq++, lightkv::kTypeValue)), "4");
    builder.Add(lightkv::Slice(lightkv::MakeInternalKey("eee", seq++, lightkv::kTypeValue)), "5");

    lightkv::Slice data = builder.Finish();

    lightkv::Block block(data.data(), data.size());
    auto iter = block.NewIterator();

    // Seek to exact key
    iter.Seek(lightkv::Slice(lightkv::SeekInternalKey("ccc")));
    assert(iter.Valid());
    assert(ExtractUserKey(iter.key()) == lightkv::Slice("ccc"));

    // Seek to non-existent key (should find next)
    iter.Seek(lightkv::Slice(lightkv::SeekInternalKey("ccx")));
    assert(iter.Valid());
    assert(ExtractUserKey(iter.key()) == lightkv::Slice("ddd"));

    // Seek past all keys
    iter.Seek(lightkv::Slice(lightkv::SeekInternalKey("zzz")));
    assert(!iter.Valid());

    // Seek before first key
    iter.Seek(lightkv::Slice(lightkv::SeekInternalKey("aaa")));
    assert(iter.Valid());
    assert(ExtractUserKey(iter.key()) == lightkv::Slice("aaa"));
}

void TestBlockEmpty() {
    lightkv::BlockBuilder builder(16);
    uint64_t seq = 1;
    lightkv::Slice data = builder.Finish();

    lightkv::Block block(data.data(), data.size());
    auto iter = block.NewIterator();

    iter.SeekToFirst();
    assert(!iter.Valid());

    iter.Seek(lightkv::Slice(lightkv::SeekInternalKey("anything")));
    assert(!iter.Valid());
}

void TestBlockLargeDataset() {
    lightkv::BlockBuilder builder(16);
    uint64_t seq = 1;

    const int N = 500;
    for (int i = 0; i < N; ++i) {
        char key[16], val[32];
        snprintf(key, sizeof(key), "%04d", i);
        snprintf(val, sizeof(val), "val_%04d", i);
        builder.Add(lightkv::Slice(lightkv::MakeInternalKey(key, seq++, lightkv::kTypeValue)), val);
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
        assert(lightkv::ExtractUserKey(iter.key()) == lightkv::Slice(expected_key));
        assert(iter.value() == lightkv::Slice(expected_val));
        iter.Next();
        ++count;
    }
    assert(count == N);

    // Seek to middle
    iter.Seek(lightkv::Slice(lightkv::SeekInternalKey("0250")));
    assert(iter.Valid());
    assert(ExtractUserKey(iter.key()) == lightkv::Slice("0250"));
}

void TestBlockPrefixCompression() {
    lightkv::BlockBuilder builder(2);  // Small restart interval to test prefix compression
    uint64_t seq = 1;

    // Keys with common prefix
    builder.Add(lightkv::Slice(lightkv::MakeInternalKey("prefix_key1", seq++, lightkv::kTypeValue)), "value1");
    builder.Add(lightkv::Slice(lightkv::MakeInternalKey("prefix_key2", seq++, lightkv::kTypeValue)), "value2");
    builder.Add(lightkv::Slice(lightkv::MakeInternalKey("prefix_key3", seq++, lightkv::kTypeValue)), "value3");
    builder.Add(lightkv::Slice(lightkv::MakeInternalKey("different_key", seq++, lightkv::kTypeValue)), "value4");
    builder.Add(lightkv::Slice(lightkv::MakeInternalKey("prefix_key4", seq++, lightkv::kTypeValue)), "value5");

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
    uint64_t seq = 1;
    builder.Add(lightkv::Slice(lightkv::MakeInternalKey("only_key", seq++, lightkv::kTypeValue)), "only_value");

    lightkv::Slice data = builder.Finish();

    lightkv::Block block(data.data(), data.size());
    auto iter = block.NewIterator();

    iter.SeekToFirst();
    assert(iter.Valid());
    assert(ExtractUserKey(iter.key()) == lightkv::Slice("only_key"));
    assert(iter.value() == "only_value");

    iter.Next();
    assert(!iter.Valid());
}

void TestBlockPrev() {
    lightkv::BlockBuilder builder(16);
    uint64_t seq = 1;

    builder.Add(lightkv::Slice(lightkv::MakeInternalKey("aaa", seq++, lightkv::kTypeValue)), "1");
    builder.Add(lightkv::Slice(lightkv::MakeInternalKey("bbb", seq++, lightkv::kTypeValue)), "2");
    builder.Add(lightkv::Slice(lightkv::MakeInternalKey("ccc", seq++, lightkv::kTypeValue)), "3");

    lightkv::Slice data = builder.Finish();

    lightkv::Block block(data.data(), data.size());
    auto iter = block.NewIterator();

    iter.SeekToLast();
    assert(iter.Valid());
    assert(ExtractUserKey(iter.key()) == lightkv::Slice("ccc"));

    iter.Prev();
    assert(iter.Valid());
    assert(ExtractUserKey(iter.key()) == lightkv::Slice("bbb"));

    iter.Prev();
    assert(iter.Valid());
    assert(ExtractUserKey(iter.key()) == lightkv::Slice("aaa"));

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
