#include "test_paths.h"
#include "lightkv/sstable.h"
#include "lightkv/table_builder.h"
#include "lightkv/encoding.h"
#include "lightkv/platform.h"
#include <algorithm>
#include <cassert>
#include <vector>
#include <cstdio>
#include <iostream>
#include <string>

namespace {
// Snapshot bound meaning "the newest version", mirroring
// ReadOptions::snapshot_seq == 0 at the DB level.
constexpr uint64_t SNAP_LATEST = UINT64_MAX;
}  // namespace

void TestBuildAndRead() {
    std::string filename = LIGHTKV_TEST_TMP "/lightkv_test_sstable.sst";
    ::unlink(filename.c_str());

    lightkv::Options opts;
    opts.bloom_bits_per_key = 10;

    // Build SSTable
    {
        lightkv::TableBuilder builder(opts, filename);
        uint64_t seq = 1;
        builder.Add("aaaa", "bbbb", seq++, lightkv::kTypeValue);
        builder.Add("key1", "value1", seq++, lightkv::kTypeValue);
        builder.Add("key2", "value2", seq++, lightkv::kTypeValue);
        builder.Add("key3", "value3", seq++, lightkv::kTypeValue);
        builder.Finish();
    }

    // Read SSTable
    {
        lightkv::SSTable table(opts, filename, 1);
        auto s = table.Open();
        assert(s.ok());

        std::string value;
        uint64_t seq = 0;
        uint8_t type = lightkv::kTypeValue;

        s = table.Get("key1", SNAP_LATEST, &value, &seq, &type);
        assert(s.ok());
        assert(value == "value1");
        assert(type == lightkv::kTypeValue);

        s = table.Get("key2", SNAP_LATEST, &value, &seq, &type);
        assert(s.ok());
        assert(value == "value2");

        s = table.Get("key3", SNAP_LATEST, &value, &seq, &type);
        assert(s.ok());
        assert(value == "value3");

        s = table.Get("aaaa", SNAP_LATEST, &value, &seq, &type);
        assert(s.ok());
        assert(value == "bbbb");

        s = table.Get("nonexistent", SNAP_LATEST, &value, &seq, &type);
        assert(s.IsNotFound());
    }

    ::unlink(filename.c_str());
}

// Multiple versions of the same key must be stored newest-first and readable
// at an arbitrary snapshot. This is the capability the internal-key format
// exists for.
void TestVersionedReadAtSnapshot() {
    std::string filename = LIGHTKV_TEST_TMP "/lightkv_test_sstable_versions.sst";
    ::unlink(filename.c_str());

    lightkv::Options opts;
    opts.bloom_bits_per_key = 10;

    // Entries for one key, added in descending sequence order as the format
    // requires. "aaa"/"zzz" pad the file so the key is not the only entry.
    {
        lightkv::TableBuilder builder(opts, filename);
        builder.Add("aaa", "pad_left", 100, lightkv::kTypeValue);
        builder.Add("kk", "v5", 5, lightkv::kTypeValue);
        builder.Add("kk", "v4", 4, lightkv::kTypeValue);
        builder.Add("kk", "v3", 3, lightkv::kTypeValue);
        builder.Add("kk", "v2", 2, lightkv::kTypeValue);
        builder.Add("kk", "v1", 1, lightkv::kTypeValue);
        builder.Add("zzz", "pad_right", 100, lightkv::kTypeValue);
        builder.Finish();
    }

    lightkv::SSTable table(opts, filename, 1);
    auto s = table.Open();
    assert(s.ok());

    struct Expect { uint64_t snap; const char* value; };
    const Expect kCases[] = {
        {UINT64_MAX, "v5"},
        {5,         "v5"},
        {4,         "v4"},
        {3,         "v3"},
        {2,         "v2"},
        {1,         "v1"},
    };
    for (const auto& c : kCases) {
        std::string value;
        uint64_t seq = 0;
        uint8_t type = lightkv::kTypeValue;
        s = table.Get("kk", c.snap, &value, &seq, &type);
        if (!s.ok() || value != c.value) {
            std::fprintf(stderr, "  snapshot %llu -> %s '%s', expected '%s'\n",
                         (unsigned long long)c.snap, s.ToString().c_str(),
                         value.c_str(), c.value);
        }
        assert(s.ok());
        assert(value == c.value);
        assert(type == lightkv::kTypeValue);
    }

    // Snapshot 0 precedes every write, so nothing is visible.
    {
        std::string value;
        uint64_t seq = 0;
        uint8_t type = lightkv::kTypeValue;
        assert(table.Get("kk", 0, &value, &seq, &type).IsNotFound());
    }

    // The iterator must surface the versions newest-first.
    {
        auto it = table.NewIterator();
        it.Seek(lightkv::Slice(lightkv::SeekInternalKey("kk")));
        int seen = 0;
        while (it.Valid() && it.key() == lightkv::Slice("kk")) {
            if (seen == 0) assert(it.value() == lightkv::Slice("v5"));
            if (seen == 1) assert(it.value() == lightkv::Slice("v4"));
            ++seen;
            it.Next();
        }
        assert(seen == 5);
    }

    ::unlink(filename.c_str());
}

// A deletion must be persisted as a value type, not as an empty value, so that
// it can shadow older versions without hiding "empty string" values.
void TestDeletionTypePersisted() {
    std::string filename = LIGHTKV_TEST_TMP "/lightkv_test_sstable_delete.sst";
    ::unlink(filename.c_str());

    lightkv::Options opts;
    opts.bloom_bits_per_key = 10;

    {
        lightkv::TableBuilder builder(opts, filename);
        builder.Add("empty_is_a_value", "", 1, lightkv::kTypeValue);
        // Same user key: descending sequence order, newest first.
        builder.Add("gone", "", 2, lightkv::kTypeDeletion);
        builder.Add("gone", "old", 1, lightkv::kTypeValue);
        builder.Finish();
    }

    lightkv::SSTable table(opts, filename, 1);
    auto s = table.Open();
    assert(s.ok());

    std::string value;
    uint64_t seq = 0;
    uint8_t type = lightkv::kTypeValue;

    // The deleted key reports the tombstone so callers can compare sequence
    // numbers across files.
    s = table.Get("gone", SNAP_LATEST, &value, &seq, &type);
    assert(s.ok());
    assert(type == lightkv::kTypeDeletion);
    assert(seq == 2);

    // An empty value is a real value, not a deletion.
    s = table.Get("empty_is_a_value", SNAP_LATEST, &value, &seq, &type);
    assert(s.ok());
    assert(type == lightkv::kTypeValue);
    assert(value.empty());

    // Before the deletion, the old value is visible.
    s = table.Get("gone", 1, &value, &seq, &type);
    assert(s.ok());
    assert(type == lightkv::kTypeValue);
    assert(value == "old");

    ::unlink(filename.c_str());
}

// The internal-key encoding must order (user_key ASC, seq DESC) under plain
// byte-wise comparison — every ordering primitive in the engine relies on it.
void TestInternalKeyOrdering() {
    using lightkv::MakeInternalKey;
    using lightkv::Slice;

    const std::string a1 = MakeInternalKey("a", 1, lightkv::kTypeValue);
    const std::string a5 = MakeInternalKey("a", 5, lightkv::kTypeValue);
    const std::string b1 = MakeInternalKey("b", 1, lightkv::kTypeValue);

    assert(a5 < a1);        // same user key: higher seq sorts first
    assert(a1 < b1);        // user key dominates
    assert(lightkv::ExtractUserKey(Slice(a5)) == Slice("a"));
    assert(lightkv::ExtractSeq(Slice(a5)) == 5);
    assert(lightkv::ExtractSeq(Slice(a1)) == 1);
    assert(lightkv::ExtractType(Slice(a1)) == lightkv::kTypeValue);

    // Seeking with the smallest possible internal key must land on the newest.
    const std::string seek = lightkv::SeekInternalKey("a");
    assert(lightkv::CompareInternalKeys(lightkv::Slice(seek), lightkv::Slice(a5)) < 0);
    assert(lightkv::CompareInternalKeys(lightkv::Slice(seek), lightkv::Slice(a1)) < 0);

    // Raw byte order is NOT internal-key order when one user key is a prefix of
    // another. This is exactly why CompareInternalKeys exists.
    const std::string p1 = MakeInternalKey("key1", 5, lightkv::kTypeValue);
    const std::string p2 = MakeInternalKey("key10", 5, lightkv::kTypeValue);
    assert(p1 > p2);                                   // raw bytes: inverted
    assert(lightkv::CompareInternalKeys(lightkv::Slice(p1),
                                        lightkv::Slice(p2)) < 0);  // logical order
}

void TestSSTableIterator() {
    std::string filename = LIGHTKV_TEST_TMP "/lightkv_test_sstable_iter.sst";
    ::unlink(filename.c_str());

    lightkv::Options opts;
    opts.bloom_bits_per_key = 10;

    // Build SSTable with ordered data
    {
        lightkv::TableBuilder builder(opts, filename);
        uint64_t seq = 1;
        for (int i = 0; i < 100; ++i) {
            char key[16], val[32];
            snprintf(key, sizeof(key), "%03d", i);
            snprintf(val, sizeof(val), "val_%03d", i);
            builder.Add(key, val, seq++, lightkv::kTypeValue);
        }
        builder.Finish();
    }

    // Read SSTable with iterator
    {
        lightkv::SSTable table(opts, filename, 1);
        auto s = table.Open();
        assert(s.ok());

        auto iter = table.NewIterator();
        iter.SeekToFirst();

        int count = 0;
        while (iter.Valid()) {
            char expected_key[16], expected_val[32];
            snprintf(expected_key, sizeof(expected_key), "%03d", count);
            snprintf(expected_val, sizeof(expected_val), "val_%03d", count);
            assert(iter.key() == lightkv::Slice(expected_key));
            assert(iter.value() == lightkv::Slice(expected_val));
            iter.Next();
            ++count;
        }
        assert(count == 100);
    }

    ::unlink(filename.c_str());
}

void TestSSTableIteratorSeek() {
    std::string filename = LIGHTKV_TEST_TMP "/lightkv_test_sstable_seek.sst";
    ::unlink(filename.c_str());

    lightkv::Options opts;

    {
        lightkv::TableBuilder builder(opts, filename);
        uint64_t seq = 1;
        builder.Add("aaa", "1", seq++, lightkv::kTypeValue);
        builder.Add("bbb", "2", seq++, lightkv::kTypeValue);
        builder.Add("ccc", "3", seq++, lightkv::kTypeValue);
        builder.Add("ddd", "4", seq++, lightkv::kTypeValue);
        builder.Add("eee", "5", seq++, lightkv::kTypeValue);
        builder.Finish();
    }

    {
        lightkv::SSTable table(opts, filename, 1);
        auto s = table.Open();
        assert(s.ok());

        auto iter = table.NewIterator();
        auto seek = [](const char* k) {
            return lightkv::Slice(lightkv::SeekInternalKey(k));
        };

        // Seek to exact key
        iter.Seek(seek("ccc"));
        assert(iter.Valid());
        assert(iter.key() == "ccc");

        // Seek to non-existent key (should find next)
        iter.Seek(seek("ccx"));
        assert(iter.Valid());
        assert(iter.key() == "ddd");

        // Seek past all keys
        iter.Seek(seek("zzz"));
        assert(!iter.Valid());

        // Seek before first key
        iter.Seek(seek("aaa"));
        assert(iter.Valid());
        assert(iter.key() == "aaa");
    }

    ::unlink(filename.c_str());
}

void TestSSTableBloomFilter() {
    std::string filename = LIGHTKV_TEST_TMP "/lightkv_test_sstable_bloom.sst";
    ::unlink(filename.c_str());

    lightkv::Options opts;
    opts.bloom_bits_per_key = 10;

    // Build SSTable with bloom filter
    {
        lightkv::TableBuilder builder(opts, filename);
        uint64_t seq = 1;
        for (int i = 0; i < 1000; ++i) {
            char key[16], val[32];
            snprintf(key, sizeof(key), "key%03d", i);
            snprintf(val, sizeof(val), "value%03d", i);
            builder.Add(key, val, seq++, lightkv::kTypeValue);
        }
        builder.Finish();
    }

    // Read and verify bloom filter helps with negative lookups
    {
        lightkv::SSTable table(opts, filename, 1);
        auto s = table.Open();
        assert(s.ok());

        // Test iterator first
        auto it = table.NewIterator();
        it.SeekToFirst();
        int count = 0;
        while (it.Valid()) {
            ++count;
            it.Next();
        }
        assert(count == 1000);

        // All existing keys should be found
        for (int i = 0; i < 1000; ++i) {
            char key[16], val[32];
            snprintf(key, sizeof(key), "key%03d", i);
            snprintf(val, sizeof(val), "value%03d", i);
            std::string value;
            uint64_t seq = 0;
            uint8_t type = lightkv::kTypeValue;
            s = table.Get(key, SNAP_LATEST, &value, &seq, &type);
            assert(s.ok());
            assert(value == val);
        }

        // Non-existent keys should return NotFound
        std::string value;
        uint64_t seq = 0;
        uint8_t type = lightkv::kTypeValue;
        s = table.Get("nonexistent_key_xyz", SNAP_LATEST, &value, &seq, &type);
        assert(s.IsNotFound());
    }

    ::unlink(filename.c_str());
}

void TestSSTableEmpty() {
    std::string filename = LIGHTKV_TEST_TMP "/lightkv_test_sstable_empty.sst";
    ::unlink(filename.c_str());

    lightkv::Options opts;

    // Build empty SSTable
    {
        lightkv::TableBuilder builder(opts, filename);
        builder.Finish();
    }

    {
        lightkv::SSTable table(opts, filename, 1);
        auto s = table.Open();
        assert(s.ok());

        auto iter = table.NewIterator();
        iter.SeekToFirst();
        assert(!iter.Valid());
    }

    ::unlink(filename.c_str());
}

// A file written by the previous format must be rejected loudly rather than
// misread as if the trailing bytes were a sequence number.
void TestRejectsLegacyFormat() {
    lightkv::Options opts;

    lightkv::TableFooter footer;
    footer.index_block_handle_offset = 0;
    footer.meta_index_block_handle_offset = 0;

    std::string v1;
    lightkv::PutFixed64(&v1, footer.index_block_handle_offset);
    lightkv::PutFixed64(&v1, footer.meta_index_block_handle_offset);
    v1.append("LIGHTKV1", 8);

    lightkv::TableFooter decoded;
    auto s = lightkv::TableFooter::DecodeFrom(lightkv::Slice(v1), &decoded);
    if (s.ok()) {
        std::fprintf(stderr, "  LIGHTKV1 footer was accepted\n");
    }
    assert(!s.ok());
    assert(s.IsCorruption());
    assert(s.ToString().find("LIGHTKV1") != std::string::npos);

    std::string v2;
    footer.EncodeTo(&v2);
    assert(lightkv::TableFooter::DecodeFrom(lightkv::Slice(v2), &decoded).ok());
}

// Many data blocks: block selection uses the index block, so a seek bug only
// shows up once a file spans several blocks.
void TestMultiBlockSeek() {
    std::string filename = LIGHTKV_TEST_TMP "/lightkv_test_sstable_multiblock.sst";
    ::unlink(filename.c_str());

    lightkv::Options opts;
    opts.paranoid_checks = true;    opts.block_size = 256;              // force many small blocks
    opts.bloom_bits_per_key = 10;

    const int kCount = 500;
    {
        lightkv::TableBuilder builder(opts, filename);
        uint64_t seq = 1;
        // TableBuilder requires ascending internal-key order; it does not sort.
        // Collect first, then emit in sorted order. The keys stay
        // variable-width with a long shared prefix ("key0", "key1", "key10",
        // "key100", ...) so prefix compression inside a block is exercised.
        std::vector<std::string> keys;
        keys.reserve(kCount);
        for (int i = 0; i < kCount; ++i) {
            char key[24];
            snprintf(key, sizeof(key), "key%d", i);
            keys.emplace_back(key);
        }
        std::sort(keys.begin(), keys.end());
        for (const auto& k : keys) {
            char val[32];
            snprintf(val, sizeof(val), "value%s", k.c_str() + 3);
            builder.Add(k, val, seq++, lightkv::kTypeValue);
        }
        builder.Finish();
    }

    lightkv::SSTable table(opts, filename, 1);
    auto s = table.Open();
    assert(s.ok());

    // Iterator must visit everything in sorted order and be complete.
    {
        auto it = table.NewIterator();
        it.SeekToFirst();
        int seen = 0;
        while (it.Valid()) {
            ++seen;
            it.Next();
        }
        std::fprintf(stderr, "  multiblock iterated %d keys\n", seen);
        assert(seen == kCount);

        // Ascending order is a hard requirement of the format. Raw byte order is
        // NOT the same thing (one user key can be a prefix of another), so the
        // check must use the internal-key comparator.
        it.SeekToFirst();
        std::string prev;
        while (it.Valid()) {
            std::string cur = it.internal_key().ToString();
            assert(prev.empty() ||
                   lightkv::CompareInternalKeys(lightkv::Slice(prev),
                                                lightkv::Slice(cur)) < 0);
            prev = cur;
            it.Next();
        }
    }

    // Every key must be findable through the index block.
    for (int i = 0; i < kCount; ++i) {
        char key[24], val[32];
        snprintf(key, sizeof(key), "key%d", i);
        snprintf(val, sizeof(val), "value%d", i);
        std::string value;
        uint64_t seq = 0;
        uint8_t type = lightkv::kTypeValue;
        s = table.Get(key, SNAP_LATEST, &value, &seq, &type);
        if (!s.ok() || value != val) {
            std::fprintf(stderr, "  multiblock Get(%s) -> %s '%s', expected '%s'\n",
                         key, s.ToString().c_str(), value.c_str(), val);
        }
        assert(s.ok());
        assert(value == val);
    }

    ::unlink(filename.c_str());
}

int main() {
    TestInternalKeyOrdering();
    TestBuildAndRead();
    TestVersionedReadAtSnapshot();
    TestDeletionTypePersisted();
    TestRejectsLegacyFormat();
    TestSSTableIterator();
    TestSSTableIteratorSeek();
    TestSSTableBloomFilter();
    TestMultiBlockSeek();
    TestSSTableEmpty();

    std::cout << "All SSTable tests passed!" << std::endl;
    return 0;
}
