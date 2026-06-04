#include "lightkv/sstable.h"
#include "lightkv/table_builder.h"
#include <iostream>
#include <cassert>
#include <cstdio>
#include <unistd.h>
#include <string>

void TestBuildAndRead() {
    std::string filename = "/tmp/lightkv_test_sstable.sst";
    ::unlink(filename.c_str());

    lightkv::Options opts;
    opts.bloom_bits_per_key = 10;

    // Build SSTable
    {
        lightkv::TableBuilder builder(opts, filename);
        builder.Add("aaaa", "bbbb");
        builder.Add("key1", "value1");
        builder.Add("key2", "value2");
        builder.Add("key3", "value3");
        builder.Finish();
    }

    // Read SSTable
    {
        lightkv::SSTable table(opts, filename, 1);
        auto s = table.Open();
        assert(s.ok());

        std::string value;
        uint64_t seq;

        s = table.Get("key1", &value, &seq);
        assert(s.ok());
        assert(value == "value1");

        s = table.Get("key2", &value, &seq);
        assert(s.ok());
        assert(value == "value2");

        s = table.Get("key3", &value, &seq);
        assert(s.ok());
        assert(value == "value3");

        s = table.Get("aaaa", &value, &seq);
        assert(s.ok());
        assert(value == "bbbb");

        s = table.Get("nonexistent", &value, &seq);
        assert(s.IsNotFound());
    }

    ::unlink(filename.c_str());
}

void TestSSTableIterator() {
    std::string filename = "/tmp/lightkv_test_sstable_iter.sst";
    ::unlink(filename.c_str());

    lightkv::Options opts;
    opts.bloom_bits_per_key = 10;

    // Build SSTable with ordered data
    {
        lightkv::TableBuilder builder(opts, filename);
        for (int i = 0; i < 100; ++i) {
            char key[16], val[32];
            snprintf(key, sizeof(key), "%03d", i);
            snprintf(val, sizeof(val), "val_%03d", i);
            builder.Add(key, val);
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
    std::string filename = "/tmp/lightkv_test_sstable_seek.sst";
    ::unlink(filename.c_str());

    lightkv::Options opts;

    {
        lightkv::TableBuilder builder(opts, filename);
        builder.Add("aaa", "1");
        builder.Add("bbb", "2");
        builder.Add("ccc", "3");
        builder.Add("ddd", "4");
        builder.Add("eee", "5");
        builder.Finish();
    }

    {
        lightkv::SSTable table(opts, filename, 1);
        auto s = table.Open();
        assert(s.ok());

        auto iter = table.NewIterator();

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

    ::unlink(filename.c_str());
}

void TestSSTableBloomFilter() {
    std::string filename = "/tmp/lightkv_test_sstable_bloom.sst";
    ::unlink(filename.c_str());

    lightkv::Options opts;
    opts.bloom_bits_per_key = 10;

    // Build SSTable with bloom filter
    {
        lightkv::TableBuilder builder(opts, filename);
        for (int i = 0; i < 1000; ++i) {
            builder.Add("key" + std::to_string(i), "value" + std::to_string(i));
        }
        builder.Finish();
    }

    // Read and verify bloom filter helps with negative lookups
    {
        lightkv::SSTable table(opts, filename, 1);
        auto s = table.Open();
        assert(s.ok());

        // All existing keys should be found
        for (int i = 0; i < 1000; ++i) {
            std::string value;
            uint64_t seq;
            s = table.Get("key" + std::to_string(i), &value, &seq);
            assert(s.ok());
            assert(value == "value" + std::to_string(i));
        }

        // Non-existent keys should return NotFound
        // Bloom filter may have false positives, but these specific keys should not match
        std::string value;
        uint64_t seq;
        s = table.Get("nonexistent_key_xyz", &value, &seq);
        assert(s.IsNotFound());
    }

    ::unlink(filename.c_str());
}

void TestSSTableEmpty() {
    std::string filename = "/tmp/lightkv_test_sstable_empty.sst";
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

int main() {
    TestBuildAndRead();
    TestSSTableIterator();
    TestSSTableIteratorSeek();
    TestSSTableBloomFilter();
    TestSSTableEmpty();

    std::cout << "All SSTable tests passed!" << std::endl;
    return 0;
}