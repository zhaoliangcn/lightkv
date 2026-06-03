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

int main() {
    TestBuildAndRead();

    std::cout << "All SSTable tests passed!" << std::endl;
    return 0;
}