#include "lightkv/wal.h"
#include <iostream>
#include <cassert>
#include <cstdio>
#include <unistd.h>
#include <string>

void TestWALWriteRead() {
    std::string fname = "/tmp/lightkv_test_wal.log";
    ::unlink(fname.c_str());

    {
        lightkv::WALWriter writer(fname);
        auto s = writer.Open();
        assert(s.ok());

        s = writer.Append(1, lightkv::WALRecord::kTypeValue, "key1", "value1");
        assert(s.ok());
        s = writer.Append(2, lightkv::WALRecord::kTypeDeletion, "key2", "");
        assert(s.ok());
        s = writer.Append(3, lightkv::WALRecord::kTypeValue, "key3", "value3");
        assert(s.ok());

        writer.Close();
    }

    {
        lightkv::WALReader reader(fname);
        auto s = reader.Open();
        assert(s.ok());

        lightkv::WALRecord rec;

        assert(reader.ReadRecord(&rec));
        assert(rec.seq == 1);
        assert(rec.type == lightkv::WALRecord::kTypeValue);
        assert(rec.key == "key1");
        assert(rec.value == "value1");

        assert(reader.ReadRecord(&rec));
        assert(rec.seq == 2);
        assert(rec.type == lightkv::WALRecord::kTypeDeletion);
        assert(rec.key == "key2");

        assert(reader.ReadRecord(&rec));
        assert(rec.seq == 3);
        assert(rec.type == lightkv::WALRecord::kTypeValue);
        assert(rec.key == "key3");
        assert(rec.value == "value3");

        assert(!reader.ReadRecord(&rec));

        reader.Close();
    }

    ::unlink(fname.c_str());
}

void TestWALTruncate() {
    std::string fname = "/tmp/lightkv_test_wal_truncate.log";
    ::unlink(fname.c_str());

    {
        lightkv::WALWriter writer(fname);
        auto s = writer.Open();
        assert(s.ok());

        // Write some records
        s = writer.Append(1, lightkv::WALRecord::kTypeValue, "key1", "value1");
        assert(s.ok());
        s = writer.Append(2, lightkv::WALRecord::kTypeValue, "key2", "value2");
        assert(s.ok());

        // Truncate to current write position
        s = writer.Truncate();
        assert(s.ok());

        // Write more records after truncate
        s = writer.Append(3, lightkv::WALRecord::kTypeValue, "key3", "value3");
        assert(s.ok());

        writer.Close();
    }

    {
        lightkv::WALReader reader(fname);
        auto s = reader.Open();
        assert(s.ok());

        lightkv::WALRecord rec;

        // Should read all 3 records
        assert(reader.ReadRecord(&rec));
        assert(rec.seq == 1);
        assert(reader.ReadRecord(&rec));
        assert(rec.seq == 2);
        assert(reader.ReadRecord(&rec));
        assert(rec.seq == 3);
        assert(!reader.ReadRecord(&rec));

        reader.Close();
    }

    ::unlink(fname.c_str());
}

void TestWALLargeValues() {
    std::string fname = "/tmp/lightkv_test_wal_large.log";
    ::unlink(fname.c_str());

    {
        lightkv::WALWriter writer(fname);
        auto s = writer.Open();
        assert(s.ok());

        std::string large_value(10000, 'x');
        s = writer.Append(1, lightkv::WALRecord::kTypeValue, "large_key", large_value);
        assert(s.ok());

        writer.Close();
    }

    {
        lightkv::WALReader reader(fname);
        auto s = reader.Open();
        assert(s.ok());

        lightkv::WALRecord rec;
        assert(reader.ReadRecord(&rec));
        assert(rec.seq == 1);
        assert(rec.key == "large_key");
        assert(rec.value.size() == 10000);
        assert(rec.value == std::string(10000, 'x'));

        reader.Close();
    }

    ::unlink(fname.c_str());
}

void TestWALEmptyRead() {
    std::string fname = "/tmp/lightkv_test_wal_empty.log";
    ::unlink(fname.c_str());

    {
        lightkv::WALWriter writer(fname);
        auto s = writer.Open();
        assert(s.ok());
        writer.Close();
    }

    {
        lightkv::WALReader reader(fname);
        auto s = reader.Open();
        assert(s.ok());

        lightkv::WALRecord rec;
        assert(!reader.ReadRecord(&rec));

        reader.Close();
    }

    ::unlink(fname.c_str());
}

int main() {
    TestWALWriteRead();
    TestWALTruncate();
    TestWALLargeValues();
    TestWALEmptyRead();

    std::cout << "All WAL tests passed!" << std::endl;
    return 0;
}