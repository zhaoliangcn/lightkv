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

int main() {
    TestWALWriteRead();

    std::cout << "All WAL tests passed!" << std::endl;
    return 0;
}