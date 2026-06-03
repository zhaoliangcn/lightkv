#include "lightkv/db.h"
#include <iostream>
#include <cassert>
#include <string>
#include <cstdlib>

void TestBasicOperations() {
    lightkv::Options options;
    options.db_path = "/tmp/lightkv_test_db";
    options.create_if_missing = true;
    options.memtable_size = 1024 * 1024; // 1MB for testing

    // Clean up previous test data
    std::system("rm -rf /tmp/lightkv_test_db");

    lightkv::DB* db = nullptr;
    auto s = lightkv::DB::Open(options, &db);
    assert(s.ok());
    assert(db != nullptr);

    lightkv::WriteOptions write_opts;
    lightkv::ReadOptions read_opts;

    // Put
    s = db->Put(write_opts, "hello", "world");
    assert(s.ok());

    s = db->Put(write_opts, "foo", "bar");
    assert(s.ok());

    // Get
    std::string value;
    s = db->Get(read_opts, "hello", &value);
    assert(s.ok());
    assert(value == "world");

    s = db->Get(read_opts, "foo", &value);
    assert(s.ok());
    assert(value == "bar");

    // Get non-existent
    s = db->Get(read_opts, "nonexistent", &value);
    assert(s.IsNotFound());

    // Delete
    s = db->Delete(write_opts, "hello");
    assert(s.ok());

    s = db->Get(read_opts, "hello", &value);
    assert(s.IsNotFound());

    // Overwrite
    s = db->Put(write_opts, "foo", "baz");
    assert(s.ok());

    s = db->Get(read_opts, "foo", &value);
    assert(s.ok());
    assert(value == "baz");

    delete db;

    // Clean up
    std::system("rm -rf /tmp/lightkv_test_db");
}

void TestPersistence() {
    lightkv::Options options;
    options.db_path = "/tmp/lightkv_test_db2";
    options.create_if_missing = true;

    std::system("rm -rf /tmp/lightkv_test_db2");

    // Write some data
    {
        lightkv::DB* db = nullptr;
        auto s = lightkv::DB::Open(options, &db);
        assert(s.ok());

        for (int i = 0; i < 1000; ++i) {
            s = db->Put(lightkv::WriteOptions(),
                        "key" + std::to_string(i),
                        "value" + std::to_string(i));
            assert(s.ok());
        }

        delete db;
    }

    // Re-open and read
    {
        lightkv::DB* db = nullptr;
        auto s = lightkv::DB::Open(options, &db);
        assert(s.ok());

        lightkv::ReadOptions read_opts;
        std::string value;

        for (int i = 0; i < 1000; ++i) {
            s = db->Get(read_opts, "key" + std::to_string(i), &value);
            assert(s.ok());
            assert(value == "value" + std::to_string(i));
        }

        delete db;
    }

    std::system("rm -rf /tmp/lightkv_test_db2");
}

int main() {
    TestBasicOperations();
    TestPersistence();

    std::cout << "All DB tests passed!" << std::endl;
    return 0;
}