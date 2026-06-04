#include "lightkv/db.h"
#include "lightkv/db_impl.h"
#include <iostream>
#include <cassert>
#include <string>
#include <cstdlib>
#include <thread>
#include <chrono>

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

void TestIterator() {
    lightkv::Options options;
    options.db_path = "/tmp/lightkv_test_iter";
    options.create_if_missing = true;
    options.memtable_size = 1024 * 1024;

    std::system("rm -rf /tmp/lightkv_test_iter");

    lightkv::DB* db = nullptr;
    auto s = lightkv::DB::Open(options, &db);
    assert(s.ok());

    // Insert ordered data
    for (int i = 0; i < 100; ++i) {
        char key[16], val[32];
        snprintf(key, sizeof(key), "%03d", i);
        snprintf(val, sizeof(val), "val_%03d", i);
        db->Put(lightkv::WriteOptions(), key, val);
    }

    // Test SeekToFirst + Next
    {
        auto* impl = static_cast<lightkv::DBImpl*>(db);
        lightkv::DBImpl::Iterator iter(impl, 0);
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

    // Test Seek
    {
        auto* impl = static_cast<lightkv::DBImpl*>(db);
        lightkv::DBImpl::Iterator iter(impl, 0);
        iter.Seek("050");

        assert(iter.Valid());
        assert(iter.key() == lightkv::Slice("050"));
        iter.Next();
        assert(iter.Valid());
        assert(iter.key() == lightkv::Slice("051"));
    }

    // Test Seek to non-existent key (should find next)
    {
        auto* impl = static_cast<lightkv::DBImpl*>(db);
        lightkv::DBImpl::Iterator iter(impl, 0);
        iter.Seek("050xyz");

        assert(iter.Valid());
        assert(iter.key() == lightkv::Slice("051"));
    }

    // Test Seek past all keys
    {
        auto* impl = static_cast<lightkv::DBImpl*>(db);
        lightkv::DBImpl::Iterator iter(impl, 0);
        iter.Seek("999");
        assert(!iter.Valid());
    }

    // Test iterator after delete
    {
        db->Put(lightkv::WriteOptions(), "del_key", "value");
        db->Delete(lightkv::WriteOptions(), "del_key");

        auto* impl = static_cast<lightkv::DBImpl*>(db);
        lightkv::DBImpl::Iterator iter(impl, 0);
        iter.Seek("del_key");
        // Deleted key should be skipped
        // Iterator should not return "del_key"
    }

    delete db;
    std::system("rm -rf /tmp/lightkv_test_iter");
}

void TestIteratorWithSSTable() {
    lightkv::Options options;
    options.db_path = "/tmp/lightkv_test_iter_sst";
    options.create_if_missing = true;
    options.memtable_size = 1024 * 100; // Small memtable to trigger flush

    std::system("rm -rf /tmp/lightkv_test_iter_sst");

    lightkv::DB* db = nullptr;
    auto s = lightkv::DB::Open(options, &db);
    assert(s.ok());

    // Write enough data to trigger multiple flushes
    for (int i = 0; i < 500; ++i) {
        char key[16], val[32];
        snprintf(key, sizeof(key), "%04d", i);
        snprintf(val, sizeof(val), "val_%04d", i);
        db->Put(lightkv::WriteOptions(), key, val);
    }

    // Iterate over data that spans MemTable + SSTable
    {
        auto* impl = static_cast<lightkv::DBImpl*>(db);
        lightkv::DBImpl::Iterator iter(impl, 0);
        iter.SeekToFirst();

        int count = 0;
        while (iter.Valid()) {
            iter.Next();
            ++count;
        }
        assert(count == 500);
        (void)count;  // suppress unused variable warning
    }

    delete db;
    std::system("rm -rf /tmp/lightkv_test_iter_sst");
}

void TestCompaction() {
    lightkv::Options options;
    options.db_path = "/tmp/lightkv_test_compact";
    options.create_if_missing = true;
    options.memtable_size = 1024 * 100; // 100KB to trigger flush
    options.l0_file_num_trigger = 4;

    std::system("rm -rf /tmp/lightkv_test_compact");

    lightkv::DB* db = nullptr;
    auto s = lightkv::DB::Open(options, &db);
    assert(s.ok());

    // Write enough data to trigger multiple flushes and compaction
    for (int round = 0; round < 10; ++round) {
        for (int i = 0; i < 200; ++i) {
            char key[16], val[64];
            snprintf(key, sizeof(key), "%04d_%04d", round, i);
            snprintf(val, sizeof(val), "value_%04d_%04d", round, i);
            db->Put(lightkv::WriteOptions(), key, val);
        }
    }

    // Give background thread time to compact
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // Verify data is still readable after compaction
    for (int round = 0; round < 10; ++round) {
        for (int i = 0; i < 200; ++i) {
            char key[16], expected_val[64];
            snprintf(key, sizeof(key), "%04d_%04d", round, i);
            snprintf(expected_val, sizeof(expected_val), "value_%04d_%04d", round, i);
            std::string value;
            auto status = db->Get(lightkv::ReadOptions(), key, &value);
            assert(status.ok());
            assert(value == expected_val);
        }
    }

    // Check stats
    auto* impl = static_cast<lightkv::DBImpl*>(db);
    auto stats = impl->GetStats();
    assert(stats.total_writes > 0);
    assert(stats.total_flushes > 0);
    (void)stats;  // suppress unused variable warning

    delete db;
    std::system("rm -rf /tmp/lightkv_test_compact");
}

void TestGetStats() {
    lightkv::Options options;
    options.db_path = "/tmp/lightkv_test_stats";
    options.create_if_missing = true;
    options.memtable_size = 1024 * 1024;

    std::system("rm -rf /tmp/lightkv_test_stats");

    lightkv::DB* db = nullptr;
    auto s = lightkv::DB::Open(options, &db);
    assert(s.ok());

    auto* impl = static_cast<lightkv::DBImpl*>(db);
    auto stats = impl->GetStats();
    assert(stats.total_writes == 0);
    assert(stats.total_reads == 0);

    // Do some operations
    db->Put(lightkv::WriteOptions(), "k1", "v1");
    db->Put(lightkv::WriteOptions(), "k2", "v2");
    db->Delete(lightkv::WriteOptions(), "k1");

    std::string value;
    db->Get(lightkv::ReadOptions(), "k2", &value);
    db->Get(lightkv::ReadOptions(), "nonexistent", &value);

    stats = impl->GetStats();
    assert(stats.total_writes == 3);  // 2 puts + 1 delete
    assert(stats.total_reads == 2);
    assert(stats.total_deletes == 1);

    delete db;
    std::system("rm -rf /tmp/lightkv_test_stats");
}

void TestEmptyDB() {
    lightkv::Options options;
    options.db_path = "/tmp/lightkv_test_empty";
    options.create_if_missing = true;

    std::system("rm -rf /tmp/lightkv_test_empty");

    lightkv::DB* db = nullptr;
    auto s = lightkv::DB::Open(options, &db);
    assert(s.ok());

    // Get from empty DB
    std::string value;
    s = db->Get(lightkv::ReadOptions(), "nonexistent", &value);
    assert(s.IsNotFound());

    // Delete from empty DB (should not crash)
    s = db->Delete(lightkv::WriteOptions(), "nonexistent");
    assert(s.ok());

    // Iterator on empty DB
    {
        auto* impl = static_cast<lightkv::DBImpl*>(db);
        lightkv::DBImpl::Iterator iter(impl, 0);
        iter.SeekToFirst();
        assert(!iter.Valid());
    }

    delete db;
    std::system("rm -rf /tmp/lightkv_test_empty");
}

void TestMultipleReopen() {
    lightkv::Options options;
    options.db_path = "/tmp/lightkv_test_reopen";
    options.create_if_missing = true;
    options.memtable_size = 1024 * 1024;

    std::system("rm -rf /tmp/lightkv_test_reopen");

    // Write, close, reopen, verify - multiple times
    for (int round = 0; round < 5; ++round) {
        lightkv::DB* db = nullptr;
        auto s = lightkv::DB::Open(options, &db);
        assert(s.ok());

        // Write data
        for (int i = 0; i < 100; ++i) {
            char key[16], val[32];
            snprintf(key, sizeof(key), "r%d_%03d", round, i);
            snprintf(val, sizeof(val), "v%d_%03d", round, i);
            db->Put(lightkv::WriteOptions(), key, val);
        }

        // Verify previous rounds' data still exists
        for (int r = 0; r <= round; ++r) {
            for (int i = 0; i < 100; ++i) {
                char key[16], expected_val[32];
                snprintf(key, sizeof(key), "r%d_%03d", r, i);
                snprintf(expected_val, sizeof(expected_val), "v%d_%03d", r, i);
                std::string value;
                s = db->Get(lightkv::ReadOptions(), key, &value);
                assert(s.ok());
                assert(value == expected_val);
            }
        }

        delete db;
    }

    std::system("rm -rf /tmp/lightkv_test_reopen");
}

void TestOverwriteAndDelete() {
    lightkv::Options options;
    options.db_path = "/tmp/lightkv_test_overwrite";
    options.create_if_missing = true;
    options.memtable_size = 1024 * 1024;

    std::system("rm -rf /tmp/lightkv_test_overwrite");

    lightkv::DB* db = nullptr;
    auto s = lightkv::DB::Open(options, &db);
    assert(s.ok());

    // Overwrite same key multiple times
    for (int i = 0; i < 10; ++i) {
        db->Put(lightkv::WriteOptions(), "same_key", "value_" + std::to_string(i));
    }

    std::string value;
    s = db->Get(lightkv::ReadOptions(), "same_key", &value);
    assert(s.ok());
    assert(value == "value_9");

    // Delete and re-insert
    db->Delete(lightkv::WriteOptions(), "same_key");
    s = db->Get(lightkv::ReadOptions(), "same_key", &value);
    assert(s.IsNotFound());

    db->Put(lightkv::WriteOptions(), "same_key", "new_value");
    s = db->Get(lightkv::ReadOptions(), "same_key", &value);
    assert(s.ok());
    assert(value == "new_value");

    delete db;
    std::system("rm -rf /tmp/lightkv_test_overwrite");
}

void TestIteratorAfterCompaction() {
    lightkv::Options options;
    options.db_path = "/tmp/lightkv_test_iter_compact";
    options.create_if_missing = true;
    options.memtable_size = 1024 * 50; // Very small to trigger many flushes
    options.l0_file_num_trigger = 2;

    std::system("rm -rf /tmp/lightkv_test_iter_compact");

    lightkv::DB* db = nullptr;
    auto s = lightkv::DB::Open(options, &db);
    assert(s.ok());

    // Write data that will span multiple SSTable files
    for (int i = 0; i < 1000; ++i) {
        char key[16], val[32];
        snprintf(key, sizeof(key), "%04d", i);
        snprintf(val, sizeof(val), "v%04d", i);
        db->Put(lightkv::WriteOptions(), key, val);
    }

    // Wait for compaction
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    // Iterate and verify all data
    {
        auto* impl = static_cast<lightkv::DBImpl*>(db);
        lightkv::DBImpl::Iterator iter(impl, 0);
        iter.SeekToFirst();

        int count = 0;
        while (iter.Valid()) {
            char expected_key[16], expected_val[32];
            snprintf(expected_key, sizeof(expected_key), "%04d", count);
            snprintf(expected_val, sizeof(expected_val), "v%04d", count);
            assert(iter.key() == lightkv::Slice(expected_key));
            assert(iter.value() == lightkv::Slice(expected_val));
            iter.Next();
            ++count;
        }
        assert(count == 1000);
    }

    // Seek to middle
    {
        auto* impl = static_cast<lightkv::DBImpl*>(db);
        lightkv::DBImpl::Iterator iter(impl, 0);
        iter.Seek("0500");
        assert(iter.Valid());
        assert(iter.key() == lightkv::Slice("0500"));
    }

    delete db;
    std::system("rm -rf /tmp/lightkv_test_iter_compact");
}

int main() {
    TestBasicOperations();
    TestPersistence();
    TestIterator();
    TestIteratorWithSSTable();
    TestCompaction();
    TestGetStats();
    TestEmptyDB();
    TestMultipleReopen();
    TestOverwriteAndDelete();
    TestIteratorAfterCompaction();

    std::cout << "All DB tests passed!" << std::endl;
    return 0;
}