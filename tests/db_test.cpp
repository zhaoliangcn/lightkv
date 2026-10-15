#include "test_paths.h"
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
    options.db_path = LIGHTKV_TEST_TMP "/lightkv_test_db";
    options.create_if_missing = true;
    options.memtable_size = 1024 * 1024; // 1MB for testing

    // Clean up previous test data
    lightkv_remove_tree(LIGHTKV_TEST_TMP "/lightkv_test_db");

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
    lightkv_remove_tree(LIGHTKV_TEST_TMP "/lightkv_test_db");
}

void TestPersistence() {
    lightkv::Options options;
    options.db_path = LIGHTKV_TEST_TMP "/lightkv_test_db2";
    options.create_if_missing = true;

    lightkv_remove_tree(LIGHTKV_TEST_TMP "/lightkv_test_db2");

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
        if (!s.ok()) std::fprintf(stderr, "  reopen failed: %s\n", s.ToString().c_str());
        assert(s.ok());

        lightkv::ReadOptions read_opts;
        std::string value;

        {
            auto* impl = static_cast<lightkv::DBImpl*>(db);
            auto st = impl->GetStats();
            std::fprintf(stderr, "  reopen: last_seq=%llu  L0 bytes=%llu  flushes=%llu\n",
                         (unsigned long long)impl->LastSeq(),
                         (unsigned long long)st.level_sizes[0],
                         (unsigned long long)st.total_flushes);
        }

        int misses = 0;
        for (int i = 0; i < 1000; ++i) {
            s = db->Get(read_opts, "key" + std::to_string(i), &value);
            if (!s.ok() && misses < 10) {
                std::fprintf(stderr, "  reopen: Get(key%d) -> %s\n", i, s.ToString().c_str());
                ++misses;
            }
            if (!s.ok()) continue;
            if (value != "value" + std::to_string(i)) {
                if (misses < 10) {
                    std::fprintf(stderr, "  reopen: Get(key%d) -> '%s' (wrong value)\n", i, value.c_str());
                    ++misses;
                }
            }
        }
        std::fprintf(stderr, "  reopen: %d problems in first 1000 keys\n", misses);
        assert(misses == 0);

        delete db;
    }

    lightkv_remove_tree(LIGHTKV_TEST_TMP "/lightkv_test_db2");
}

void TestIterator() {
    lightkv::Options options;
    options.db_path = LIGHTKV_TEST_TMP "/lightkv_test_iter";
    options.create_if_missing = true;
    options.memtable_size = 1024 * 1024;

    lightkv_remove_tree(LIGHTKV_TEST_TMP "/lightkv_test_iter");

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
        // Use UINT64_MAX as snapshot to see all entries (bypass snapshot visibility check)
        lightkv::DBImpl::Iterator iter(impl, UINT64_MAX);
        iter.SeekToFirst();

        int count = 0;
        while (iter.Valid()) {
            char expected_key[16], expected_val[32];
            snprintf(expected_key, sizeof(expected_key), "%03d", count);
            snprintf(expected_val, sizeof(expected_val), "val_%03d", count);
            if (!(iter.key() == lightkv::Slice(expected_key))) {
                std::cerr << "FAIL key: iter=" << iter.key().ToString() 
                          << " expected=" << expected_key << " count=" << count << std::endl;
                assert(false);
            }
            auto iter_val_slice = iter.value();
            std::string iter_val_str = iter_val_slice.ToString();
            std::string expected_val_str(expected_val);
            if (iter_val_str != expected_val_str) {
                std::cerr << "FAIL val: key=" << iter.key().ToString()
                          << " iter_val='" << iter_val_str << "'"
                          << " iter_val_len=" << iter_val_str.size()
                          << " expected='" << expected_val_str << "'" 
                          << " expected_len=" << expected_val_str.size()
                          << " count=" << count << std::endl;
                std::cerr << "Slice equality: " 
                          << (iter.value() == lightkv::Slice(expected_val)) << std::endl;
                std::cerr << "data pointer iter=" << (void*)iter_val_slice.data()
                          << " expected=" << (void*)expected_val << std::endl;
                assert(false);
            }
            iter.Next();
            ++count;
        }
        std::cout << "Iterator count: " << count << std::endl;
        assert(count == 100);
    }

    // Test Seek
    {
        auto* impl = static_cast<lightkv::DBImpl*>(db);
        lightkv::DBImpl::Iterator iter(impl, UINT64_MAX);
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
        lightkv::DBImpl::Iterator iter(impl, UINT64_MAX);
        iter.Seek("050xyz");

        assert(iter.Valid());
        assert(iter.key() == lightkv::Slice("051"));
    }

    // Test Seek past all keys
    {
        auto* impl = static_cast<lightkv::DBImpl*>(db);
        lightkv::DBImpl::Iterator iter(impl, UINT64_MAX);
        iter.Seek("999");
        assert(!iter.Valid());
    }

    // Test iterator after delete
    {
        db->Put(lightkv::WriteOptions(), "del_key", "value");
        db->Delete(lightkv::WriteOptions(), "del_key");

        auto* impl = static_cast<lightkv::DBImpl*>(db);
        lightkv::DBImpl::Iterator iter(impl, UINT64_MAX);
        iter.Seek("del_key");
        // Deleted key should be skipped
        // Iterator should not return "del_key"
    }

    delete db;
    lightkv_remove_tree(LIGHTKV_TEST_TMP "/lightkv_test_iter");
}

void TestIteratorWithSSTable() {
    lightkv::Options options;
    options.db_path = LIGHTKV_TEST_TMP "/lightkv_test_iter_sst";
    options.create_if_missing = true;
    options.memtable_size = 1024 * 100; // Small memtable to trigger flush

    lightkv_remove_tree(LIGHTKV_TEST_TMP "/lightkv_test_iter_sst");

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
        lightkv::DBImpl::Iterator iter(impl, UINT64_MAX);
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
    lightkv_remove_tree(LIGHTKV_TEST_TMP "/lightkv_test_iter_sst");
}

void TestCompaction() {
    lightkv::Options options;
    options.db_path = LIGHTKV_TEST_TMP "/lightkv_test_compact";
    options.create_if_missing = true;
    options.memtable_size = 1024 * 10; // 10KB to trigger flush more easily
    options.l0_file_num_trigger = 4;

    lightkv_remove_tree(LIGHTKV_TEST_TMP "/lightkv_test_compact");

    lightkv::DB* db = nullptr;
    auto s = lightkv::DB::Open(options, &db);
    assert(s.ok());

    // Write enough data to trigger multiple flushes and compaction
    // Use sync=true to ensure TriggerFlush is called
    lightkv::WriteOptions sync_opts;
    sync_opts.sync = true;
    for (int round = 0; round < 10; ++round) {
        for (int i = 0; i < 200; ++i) {
            char key[16], val[64];
            snprintf(key, sizeof(key), "%04d_%04d", round, i);
            snprintf(val, sizeof(val), "value_%04d_%04d", round, i);
            db->Put(sync_opts, key, val);
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
    lightkv_remove_tree(LIGHTKV_TEST_TMP "/lightkv_test_compact");
}

void TestGetStats() {
    lightkv::Options options;
    options.db_path = LIGHTKV_TEST_TMP "/lightkv_test_stats";
    options.create_if_missing = true;
    options.memtable_size = 1024 * 1024;

    lightkv_remove_tree(LIGHTKV_TEST_TMP "/lightkv_test_stats");

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
    assert(stats.total_writes == 2);   // 2 puts
    assert(stats.total_reads == 2);
    assert(stats.total_deletes == 1);  // 1 delete

    delete db;
    lightkv_remove_tree(LIGHTKV_TEST_TMP "/lightkv_test_stats");
}

void TestEmptyDB() {
    lightkv::Options options;
    options.db_path = LIGHTKV_TEST_TMP "/lightkv_test_empty";
    options.create_if_missing = true;

    lightkv_remove_tree(LIGHTKV_TEST_TMP "/lightkv_test_empty");

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
        lightkv::DBImpl::Iterator iter(impl, UINT64_MAX);
        iter.SeekToFirst();
        assert(!iter.Valid());
    }

    delete db;
    lightkv_remove_tree(LIGHTKV_TEST_TMP "/lightkv_test_empty");
}

void TestMultipleReopen() {
    lightkv::Options options;
    options.db_path = LIGHTKV_TEST_TMP "/lightkv_test_reopen";
    options.create_if_missing = true;
    options.memtable_size = 1024 * 1024;

    lightkv_remove_tree(LIGHTKV_TEST_TMP "/lightkv_test_reopen");

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
                if (!s.ok()) {
                    std::fprintf(stderr, "  round=%d key=%s Get failed: %s\n",
                                 round, key, s.ToString().c_str());
                }
                assert(s.ok());
                if (s.ok() && value != expected_val) {
                    std::fprintf(stderr, "  round=%d key=%s got='%s' want='%s'\n",
                                 round, key, value.c_str(), expected_val);
                }
                assert(value == expected_val);
            }
        }

        delete db;
    }

    lightkv_remove_tree(LIGHTKV_TEST_TMP "/lightkv_test_reopen");
}

void TestOverwriteAndDelete() {
    lightkv::Options options;
    options.db_path = LIGHTKV_TEST_TMP "/lightkv_test_overwrite";
    options.create_if_missing = true;
    options.memtable_size = 1024 * 1024;

    lightkv_remove_tree(LIGHTKV_TEST_TMP "/lightkv_test_overwrite");

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
    lightkv_remove_tree(LIGHTKV_TEST_TMP "/lightkv_test_overwrite");
}

void TestIteratorAfterCompaction() {
    lightkv::Options options;
    options.db_path = LIGHTKV_TEST_TMP "/lightkv_test_iter_compact";
    options.create_if_missing = true;
    options.memtable_size = 1024 * 50; // Very small to trigger many flushes
    options.l0_file_num_trigger = 2;

    lightkv_remove_tree(LIGHTKV_TEST_TMP "/lightkv_test_iter_compact");

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
        lightkv::DBImpl::Iterator iter(impl, UINT64_MAX);
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
        lightkv::DBImpl::Iterator iter(impl, UINT64_MAX);
        iter.Seek("0500");
        assert(iter.Valid());
        assert(iter.key() == lightkv::Slice("0500"));
    }

    delete db;
    lightkv_remove_tree(LIGHTKV_TEST_TMP "/lightkv_test_iter_compact");
}

// Regression test for the "newest write silently lost during compaction" bug.
//
// SSTable entries carry no sequence number (SSTable::Iterator::seq() always
// returns 0), so when the SAME key exists in two different input files the
// merge heap cannot tell which version is newer and the winner is arbitrary.
// The scenario below forces exactly that: flush v1, overwrite with v2, flush
// again, then let L0 compaction merge both files.
void TestCompactionKeepsNewestVersion() {
    const int kRounds = 40;
    const int kKeys = 16;
    std::string db_path = LIGHTKV_TEST_TMP "/lightkv_test_newest_wins";
    lightkv_remove_tree(db_path);

    lightkv::Options options;
    options.db_path = db_path;
    options.create_if_missing = true;
    options.memtable_size = 4 * 1024;   // tiny: forces a flush every few writes
    options.l0_file_num_trigger = 2;    // compact aggressively

    lightkv::DB* db = nullptr;
    auto s = lightkv::DB::Open(options, &db);
    assert(s.ok());

    auto key_of = [](int k) { return "contended_" + std::to_string(k); };

    // Overwrite the SAME keys every round, writing enough filler to force a
    // flush between rounds so each version lands in its own L0 file. The check
    // runs after EVERY round, so a single bad merge is caught at the round that
    // caused it rather than being masked by later writes.
    for (int round = 0; round < kRounds; ++round) {
        const std::string expected = "round_" + std::to_string(round);

        for (int i = 0; i < 200; ++i) {
            char filler[48];
            snprintf(filler, sizeof(filler), "filler_%06d_%03d", round, i);
            db->Put(lightkv::WriteOptions(), filler, expected);
        }
        for (int k = 0; k < kKeys; ++k) {
            db->Put(lightkv::WriteOptions(), key_of(k), expected);
        }
        for (int i = 0; i < 200; ++i) {
            char pad[48];
            snprintf(pad, sizeof(pad), "pad_%06d_%03d", round, i);
            db->Put(lightkv::WriteOptions(), pad, "p");
        }

        // Let the background thread flush and compact this round.
        std::this_thread::sleep_for(std::chrono::milliseconds(80));

        for (int k = 0; k < kKeys; ++k) {
            std::string got;
            auto gs = db->Get(lightkv::ReadOptions(), key_of(k), &got);
            if (!gs.ok() || got != expected) {
                std::fprintf(stderr,
                             "  compaction lost the newest version at round=%d key=%s: "
                             "Get -> %s, value='%s', expected='%s'\n",
                             round, key_of(k).c_str(), gs.ToString().c_str(),
                             got.c_str(), expected.c_str());
                assert(gs.ok());
                assert(got == expected);
            }
        }
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(800));

    // Guard: the test is only meaningful if compaction actually ran.
    auto* impl = static_cast<lightkv::DBImpl*>(db);
    auto stats = impl->GetStats();
    std::fprintf(stderr, "  [newest-wins] flushes=%llu compactions=%llu\n",
                 (unsigned long long)stats.total_flushes,
                 (unsigned long long)stats.total_compactions);
    assert(stats.total_compactions > 0);

    // The SCAN path uses MergingIterator rather than DBImpl::Get, so it needs its
    // own check that the newest version wins there too.
    {
        const std::string expected = "round_" + std::to_string(kRounds - 1);
        std::vector<std::pair<std::string, std::string>> rows;
        auto ss = db->Scan(lightkv::ReadOptions(), "contended_", 1000, &rows);
        assert(ss.ok());
        int seen = 0;
        for (const auto& row : rows) {
            if (row.first.rfind("contended_", 0) != 0) continue;
            ++seen;
            if (row.second != expected) {
                std::fprintf(stderr,
                             "  SCAN returned a stale version for %s: '%s', expected '%s'\n",
                             row.first.c_str(), row.second.c_str(), expected.c_str());
                assert(row.second == expected);
            }
        }
        std::fprintf(stderr, "  [newest-wins] scan rows=%zu contended=%d\n",
                     rows.size(), seen);
        assert(seen == kKeys);
    }

    delete db;
    lightkv_remove_tree(db_path);
}

// Snapshot reads must observe the value as of ReadOptions::snapshot_seq.
// v1 is flushed to disk BEFORE the snapshot is taken; v2 is flushed after.
// Reading at the snapshot must return v1 even though v2 lives in a newer file.
void TestSnapshotReadIsolation() {
    std::string db_path = LIGHTKV_TEST_TMP "/lightkv_test_snapshot";
    lightkv_remove_tree(db_path);

    lightkv::Options options;
    options.db_path = db_path;
    options.create_if_missing = true;
    options.memtable_size = 4 * 1024;
    options.l0_file_num_trigger = 2;

    lightkv::DB* db = nullptr;
    auto s = lightkv::DB::Open(options, &db);
    assert(s.ok());
    auto* impl = static_cast<lightkv::DBImpl*>(db);

    // Force everything to disk so the reads below cannot be served from the
    // memtable (which does have real sequence numbers).
    auto flush_all = [&]() {
        for (int i = 0; i < 300; ++i) {
            char k[48];
            snprintf(k, sizeof(k), "filler_%03d", i);
            db->Put(lightkv::WriteOptions(), k, "x");
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(120));
    };

    db->Put(lightkv::WriteOptions(), "snapkey", "v1");
    flush_all();

    // Take the snapshot AFTER v1 is durable but BEFORE v2 is written.
    // LastSeq() is the NEXT sequence number to be assigned, and snapshot_seq is
    // inclusive, so the largest visible seq right now is LastSeq() - 1.
    const uint64_t snap = impl->LastSeq() - 1;
    std::fprintf(stderr, "  [snapshot] snap_seq=%llu\n", (unsigned long long)snap);

    db->Put(lightkv::WriteOptions(), "snapkey", "v2");
    flush_all();

    // Latest read must see v2.
    {
        std::string v;
        auto gs = db->Get(lightkv::ReadOptions(), "snapkey", &v);
        assert(gs.ok());
        if (v != "v2") {
            std::fprintf(stderr, "  latest read got '%s', expected 'v2'\n", v.c_str());
        }
        assert(v == "v2");
    }

    // A snapshot read must observe the value as it was at that sequence number.
    // v1 is flushed to disk BEFORE the snapshot is taken; v2 after. Reading at the
    // snapshot must return v1 even though v2 lives in a newer file.
    // (Regression: before the internal-key format, snapshot_seq was ignored for
    // flushed data and this silently returned v2.)
    {
        lightkv::ReadOptions ro;
        ro.snapshot_seq = snap;
        std::string v;
        auto gs = db->Get(ro, "snapkey", &v);
        if (!gs.ok() || v != "v1") {
            std::fprintf(stderr,
                         "  snapshot read FAILED: Get -> %s, value='%s', expected 'v1'\n",
                         gs.ToString().c_str(), v.c_str());
        }
        assert(gs.ok());
        assert(v == "v1");

        // The bound is inclusive: one sequence further must reveal v2.
        lightkv::ReadOptions next;
        next.snapshot_seq = snap + 1;
        std::string newer;
        auto ns = db->Get(next, "snapkey", &newer);
        if (!ns.ok() || newer != "v2") {
            std::fprintf(stderr,
                         "  snapshot+1 -> %s '%s', expected 'v2'\n",
                         ns.ToString().c_str(), newer.c_str());
        }
        assert(ns.ok());
        assert(newer == "v2");

        // A plain read still sees the newest value.
        std::string latest;
        auto ls = db->Get(lightkv::ReadOptions(), "snapkey", &latest);
        assert(ls.ok());
        assert(latest == "v2");
    }

    delete db;
    lightkv_remove_tree(db_path);
}

// DELRANGE must report how many live keys it shadowed, and a second identical
// range must report 0 (the keys are already gone).
void TestDeleteRangeReportsCount() {
    std::string db_path = LIGHTKV_TEST_TMP "/lightkv_test_delrange_count";
    lightkv_remove_tree(db_path);

    lightkv::Options options;
    options.db_path = db_path;
    options.create_if_missing = true;

    lightkv::DB* db = nullptr;
    auto s = lightkv::DB::Open(options, &db);
    assert(s.ok());

    lightkv::WriteOptions wo;
    int64_t n = -1;

    s = db->DeleteRange(wo, "a", "z", &n);
    assert(s.ok());
    assert(n == 0);          // nothing stored yet

    for (int i = 0; i < 5; ++i) {
        assert(db->Put(wo, "k" + std::to_string(i), "v").ok());
    }
    // Keys outside the range must not be counted.
    assert(db->Put(wo, "z0", "v").ok());

    s = db->DeleteRange(wo, "a", "z", &n);
    assert(s.ok());
    if (n != 5) std::fprintf(stderr, "  DeleteRange counted %lld, expected 5\n", (long long)n);
    assert(n == 5);

    // Already-deleted keys must not be counted again.
    s = db->DeleteRange(wo, "a", "z", &n);
    assert(s.ok());
    if (n != 0) std::fprintf(stderr, "  second DeleteRange counted %lld, expected 0\n", (long long)n);
    assert(n == 0);

    // And they must be invisible to iteration.
    std::vector<std::pair<std::string, std::string>> rows;
    assert(db->Scan(lightkv::ReadOptions(), "", 100, &rows).ok());
    for (const auto& r : rows) {
        if (r.first.rfind("k", 0) == 0 && r.first.size() == 2) {
            std::fprintf(stderr, "  range-deleted key %s still visible in Scan\n",
                         r.first.c_str());
            assert(false);
        }
    }

    // A write made AFTER a DELRANGE must stay visible: the tombstone only
    // shadows versions older than itself.
    assert(db->Put(wo, "k0", "after").ok());
    {
        std::string v;
        auto st = db->Get(lightkv::ReadOptions(), "k0", &v);
        if (!st.ok() || v != "after") {
            std::fprintf(stderr,
                         "  write after DELRANGE is invisible: %s '%s'\n",
                         st.ToString().c_str(), v.c_str());
        }
        assert(st.ok());
        assert(v == "after");
    }
    // ...and it is counted again by a later DeleteRange.
    s = db->DeleteRange(wo, "a", "z", &n);
    assert(s.ok());
    if (n != 1) {
        std::fprintf(stderr, "  DeleteRange after rewrite counted %lld, expected 1\n",
                     (long long)n);
    }
    assert(n == 1);

    delete db;
    lightkv_remove_tree(db_path);
}

// Backup() has to produce a directory that DB::Open can read back on its own.
// The previous implementation only copied SSTables plus the WAL and rewrote the
// manifest outside the DB lock, which lost three different things:
//
//   1. writes still sitting in the memtable (it hoped the WAL would cover them,
//      but copied the whole 64MB preallocated mmap file, unsynced)
//   2. values above value_threshold, which live in vlog-*.log and are only
//      referenced by pointer from the SSTable
//   3. consistency, since a concurrent compaction could add or unlink a file
//      between the copy and the manifest write
void TestBackupIsRestorable() {
    const std::string db_path = LIGHTKV_TEST_TMP "/lightkv_backup_src";
    const std::string backup_path = LIGHTKV_TEST_TMP "/lightkv_backup_dst";
    lightkv_remove_tree(db_path);
    lightkv_remove_tree(backup_path);
    lightkv_remove_tree(backup_path + ".tmp");

    lightkv::Options options;
    options.db_path = db_path;
    options.create_if_missing = true;
    // Force large values out of line so the vlog copy path is exercised.
    options.value_threshold = 256;

    lightkv::DB* db = nullptr;
    assert(lightkv::DB::Open(options, &db).ok());

    lightkv::WriteOptions wo;
    for (int i = 0; i < 200; ++i) {
        assert(db->Put(wo, "small_" + std::to_string(i),
                       "v" + std::to_string(i)).ok());
    }
    // Big values: these never reach the SSTable, only a vlog pointer does.
    const std::string big1(4096, 'A');
    const std::string big2(8192, 'B');
    assert(db->Put(wo, "big1", big1).ok());
    assert(db->Put(wo, "big2", big2).ok());
    // Overwrite one key so the backup has to keep the newest version.
    assert(db->Put(wo, "small_7", "latest").ok());
    assert(db->Delete(wo, "small_8").ok());

    auto* impl = static_cast<lightkv::DBImpl*>(db);
    auto bs = impl->Backup(backup_path);
    if (!bs.ok()) {
        std::fprintf(stderr, "  Backup failed: %s\n", bs.ToString().c_str());
    }
    assert(bs.ok());

    // The backup must not ship a WAL: it is a standalone image, and DB::Open
    // creates a fresh one. A 64MB preallocated wal.log would also make every
    // backup enormous.
    {
        std::error_code ec;
        if (std::filesystem::exists(backup_path + "/wal.log", ec)) {
            std::fprintf(stderr, "  backup contains a wal.log\n");
        }
        assert(!std::filesystem::exists(backup_path + "/wal.log", ec));
    }
    // The staging directory must be cleaned up.
    {
        std::error_code ec;
        assert(!std::filesystem::exists(backup_path + ".tmp", ec));
    }

    // Close the source so nothing is still writing.
    delete db;
    db = nullptr;

    // Open the backup as a database in its own right.
    lightkv::Options restored = options;
    restored.db_path = backup_path;
    lightkv::DB* backup = nullptr;
    auto rs = lightkv::DB::Open(restored, &backup);
    if (!rs.ok()) {
        std::fprintf(stderr, "  cannot open the backup: %s\n", rs.ToString().c_str());
    }
    assert(rs.ok());

    lightkv::ReadOptions ro;
    std::string value;

    // (2) Large values survived -- this is what the vlog copy is for.
    assert(backup->Get(ro, "big1", &value).ok());
    if (value != big1) {
        std::fprintf(stderr, "  big1 differs after restore (size %zu vs %zu)\n",
                     value.size(), big1.size());
    }
    assert(value == big1);
    assert(backup->Get(ro, "big2", &value).ok());
    assert(value == big2);

    // (1) Data that was only in the memtable made it across.
    for (int i = 0; i < 200; ++i) {
        const std::string key = "small_" + std::to_string(i);

        // small_8 was deleted before the backup: it must stay deleted, which
        // means the backup has to carry the tombstone rather than just the
        // surviving values.
        if (i == 8) {
            if (backup->Exists(ro, key)) {
                std::fprintf(stderr, "  deleted key %s came back after restore\n",
                             key.c_str());
                assert(false);
            }
            continue;
        }

        auto st = backup->Get(ro, key, &value);
        if (!st.ok()) {
            std::fprintf(stderr, "  key %s missing from backup: %s\n", key.c_str(),
                         st.ToString().c_str());
            assert(false);
        }
        if (i == 7) {
            // Overwritten before the backup: the newest version must win.
            assert(value == "latest");
        } else {
            assert(value == "v" + std::to_string(i));
        }
    }

    // The restored DB must keep allocating sequence numbers above everything it
    // already stored, otherwise new writes collide with recovered ones.
    const uint64_t restored_last_seq = static_cast<lightkv::DBImpl*>(backup)->LastSeq();
    assert(backup->Put(wo, "after_restore", "ok").ok());
    const uint64_t next_seq = static_cast<lightkv::DBImpl*>(backup)->LastSeq();
    if (next_seq <= restored_last_seq) {
        std::fprintf(stderr, "  restored DB reused sequence numbers (%llu -> %llu)\n",
                     (unsigned long long)restored_last_seq, (unsigned long long)next_seq);
    }
    assert(next_seq > restored_last_seq);

    delete backup;
    lightkv_remove_tree(db_path);
    lightkv_remove_tree(backup_path);
}

// A backup must not corrupt a pre-existing backup when it fails, and must not
// accept a target that would make it delete the live database.
void TestBackupGuards() {
    const std::string db_path = LIGHTKV_TEST_TMP "/lightkv_backup_guard_src";
    lightkv_remove_tree(db_path);

    lightkv::Options options;
    options.db_path = db_path;
    options.create_if_missing = true;

    lightkv::DB* db = nullptr;
    assert(lightkv::DB::Open(options, &db).ok());
    assert(db->Put(lightkv::WriteOptions(), "k", "v").ok());
    auto* impl = static_cast<lightkv::DBImpl*>(db);

    // Backing up onto itself would delete the database mid-copy.
    auto s = impl->Backup(db_path);
    if (s.ok()) {
        std::fprintf(stderr, "  Backup accepted db_path as its own target\n");
    }
    assert(!s.ok());
    {
        std::string value;
        assert(db->Get(lightkv::ReadOptions(), "k", &value).ok());
        assert(value == "v");
    }

    assert(!impl->Backup("").ok());

    // Re-running over an existing backup replaces it atomically.
    const std::string backup_path = LIGHTKV_TEST_TMP "/lightkv_backup_guard_dst";
    lightkv_remove_tree(backup_path);
    assert(impl->Backup(backup_path).ok());
    assert(db->Put(lightkv::WriteOptions(), "k2", "v2").ok());
    assert(impl->Backup(backup_path).ok());

    delete db;
    lightkv::Options restored = options;
    restored.db_path = backup_path;
    lightkv::DB* backup = nullptr;
    assert(lightkv::DB::Open(restored, &backup).ok());
    std::string value;
    assert(backup->Get(lightkv::ReadOptions(), "k2", &value).ok());
    assert(value == "v2");
    delete backup;

    lightkv_remove_tree(db_path);
    lightkv_remove_tree(backup_path);
}

// Clear() is what makes a Raft snapshot installable without swapping the DB
// object out from under Server. It must drop everything -- memtable, SSTables,
// vlog and sequence state -- and leave a database that still works.
void TestClearWipesEverything() {
    const std::string db_path = LIGHTKV_TEST_TMP "/lightkv_clear_test";
    lightkv_remove_tree(db_path);

    lightkv::Options options;
    options.db_path = db_path;
    options.create_if_missing = true;
    options.value_threshold = 256;  // push large values into the vlog

    lightkv::DB* db = nullptr;
    assert(lightkv::DB::Open(options, &db).ok());

    lightkv::WriteOptions wo;
    lightkv::ReadOptions ro;
    std::string value;

    // Enough data to force at least one flush into an SSTable, so the test also
    // covers the on-disk side rather than just the memtable.
    for (int round = 0; round < 4; ++round) {
        for (int i = 0; i < 100; ++i) {
            assert(db->Put(wo, "k" + std::to_string(i), std::string(1024, 'v')).ok());
        }
    }
    assert(db->Put(wo, "big", std::string(8192, 'B')).ok());
    assert(db->Put(wo, "small", "s").ok());
    assert(db->Delete(wo, "k0").ok());

    auto* impl = static_cast<lightkv::DBImpl*>(db);
    auto before_flushes = impl->GetStats().total_flushes;
    assert(db->Get(ro, "k5", &value).ok());
    assert(db->Get(ro, "big", &value).ok());

    lightkv::Status cs = db->Clear();
    if (!cs.ok()) {
        std::fprintf(stderr, "  Clear failed: %s\n", cs.ToString().c_str());
    }
    assert(cs.ok());

    // Nothing may remain.
    assert(!db->Exists(ro, "k5"));
    assert(!db->Exists(ro, "small"));
    assert(!db->Exists(ro, "big"));
    assert(!db->Exists(ro, "k0"));
    {
        std::vector<std::pair<std::string, std::string>> rows;
        assert(db->Scan(ro, "", 1000, &rows).ok());
        if (!rows.empty()) {
            std::fprintf(stderr, "  %zu keys survived Clear()\n", rows.size());
        }
        assert(rows.empty());
    }

    // Sequence space restarts, otherwise the first post-Clear write would reuse
    // sequence numbers that the dropped SSTables still contain.
    assert(impl->LastSeq() == 0);

    // And the database is fully usable afterwards.
    assert(db->Put(wo, "after_clear", "ok").ok());
    assert(db->Get(ro, "after_clear", &value).ok());
    assert(value == "ok");
    assert(db->Put(wo, "big2", std::string(8192, 'C')).ok());
    assert(db->Get(ro, "big2", &value).ok());
    assert(value == std::string(8192, 'C'));

    delete db;

    // Reopening must not resurrect anything from the WAL, and must see the
    // post-Clear writes.
    lightkv::DB* reopened = nullptr;
    assert(lightkv::DB::Open(options, &reopened).ok());
    assert(!reopened->Exists(ro, "k5"));
    assert(!reopened->Exists(ro, "big"));
    assert(reopened->Get(ro, "after_clear", &value).ok());
    assert(value == "ok");
    assert(reopened->Get(ro, "big2", &value).ok());
    assert(value == std::string(8192, 'C'));
    delete reopened;

    lightkv_remove_tree(db_path);
}

// Backpressure: a writer must never lose data or be rejected while L0 is over
// its budget. Threshold plumbing is exercised through a burst large enough to
// produce several L0 files.
void TestCompactionBackpressure() {
    lightkv::Options options;
    options.db_path = LIGHTKV_TEST_TMP "/lightkv_test_backpressure";
    options.l0_file_num_trigger = 2;           // compaction after 2 L0 files
    options.l0_slowdown_trigger = 2;
    options.l0_stop_trigger = 3;
    // Keep the bound low: the wait is a bounded poll, and a test that waits
    // seconds per write is just a slow test.
    options.compaction_backpressure_timeout_ms = 200;
    lightkv_remove_tree(options.db_path);

    lightkv::DB* db = nullptr;
    auto s = lightkv::DB::Open(options, &db);
    assert(s.ok());

    // Multiple reopen rounds: each teardown flushes the memtable into one L0
    // file, so the level genuinely grows past the thresholds while writes are
    // still arriving.
    const int kRounds = 6;
    const int kPerRound = 200;
    for (int round = 0; round < kRounds; ++round) {
        for (int i = 0; i < kPerRound; ++i) {
            const std::string key = "bp_" + std::to_string(round) + "_" +
                                    std::to_string(i);
            s = db->Put(lightkv::WriteOptions(), key, std::string(64, 'x'));
            if (!s.ok()) {
                std::fprintf(stderr, "  Put %s failed: %s\n", key.c_str(),
                             s.ToString().c_str());
            }
            assert(s.ok());
        }
    }

    // Nothing may be missing.
    lightkv::ReadOptions ro;
    std::string value;
    for (int round = 0; round < kRounds; ++round) {
        for (int i = 0; i < kPerRound; ++i) {
            const std::string key = "bp_" + std::to_string(round) + "_" +
                                    std::to_string(i);
            s = db->Get(ro, key, &value);
            if (!s.ok()) {
                std::fprintf(stderr, "  %s lost: %s\n", key.c_str(),
                             s.ToString().c_str());
            }
            assert(s.ok());
            assert(value == std::string(64, 'x'));
        }
    }

    delete db;
    lightkv_remove_tree(options.db_path);
    std::cout << "[Test] Compaction backpressure never costs a write" << std::endl;
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
    TestCompactionKeepsNewestVersion();
    TestSnapshotReadIsolation();
    TestDeleteRangeReportsCount();
    TestBackupIsRestorable();
    TestBackupGuards();
    TestClearWipesEverything();
    TestCompactionBackpressure();

    std::cout << "All DB tests passed!" << std::endl;
    return 0;
}
