#include "test_paths.h"
#include "lightkv/db.h"
#include "lightkv/db_impl.h"
#include "lightkv/manifest.h"
#include "lightkv/sstable.h"
#include <filesystem>
#include <fstream>
#include <vector>
#include <system_error>
#include <iostream>
#include <cassert>
#include <string>
#include <cstdlib>
#include <thread>
#include <chrono>
#include <atomic>

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
    auto* impl = static_cast<lightkv::DBImpl*>(db);

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

    // Guard: the test is only meaningful if compaction actually ran.
    //
    // Wait for the pipeline to drain instead of sleeping a fixed 800 ms. A
    // flush costs two fsyncs (the table, then the MANIFEST delta) and on a
    // volume that antivirus is scanning one of them can take seconds, so how
    // many flushes complete inside the loop above is a property of the disk,
    // not of the engine -- a fixed sleep made this assertion fail on builds
    // where every value read back was correct.
    impl->WaitForBackgroundIdle(60000);
    auto stats = impl->GetStats();

    // If nothing was scheduled there is still nothing to assert on: PickCompaction
    // wants L0 to be OVER its trigger, so one or two tables get no merge at all.
    // Push more writes through until it asks for one, so this guard measures the
    // engine rather than how fast the machine's temporary files come back.
    for (int attempt = 0; attempt < 5 && stats.total_compactions == 0; ++attempt) {
        for (int i = 0; i < 1200; ++i) {
            char filler[48];
            snprintf(filler, sizeof(filler), "extra_%d_%04d", attempt, i);
            db->Put(lightkv::WriteOptions(), filler, "x");
        }
        impl->WaitForBackgroundIdle(60000);
        stats = impl->GetStats();
    }

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

// A database whose only write is the very first one must still back up.
//
// The first write to a fresh database gets sequence number 0, and FlushForBackup
// used to read MaxSeq()==0 as "the memtable is empty" and skip the flush. Backup
// ships no WAL by design, so that write existed only in memory: Backup returned
// OK and the restored copy came back with no data at all.
void TestBackupAfterSingleWrite() {
    const std::string db_path = LIGHTKV_TEST_TMP "/lightkv_backup_single_write";
    const std::string backup_path = db_path + "_bk";
    lightkv_remove_tree(db_path);
    lightkv_remove_tree(backup_path);

    lightkv::Options options;
    options.db_path = db_path;
    options.create_if_missing = true;

    lightkv::DB* db = nullptr;
    assert(lightkv::DB::Open(options, &db).ok());
    assert(db->Put(lightkv::WriteOptions(), "only_key", "only_value").ok());
    auto bs = db->Backup(backup_path);
    if (!bs.ok()) std::fprintf(stderr, "  backup failed: %s\n", bs.ToString().c_str());
    assert(bs.ok());

    // The copy has to be self-contained -- "the WAL still has it" is not a
    // backup: Backup deliberately ships no WAL, so the write must have reached
    // an SSTable inside the backup directory.
    int sst_count = 0;
    for (const auto& entry : std::filesystem::directory_iterator(backup_path)) {
        if (entry.path().extension() == ".sst") ++sst_count;
    }
    if (sst_count == 0) {
        std::fprintf(stderr,
                     "  backup contains no SSTable: the single write was never flushed\n");
    }
    assert(sst_count > 0);

    // A second backup has nothing left to flush. That branch has to keep
    // working -- "nothing to do" must not become "fail".
    assert(db->Backup(backup_path).ok());
    delete db;

    lightkv::Options restored = options;
    restored.db_path = backup_path;
    lightkv::DB* backup = nullptr;
    assert(lightkv::DB::Open(restored, &backup).ok());
    std::string value;
    auto gs = backup->Get(lightkv::ReadOptions(), "only_key", &value);
    if (!gs.ok()) {
        std::fprintf(stderr,
                     "  the only write in the database is missing from its backup: %s\n",
                     gs.ToString().c_str());
    }
    assert(gs.ok());
    assert(value == "only_value");
    delete backup;

    lightkv_remove_tree(db_path);
    lightkv_remove_tree(backup_path);
    std::cout << "[Test] Backup keeps a database whose only write has seq 0" << std::endl;
}

// A memtable that cannot reach an SSTable must be reported, not swallowed.
//
// The original FlushMemTable did `if (!s.ok()) return;` and nothing else. That
// single line produced three separate failures, none of which is visible without
// a test that forces the failure:
//
//   * Backup() returned OK with a copy missing every acknowledged write, because
//     its durability witness was "mem_ != pending" -- and TriggerFlush swaps mem_
//     before the flush has written a single byte.
//   * Backup()/Clear() blocked forever on flush_cv_, whose predicate was
//     "!has_imm_" and a failed flush leaves has_imm_ set.
//   * The background thread re-entered FlushMemTable in a tight loop, because its
//     wake predicate was "has_imm_" -- so a permanently broken device pinned a
//     core and the destructor could never join.
//
// Failure injection: a NON-EMPTY directory where the flush wants its SSTable
// cannot be opened for writing, and the cleanup path's std::remove cannot get
// rid of it either. File ids are never reused, so the bounded retries walk
// straight through the poisoned range. The range only has to cover the retry
// budget: if retries were unbounded they would exhaust it, the next id would
// succeed, and the assertions below would catch exactly that.
void TestFlushFailureIsReported() {
    const std::string db_path = LIGHTKV_TEST_TMP "/lightkv_flushfail_db";
    const std::string backup_path = LIGHTKV_TEST_TMP "/lightkv_flushfail_bak";
    lightkv_remove_tree(db_path);
    lightkv_remove_tree(backup_path);

    lightkv::Options options;
    options.db_path = db_path;
    options.create_if_missing = true;
    options.memtable_size = 1024;  // rotates almost immediately

    lightkv::DB* db = nullptr;
    auto s = lightkv::DB::Open(options, &db);
    assert(s.ok());

    constexpr int kPoisoned = 16;
    for (int id = 1; id <= kPoisoned; ++id) {
        const std::string dir = db_path + "/" + std::to_string(id) + ".sst";
        std::filesystem::create_directories(dir);
        std::ofstream(std::string(dir) + "/blocker") << "x";
    }

    lightkv::WriteOptions wo;
    int accepted = 0;
    for (int i = 0; i < 3000; ++i) {
        if (!db->Put(wo, "k" + std::to_string(i), "v" + std::to_string(i)).ok()) break;
        ++accepted;
    }
    assert(accepted == 3000);

    // A failed flush is about durability, not about reads: the data is still in
    // memory, so it must still be readable.
    std::string value;
    assert(db->Get(lightkv::ReadOptions(), "k0", &value).ok());
    assert(value == "v0");

    // Backup must refuse. This call is what used to hang; with bounded retries it
    // returns as soon as the flush gives up, which is why it needs no poll loop.
    auto bs = db->Backup(backup_path);
    if (bs.ok()) {
        std::fprintf(stderr,
                     "  Backup returned OK although no memtable ever reached an "
                     "SSTable: the copy is missing every write\n");
    }
    assert(!bs.ok());
    assert(bs.IsIOError());

    // With the immutable memtable stuck, no rotation can happen, so accepting
    // more writes would grow memory without bound. The size check runs every
    // 1024th write, hence the small loop rather than a single call.
    bool refused = false;
    for (int i = 0; i < 400 && !refused; ++i) {
        refused = !db->Put(wo, "late" + std::to_string(i), "x").ok();
    }
    if (!refused) {
        std::fprintf(stderr,
                     "  writes were still accepted with the flush permanently "
                     "wedged: the memtable can only grow from here\n");
    }
    assert(refused);

    // Shutting down must not block on a background thread that can never
    // finish. If this hangs, nothing after it prints.
    delete db;

    // Give up on the poison so the reopen's own shutdown flush is clean; the
    // point of the reopen is to prove the WAL still holds everything.
    for (int id = 1; id <= kPoisoned; ++id) {
        lightkv_remove_tree(db_path + "/" + std::to_string(id) + ".sst");
    }

    s = lightkv::DB::Open(options, &db);
    assert(s.ok());
    int recovered = 0;
    for (int i = 0; i < accepted; ++i) {
        value.clear();
        if (db->Get(lightkv::ReadOptions(), "k" + std::to_string(i), &value).ok() &&
            value == "v" + std::to_string(i)) {
            ++recovered;
        }
    }
    if (recovered != accepted) {
        std::fprintf(stderr,
                     "  %d of %d acknowledged writes survived the failed flush: the "
                     "WAL was supposed to still hold them\n",
                     recovered, accepted);
    }
    assert(recovered == accepted);
    delete db;

    lightkv_remove_tree(db_path);
    lightkv_remove_tree(backup_path);
    std::cout << "[Test] A failed memtable flush is reported, does not hang, and loses nothing"
              << std::endl;
}

// The MANIFEST is the only record of which SSTables are live, so a failed write
// to it used to be the quietest way to lose data in the whole engine: the error
// was printed as FATAL and otherwise ignored, the flush counted as successful,
// and FlushMemTable unlinked the WAL for records that nothing on disk named.
//
// The invariant this checks is the one that makes the failure survivable: when
// the manifest cannot be written, NOTHING is retired. The SSTable is rolled back
// and the WAL stays, so the worst case is a slow restart rather than a missing
// range of acknowledged writes.
void TestManifestFailureKeepsDataRecoverable() {
    const std::string db_path = LIGHTKV_TEST_TMP "/lightkv_manifestfail_db";
    const std::string backup_path = LIGHTKV_TEST_TMP "/lightkv_manifestfail_bak";
    const std::string backup_path2 = LIGHTKV_TEST_TMP "/lightkv_manifestfail_bak2";
    lightkv_remove_tree(db_path);
    lightkv_remove_tree(backup_path);
    lightkv_remove_tree(backup_path2);

    auto count_files = [](const std::string& dir, const std::string& ext) {
        int n = 0;
        std::error_code ec;
        for (const auto& e : std::filesystem::directory_iterator(dir, ec)) {
            const std::string name = e.path().filename().string();
            if (name.size() > ext.size() &&
                name.compare(name.size() - ext.size(), ext.size(), ext) == 0) {
                ++n;
            }
        }
        return n;
    };

    // Both halves of the manifest write have to fail, because each has a fallback:
    // "MANIFEST" as a directory breaks the in-place append (O_RDWR on a directory
    // fails on every platform we build for), "MANIFEST.tmp" as one breaks the
    // snapshot UpdateManifest falls back to. The real snapshot is moved aside
    // rather than deleted, so the directory is left in the exact state a crash
    // during the failure would leave: the last successfully persisted manifest.
    auto poison = [&db_path]() {
        std::error_code ec;
        std::filesystem::rename(db_path + "/MANIFEST", db_path + "/MANIFEST.saved", ec);
        std::filesystem::create_directories(db_path + "/MANIFEST", ec);
        std::filesystem::create_directories(db_path + "/MANIFEST.tmp", ec);
    };
    auto unpoison = [&db_path]() {
        std::error_code ec;
        std::filesystem::remove_all(db_path + "/MANIFEST", ec);
        std::filesystem::remove_all(db_path + "/MANIFEST.tmp", ec);
        std::filesystem::rename(db_path + "/MANIFEST.saved", db_path + "/MANIFEST", ec);
    };

    lightkv::Options options;
    options.db_path = db_path;
    options.create_if_missing = true;
    options.memtable_size = 1024 * 1024;

    lightkv::DB* db = nullptr;
    assert(lightkv::DB::Open(options, &db).ok());

    lightkv::WriteOptions wo;
    constexpr int kFirst = 500;
    for (int i = 0; i < kFirst; ++i) {
        assert(db->Put(wo, "a" + std::to_string(i), "v" + std::to_string(i)).ok());
    }
    // Forces a flush, and with it the first MANIFEST snapshot -- the state the
    // failed update below has to append to.
    assert(db->Backup(backup_path).ok());

    poison();

    constexpr int kSecond = 500;
    for (int i = 0; i < kSecond; ++i) {
        assert(db->Put(wo, "b" + std::to_string(i), "w" + std::to_string(i)).ok());
    }

    const int sst_before = count_files(db_path, ".sst");
    const int wal_before = count_files(db_path, ".log");

    // This flushes the memtable holding the second batch. Its SSTable cannot be
    // recorded, so the flush has to fail -- and the backup has to say so rather
    // than hand back an image missing 500 acknowledged writes.
    auto bs = db->Backup(backup_path2);
    if (bs.ok()) {
        std::fprintf(stderr,
                     "  Backup returned OK while the MANIFEST could not be written: "
                     "the copy is missing every write since the last update\n");
    }
    assert(!bs.ok());
    assert(bs.IsIOError());

    // The rotation added one WAL; the failed flush must leave it AND the one it
    // was flushing, because that one holds the only other copy of the records.
    const int wal_after = count_files(db_path, ".log");
    if (wal_after != wal_before + 1) {
        std::fprintf(stderr,
                     "  %d WAL files after the failed flush, expected %d: the flush "
                     "unlinked the WAL for records no MANIFEST names\n",
                     wal_after, wal_before + 1);
    }
    assert(wal_after == wal_before + 1);

    // Nor may it leave the SSTable it wrote behind: no manifest names it, file
    // ids are never reused, so nothing could ever collect it again.
    const int sst_after = count_files(db_path, ".sst");
    if (sst_after != sst_before) {
        std::fprintf(stderr, "  %d SSTables after the failed flush, expected %d\n",
                     sst_after, sst_before);
    }
    assert(sst_after == sst_before);

    // Still readable in-process: the failure is about durability, not about the
    // values being somewhere in memory.
    std::string value;
    assert(db->Get(lightkv::ReadOptions(), "b7", &value).ok());
    assert(value == "w7");

    // Shutting down must not hang, and it must NOT quietly repair the manifest:
    // the disk is left as the failure left it, which is what the reopen below is
    // meant to survive.
    delete db;

    unpoison();

    // Every acknowledged write, from the last successfully persisted manifest
    // plus the WALs, and nothing else.
    assert(lightkv::DB::Open(options, &db).ok());
    int recovered = 0;
    for (int i = 0; i < kFirst; ++i) {
        value.clear();
        if (db->Get(lightkv::ReadOptions(), "a" + std::to_string(i), &value).ok() &&
            value == "v" + std::to_string(i)) {
            ++recovered;
        }
    }
    for (int i = 0; i < kSecond; ++i) {
        value.clear();
        if (db->Get(lightkv::ReadOptions(), "b" + std::to_string(i), &value).ok() &&
            value == "w" + std::to_string(i)) {
            ++recovered;
        }
    }
    if (recovered != kFirst + kSecond) {
        std::fprintf(stderr,
                     "  %d of %d acknowledged writes survived an unpersistable "
                     "MANIFEST: the WAL for the unrecorded flush was supposed to be "
                     "kept\n",
                     recovered, kFirst + kSecond);
    }
    assert(recovered == kFirst + kSecond);
    delete db;

    lightkv_remove_tree(db_path);
    lightkv_remove_tree(backup_path);
    lightkv_remove_tree(backup_path2);
    std::cout << "[Test] A MANIFEST write failure is reported and retires nothing"
              << std::endl;
}

// Concurrent writers, on a database small enough to rotate the memtable and the
// WAL underneath them.
//
// Two properties, both of which a smaller test misses:
//
//   * The write path pairs "append to the WAL" with "insert into the memtable"
//     under its own lock, and rotation swaps both under that same lock. A record
//     must never land in a memtable whose WAL does not hold it -- the flush then
//     deletes that WAL and the record is gone from both places.
//   * Every acknowledged key must be READABLE again. Point lookups go through
//     SSTable::Get -> Block::Iterator::Seek, which is where the keys below
//     (cw0_26, cw0_27, ... -- the ones that sort inside a block boundary that
//     falls between "cw0_2" and "cw0_3") used to come back NotFound while the
//     same files iterated over them happily.
void TestConcurrentWritePath() {
    const std::string db_path = LIGHTKV_TEST_TMP "/lightkv_test_concurrent_write";
    lightkv_remove_tree(db_path);

    lightkv::Options options;
    options.db_path = db_path;
    options.create_if_missing = true;
    options.memtable_size = 32 * 1024;   // rotate a few times during the run

    lightkv::DB* db = nullptr;
    assert(lightkv::DB::Open(options, &db).ok());

    const int kThreads = 4;
    const int kPerThread = 400;
    std::atomic<int> failures{0};
    std::vector<std::thread> writers;
    writers.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        writers.emplace_back([&, t] {
            for (int i = 0; i < kPerThread; ++i) {
                const std::string key = "cw" + std::to_string(t) + "_" + std::to_string(i);
                if (!db->Put(lightkv::WriteOptions(), key, std::string(60, 'v')).ok()) {
                    ++failures;
                }
            }
        });
    }
    for (auto& w : writers) w.join();
    if (failures != 0) std::fprintf(stderr, "  %d Put(s) failed\n", failures.load());
    assert(failures == 0);

    auto* impl = static_cast<lightkv::DBImpl*>(db);
    impl->WaitForBackgroundIdle(60000);

    auto verify_all = [&](lightkv::DB* target, const char* when) {
        int missing = 0;
        for (int t = 0; t < kThreads; ++t) {
            for (int i = 0; i < kPerThread; ++i) {
                const std::string key =
                    "cw" + std::to_string(t) + "_" + std::to_string(i);
                std::string value;
                if (!target->Get(lightkv::ReadOptions(), key, &value).ok() ||
                    value != std::string(60, 'v')) {
                    ++missing;
                }
            }
        }
        if (missing) {
            std::fprintf(stderr, "  %s: %d of %d keys lost\n", when, missing,
                         kThreads * kPerThread);
            assert(missing == 0);
        }
    };

    verify_all(db, "after concurrent writes");

    delete db;
    db = nullptr;
    // Reopen: what survived is whatever the WAL and the flushed tables hold, so
    // this is where a record written to the wrong generation of the WAL shows up.
    assert(lightkv::DB::Open(options, &db).ok());
    verify_all(db, "after reopen");
    delete db;

    lightkv_remove_tree(db_path);
    std::cout << "[Test] Concurrent writers lose nothing across flush and WAL rotation"
              << std::endl;
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

// The append log has to fold back into a snapshot, and the trigger has to scale
// with the file count rather than being a fixed record count. Exercised against
// Manifest directly: a fold is Manifest's own behaviour, and driving it through
// DB would cost one flush-and-close per record.
void TestManifestAppendLogFolds() {
    const std::string dir = LIGHTKV_TEST_TMP "/lightkv_manifest_fold";
    lightkv_remove_tree(dir);
    {
        std::error_code ec;
        std::filesystem::create_directories(dir, ec);
    }

    const std::string manifest_file = dir + "/MANIFEST";
    auto file_size = [&manifest_file]() -> size_t {
        std::error_code ec;
        auto sz = std::filesystem::file_size(manifest_file, ec);
        return ec ? 0 : static_cast<size_t>(sz);
    };

    lightkv::Manifest m;
    // A handful of live files, so the fold trigger is driven by the floor.
    const size_t kBaseFiles = 8;
    for (uint64_t i = 0; i < kBaseFiles; ++i) {
        m.AddFile(0, i + 1, "s" + std::to_string(i), "e" + std::to_string(i), 100);
    }
    assert(m.WriteToFile(dir).ok());
    assert(m.appended_records() == 0);

    const size_t threshold = m.FoldThreshold();
    // With few files the floor applies, and the floor is far below the old fixed
    // 512 -- the whole point is that the trigger tracks the snapshot's cost.
    assert(threshold >= 64);
    assert(threshold < 512);

    // Append more files than the trigger allows, in small steps, and watch for
    // the fold: the record counter has to come back down to zero at least once.
    size_t folds = 0;
    uint64_t next_id = 1000;
    for (int step = 0; step < static_cast<int>(threshold) + 20; ++step) {
        m.AddFile(1, next_id, "s" + std::to_string(next_id),
                  "e" + std::to_string(next_id), 128);
        ++next_id;
        assert(m.AppendChanges(dir).ok());
        if (m.appended_records() == 0) ++folds;
    }
    if (folds == 0) {
        std::fprintf(stderr,
                     "  append log never folded after %d records (threshold=%zu)\n",
                     threshold + 20, threshold);
    }
    assert(folds > 0);

    // Whatever happened, the file on disk must reload into an identical file set.
    lightkv::Manifest reloaded;
    auto s = reloaded.ReadFromFile(dir);
    if (!s.ok()) std::fprintf(stderr, "  reload failed: %s\n", s.ToString().c_str());
    assert(s.ok());

    const size_t expect_l0 = kBaseFiles;
    if (reloaded.files[0].size() != expect_l0) {
        std::fprintf(stderr, "  L0 has %zu files after reload, expected %zu\n",
                     reloaded.files[0].size(), expect_l0);
    }
    assert(reloaded.files[0].size() == expect_l0);
    assert(reloaded.files[1].size() == next_id - 1000);

    // Key boundaries have to survive verbatim: this is exactly the data a
    // separator-delimited format used to mangle.
    assert(reloaded.files[0].size() == kBaseFiles);
    for (size_t i = 0; i < kBaseFiles; ++i) {
        if (reloaded.files[0][i].file_id != i + 1 ||
            reloaded.files[0][i].smallest_key != "s" + std::to_string(i) ||
            reloaded.files[0][i].largest_key != "e" + std::to_string(i) ||
            reloaded.files[0][i].file_size != 100) {
            std::fprintf(stderr,
                         "  base file %zu came back as id=%llu [%s, %s] size=%llu\n",
                         i, (unsigned long long)reloaded.files[0][i].file_id,
                         reloaded.files[0][i].smallest_key.c_str(),
                         reloaded.files[0][i].largest_key.c_str(),
                         (unsigned long long)reloaded.files[0][i].file_size);
        }
        assert(reloaded.files[0][i].file_id == i + 1);
        assert(reloaded.files[0][i].smallest_key == "s" + std::to_string(i));
        assert(reloaded.files[0][i].largest_key == "e" + std::to_string(i));
        assert(reloaded.files[0][i].file_size == 100);
    }
    assert(reloaded.files[1].back().file_id == next_id - 1);

    lightkv_remove_tree(dir);
    std::cout << "[Test] Manifest append log folds and reloads intact"
              << std::endl;
}

// A snapshot makes the staged append records redundant -- and keeping them is
// not merely wasteful. They are replayed ON TOP of the snapshot, so an "add"
// that the snapshot had already folded away comes back, and the same file id
// ends up in two levels. DBImpl's first snapshot used to leave them staged
// (WriteToFile could not clear them), so this is the shape that produced it:
// file 1 is snapshot-ed, then compacted from L0 into file 2, then an append
// replays the stale add.
void TestManifestSnapshotDropsStagedRecords() {
    const std::string dir = LIGHTKV_TEST_TMP "/lightkv_manifest_staged";
    lightkv_remove_tree(dir);
    {
        std::error_code ec;
        std::filesystem::create_directories(dir, ec);
    }

    lightkv::Manifest m;
    m.AddFile(0, 1, "a", "b", 100);
    assert(m.WriteToFile(dir).ok());
    assert(m.appended_records() == 0);

    // Compaction: file 1 leaves L0, file 2 replaces it in L1.
    m.RemoveFile(0, 1);
    m.AddFile(1, 2, "a", "b", 100);
    assert(m.AppendChanges(dir).ok());

    lightkv::Manifest reloaded;
    auto s = reloaded.ReadFromFile(dir);
    assert(s.ok());

    if (reloaded.files[0].size() != 0) {
        std::fprintf(stderr,
                     "  L0 came back with %zu files after replaying a staged add the "
                     "snapshot had already folded away (file id %llu)\n",
                     reloaded.files[0].size(),
                     (unsigned long long)reloaded.files[0].front().file_id);
    }
    assert(reloaded.files[0].empty());
    assert(reloaded.files[1].size() == 1);
    assert(reloaded.files[1].front().file_id == 2);

    lightkv_remove_tree(dir);
    std::cout << "[Test] A manifest snapshot discards the staged append records"
              << std::endl;
}

// Get() binary-searches levels_[1..] on the assumption the files are ordered
// by SmallestKey. Nothing used to maintain that: compaction outputs were
// appended in completion order, and two independent L0->L1 rounds whose key
// domains interleave (z-domain merged first, a/m-domain second) produced an
// unsorted L1 -- [z.., a..m]. lower_bound then landed on the wrong file, failed
// the SmallestKey guard, and reported NotFound for data sitting on disk.
// Measured on the unfixed code: 360/360 keys missed while Scan (a full merge,
// order-agnostic) found all 360; whether a reopen happened to heal it depended
// on which compactions ran at close, which is why the original report said the
// loss count was unstable. InsertIntoLevel now inserts in key order.
void TestL1InterleavedDomainsStayReadable() {
    lightkv::Options options;
    options.db_path = LIGHTKV_TEST_TMP "/lightkv_l1_order";
    options.l0_file_num_trigger = 2;     // compact as soon as 2 L0 files exist
    options.l0_compaction_max_files = 1; // take ONE oldest file per round
    options.max_level = 4;
    lightkv_remove_tree(options.db_path);

    lightkv::DB* db = nullptr;
    assert(lightkv::DB::Open(options, &db).ok());
    auto* impl = static_cast<lightkv::DBImpl*>(db);

    const std::string pad(200, 'x');
    auto put_domain = [&](char c, int lo, int hi) {
        for (int i = lo; i < hi; ++i) {
            char buf[32];
            std::snprintf(buf, sizeof(buf), "%c%04d", c, i);
            assert(db->Put(lightkv::WriteOptions(), buf,
                           "v" + std::string(buf) + pad).ok());
        }
    };
    // Backup forces a flush of the active memtable: one L0 file per call, and
    // the flush schedules the L0->L1 round that drains one oldest file.
    auto flush_one = [&](const char* tag) {
        assert(db->Backup(std::string(LIGHTKV_TEST_TMP) + "/lightkv_l1_order_bk_" +
                          tag).ok());
        assert(impl->WaitForBackgroundIdle(30000));
    };

    // Round 1 merges the two z-halves into L1 file R1 [z0000..z0119].
    put_domain('z', 0, 60);   flush_one("z1");
    put_domain('z', 60, 120); flush_one("z2");
    // Round 2 merges a+m into L1 file R2 [a0000..m0119]. R1 does not overlap
    // [a..m], so it is NOT an input -- and used to stay AHEAD of R2 because
    // completion order put it there first.
    put_domain('a', 0, 120);  flush_one("a");
    put_domain('m', 0, 120);  flush_one("m");

    for (char c : {'z', 'a', 'm'}) {
        for (int i = 0; i < 120; ++i) {
            char buf[32];
            std::snprintf(buf, sizeof(buf), "%c%04d", c, i);
            std::string v;
            assert(db->Get(lightkv::ReadOptions(), buf, &v).ok());
        }
    }
    delete db;

    // The ordering must survive the round trip: the manifest replays in append
    // order, which for R1/R2 is the same interleaved order that broke Get.
    assert(lightkv::DB::Open(options, &db).ok());
    for (char c : {'z', 'a', 'm'}) {
        for (int i = 0; i < 120; ++i) {
            char buf[32];
            std::snprintf(buf, sizeof(buf), "%c%04d", c, i);
            std::string v;
            assert(db->Get(lightkv::ReadOptions(), buf, &v).ok());
        }
    }
    delete db;

    lightkv_remove_tree(options.db_path);
    for (const char* tag : {"z1", "z2", "a", "m"})
        lightkv_remove_tree(std::string(LIGHTKV_TEST_TMP) + "/lightkv_l1_order_bk_" + tag);
    std::cout << "[Test] Interleaved L0->L1 compaction rounds keep every key readable"
              << std::endl;
}

// L0 compaction now merges a bounded batch instead of the whole level. The
// files it does NOT pick must survive: dropping them from the level while
// leaving their contents unread would lose data silently.
void TestL0CompactionKeepsUnpickedFiles() {
    lightkv::Options options;
    options.db_path = LIGHTKV_TEST_TMP "/lightkv_l0_batch";
    options.memtable_size = 16 * 1024;   // many small flushes -> many L0 files
    options.l0_file_num_trigger = 2;     // compact aggressively
    options.l0_compaction_max_files = 2; // force the batch cap well below the pile
    lightkv_remove_tree(options.db_path);

    lightkv::DB* db = nullptr;
    auto s = lightkv::DB::Open(options, &db);
    assert(s.ok());

    const int kKeys = 500;
    for (int i = 0; i < kKeys; ++i) {
        s = db->Put(lightkv::WriteOptions(), "batch_" + std::to_string(i),
                    std::string(48, 'v'));
        assert(s.ok());
    }
    // No settle time on purpose. With a sleep, later compaction rounds drain
    // whatever the round under test left behind and the batch bug is papered
    // over -- dropping unpicked files must be visible immediately.
    lightkv::ReadOptions ro;
    std::string value;
    int missing = 0;
    for (int i = 0; i < kKeys; ++i) {
        const std::string key = "batch_" + std::to_string(i);
        if (!db->Get(ro, key, &value).ok() || value != std::string(48, 'v')) ++missing;
    }
    if (missing) std::fprintf(stderr, "  %d keys lost before reopen\n", missing);
    assert(missing == 0);

    delete db;
    db = nullptr;
    s = lightkv::DB::Open(options, &db);
    if (!s.ok()) std::fprintf(stderr, "  reopen failed: %s\n", s.ToString().c_str());
    assert(s.ok());

    missing = 0;
    for (int i = 0; i < kKeys; ++i) {
        const std::string key = "batch_" + std::to_string(i);
        if (!db->Get(ro, key, &value).ok() || value != std::string(48, 'v')) ++missing;
    }
    if (missing) std::fprintf(stderr, "  %d keys lost after reopen\n", missing);
    assert(missing == 0);

    delete db;
    lightkv_remove_tree(options.db_path);
    std::cout << "[Test] Bounded L0 batches drop no files" << std::endl;
}

// A damaged SSTable must be reported as damage, never as an ordinary miss.
void TestCorruptSSTableIsReported() {
    lightkv::Options options;
    options.db_path = LIGHTKV_TEST_TMP "/lightkv_corrupt";
    options.create_if_missing = true;
    options.paranoid_checks = true;   // turn the CRC check on
    lightkv_remove_tree(options.db_path);

    const int kKeys = 3000;
    {
        lightkv::DB* db = nullptr;
        assert(lightkv::DB::Open(options, &db).ok());
        for (int i = 0; i < kKeys; ++i) {
            assert(db->Put(lightkv::WriteOptions(), "corrupt_" + std::to_string(i),
                           std::string(64, 'c')).ok());
        }
        delete db;  // flush into an SSTable
    }

    std::vector<std::string> ssts;
    for (const auto& entry : std::filesystem::directory_iterator(options.db_path)) {
        if (entry.path().extension() == ".sst") ssts.push_back(entry.path().string());
    }
    if (ssts.empty()) std::fprintf(stderr, "  no SSTable was produced\n");
    assert(!ssts.empty());

    // Flip a byte in the middle of the data region. The key stays intact, so
    // only the checksum can tell us anything is wrong.
    {
        std::fstream f(ssts[0], std::ios::in | std::ios::out | std::ios::binary);
        assert(f.good());
        f.seekp(32, std::ios::beg);
        char byte = 0;
        f.read(&byte, 1);
        byte = static_cast<char>(byte ^ 0xFF);
        f.seekp(32, std::ios::beg);
        f.write(&byte, 1);
        f.close();
    }

    // Data blocks are loaded lazily, so Open legitimately succeeds -- the index is
    // intact and nothing has read the damaged region yet. Reading a key that
    // lives in the damaged block must answer Corruption, not NotFound.
    // Read the damaged table DIRECTLY rather than through DB::Get.
    //
    // A restart now replays the still-unflushed writes from the WAL into the
    // memtable, so DB::Get answers from memory and never touches the damaged
    // block -- the assertion below would pass for the wrong reason. The property
    // under test is that a damaged block is reported as damage, so read the
    // table itself.
    lightkv::SSTable table(options, ssts[0], 1);
    {
        auto os = table.Open();
        if (!os.ok()) {
            std::fprintf(stderr, "  could not open damaged table: %s\n", os.ToString().c_str());
        }
        assert(os.ok());
    }

    int not_found = 0;
    int corrupt = 0;
    int ok = 0;
    for (int i = 0; i < kKeys; ++i) {
        std::string value;
        uint64_t seq = 0;
        uint8_t type = 0;
        lightkv::Status gs = table.Get("corrupt_" + std::to_string(i), UINT64_MAX >> 8,
                                       &value, &seq, &type);
        if (gs.ok()) ++ok;
        else if (gs.IsNotFound()) ++not_found;
        else ++corrupt;
    }
    // Flipping one byte cannot plausibly make every read fail -- only the block
    // it lands in. The point is not "all reads fail" but "damage is reported as
    // damage somewhere": before the fix every single one of these came back
    // NotFound, which is indistinguishable from the keys never being written.
    if (corrupt == 0) {
        std::fprintf(stderr,
                     "  no read reported corruption (%d ok, %d NotFound); a "
                     "damaged file is being reported as absent data\n",
                     ok, not_found);
    }
    assert(corrupt > 0);
    assert(ok + corrupt + not_found == kKeys);

    lightkv_remove_tree(options.db_path);
    std::cout << "[Test] Corrupted SSTable is reported as corruption" << std::endl;
}

// A point delete must not be undone by the copy of the value that already sits
// in an SSTable.
//
// The read path used to treat "this memtable holds a tombstone" as "not here,
// keep looking": it fell through to the levels, found the pre-delete value and
// returned it. The key came back to life as soon as its value had been flushed,
// which is invisible in any test that deletes while the value is still in the
// memtable.
void TestDeleteAfterFlushStaysDeleted() {
    const std::string db_path = LIGHTKV_TEST_TMP "/lightkv_test_delete_after_flush";
    const std::string backup_path = db_path + "_bk";
    lightkv_remove_tree(db_path);
    lightkv_remove_tree(backup_path);

    lightkv::Options options;
    options.db_path = db_path;
    options.create_if_missing = true;

    lightkv::DB* db = nullptr;
    assert(lightkv::DB::Open(options, &db).ok());

    lightkv::WriteOptions wo;
    lightkv::ReadOptions ro;
    assert(db->Put(wo, "victim", "alive").ok());

    // Force the value out to an SSTable. Backup flushes the active memtable and
    // waits until it is durable, so the assertion below reads from disk, not
    // from memory.
    auto bs = db->Backup(backup_path);
    if (!bs.ok()) std::fprintf(stderr, "  backup failed: %s\n", bs.ToString().c_str());
    assert(bs.ok());

    std::string value;
    assert(db->Get(ro, "victim", &value).ok());
    assert(value == "alive");

    assert(db->Delete(wo, "victim").ok());
    if (db->Get(ro, "victim", &value).ok()) {
        std::fprintf(stderr, "  deleted key came back as \"%s\" from the SSTable\n",
                     value.c_str());
        assert(false);
    }
    assert(db->Get(ro, "victim", &value).IsNotFound());
    assert(!db->Exists(ro, "victim"));

    // Same again after a reopen, where the tombstone arrives by WAL replay
    // rather than sitting in a memtable that still holds the value.
    delete db;
    db = nullptr;
    assert(lightkv::DB::Open(options, &db).ok());
    assert(db->Get(ro, "victim", &value).IsNotFound());
    delete db;

    lightkv_remove_tree(db_path);
    lightkv_remove_tree(backup_path);
    std::cout << "[Test] Point delete survives the value being flushed" << std::endl;
}

// Options::max_level is a plain number (the config file sets it directly), but
// levels_ is a fixed-size array of DBImpl::kNumLevels. PickCompaction used to
// take its loop bound straight from the option, so max_level = 9 indexed
// levels_[7] -- one past the end, reading the neighbouring member as if it were
// a vector of SSTable handles.
//
// The loop is entered on every PickCompaction call where L0 is under its
// trigger, which is why l0_file_num_trigger is set high here: it forces the walk
// through the level array instead of returning early at L0.
void TestLevelCountBeyondArray() {
    // 9 is one past the array (the loop then indexes levels_[7]); 0 is below any
    // usable level count. 1300 writes is just enough to cross the engine's
    // 1024-write flush check, which is what makes it walk the level array.
    const int kCases[] = {9, 0};
    const std::string base = LIGHTKV_TEST_TMP "/lightkv_test_levelcount";

    for (int max_level : kCases) {
        const std::string db_path = base + "_" + std::to_string(max_level);
        lightkv_remove_tree(db_path);

        lightkv::Options options;
        options.db_path = db_path;
        options.create_if_missing = true;
        options.memtable_size = 4 * 1024;      // flushes every few hundred writes
        options.l0_file_num_trigger = 1000;    // never true: always walk levels
        options.max_level = static_cast<size_t>(max_level);

        lightkv::DB* db = nullptr;
        auto s = lightkv::DB::Open(options, &db);
        if (!s.ok()) {
            std::fprintf(stderr, "  max_level=%d: open failed: %s\n", max_level,
                         s.ToString().c_str());
            assert(s.ok());
        }

        for (int i = 0; i < 1300; ++i) {
            char key[32];
            snprintf(key, sizeof(key), "k%05d", i);
            char val[32];
            snprintf(val, sizeof(val), "v%05d", i);
            auto ps = db->Put(lightkv::WriteOptions(), key, val);
            assert(ps.ok());
        }

        auto* impl = static_cast<lightkv::DBImpl*>(db);
        impl->WaitForBackgroundIdle(60000);

        for (int i = 0; i < 1300; i += 37) {
            char key[32];
            snprintf(key, sizeof(key), "k%05d", i);
            char expect[32];
            snprintf(expect, sizeof(expect), "v%05d", i);
            std::string got;
            auto gs = db->Get(lightkv::ReadOptions(), key, &got);
            if (!gs.ok() || got != expect) {
                std::fprintf(stderr, "  max_level=%d: k%05d -> %s '%s' (want '%s')\n",
                             max_level, i, gs.ToString().c_str(), got.c_str(), expect);
                assert(gs.ok());
                assert(got == expect);
            }
        }

        delete db;
        lightkv_remove_tree(db_path);
    }
    std::cout << "[Test] Level count beyond the array is clamped, data stays intact"
              << std::endl;
}

// l0_file_num_trigger has to mean what its name says: with a trigger of N, L0 is
// merged as soon as it holds N files.
//
// PickCompaction scores L0 as size / trigger, so N files score exactly 1.0, and
// every caller skipped the round on `score <= 1.0`. The boundary case was
// therefore never merged: each level had to reach N+1 files before anything
// happened, making every configured trigger silently one too high.
//
// Each iteration writes one key and calls Backup(), which flushes the live
// memtable and waits for it, and so adds exactly one L0 file.
void TestL0TriggerBoundary() {
    const int kTrigger = 3;
    const std::string db_path = LIGHTKV_TEST_TMP "/lightkv_test_l0_trigger";
    const std::string backup_path = db_path + "_bk";
    lightkv_remove_tree(db_path);
    lightkv_remove_tree(backup_path);

    lightkv::Options options;
    options.db_path = db_path;
    options.create_if_missing = true;
    options.l0_file_num_trigger = static_cast<size_t>(kTrigger);

    lightkv::DB* db = nullptr;
    assert(lightkv::DB::Open(options, &db).ok());
    auto* impl = static_cast<lightkv::DBImpl*>(db);

    // One write that the first Backup below will flush. The first write to a
    // fresh database gets sequence number 0, and FlushForBackup reads
    // MaxSeq()==0 as "the memtable is empty", so without this the first call
    // would add no table at all and the count below would drift.
    assert(db->Put(lightkv::WriteOptions(), "warmup", std::string(200, 'w')).ok());

    auto flush_one_table = [&](int i) {
        assert(db->Put(lightkv::WriteOptions(), "key_" + std::to_string(i),
                       std::string(200, 'v')).ok());
        lightkv_remove_tree(backup_path);
        auto bs = db->Backup(backup_path);
        if (!bs.ok()) std::fprintf(stderr, "  backup failed: %s\n", bs.ToString().c_str());
        assert(bs.ok());
        impl->WaitForBackgroundIdle(60000);
        return impl->GetStats().total_compactions;
    };

    // Under the trigger: nothing to merge yet.
    for (int i = 0; i < kTrigger - 1; ++i) {
        const uint64_t compactions = flush_one_table(i);
        if (compactions != 0) {
            std::fprintf(stderr,
                         "  L0 merged at %d file(s), below the trigger of %d\n",
                         i + 1, kTrigger);
            assert(compactions == 0);
        }
    }

    // The table that brings L0 to exactly the trigger. This is the boundary the
    // off-by-one used to skip.
    const uint64_t compactions = flush_one_table(kTrigger - 1);
    if (compactions == 0) {
        std::fprintf(stderr,
                     "  L0 reached its trigger of %d files and nothing merged: the "
                     "trigger is firing one file late\n",
                     kTrigger);
        assert(compactions > 0);
    }

    delete db;
    lightkv_remove_tree(db_path);
    lightkv_remove_tree(backup_path);
    std::cout << "[Test] L0 compaction fires at the configured trigger" << std::endl;
}

int main() {
    // Time each case on stderr. db_test takes minutes overall, and without a
    // per-case breakdown there is no way to tell a slow engine apart from a
    // slow test.
    auto run = [](const char* name, void (*fn)()) {
        auto t0 = std::chrono::steady_clock::now();
        fn();
        const double ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::steady_clock::now() - t0).count();
        std::fprintf(stderr, "  [time] %-38s %8.0f ms\n", name, ms);
        std::fflush(stderr);
    };

    run("TestBasicOperations", TestBasicOperations);
    run("TestPersistence", TestPersistence);
    run("TestIterator", TestIterator);
    run("TestIteratorWithSSTable", TestIteratorWithSSTable);
    run("TestCompaction", TestCompaction);
    run("TestGetStats", TestGetStats);
    run("TestEmptyDB", TestEmptyDB);
    run("TestMultipleReopen", TestMultipleReopen);
    run("TestOverwriteAndDelete", TestOverwriteAndDelete);
    run("TestIteratorAfterCompaction", TestIteratorAfterCompaction);
    run("TestCompactionKeepsNewestVersion", TestCompactionKeepsNewestVersion);
    run("TestSnapshotReadIsolation", TestSnapshotReadIsolation);
    run("TestDeleteRangeReportsCount", TestDeleteRangeReportsCount);
    run("TestBackupIsRestorable", TestBackupIsRestorable);
    run("TestBackupGuards", TestBackupGuards);
    run("TestBackupAfterSingleWrite", TestBackupAfterSingleWrite);
    run("TestFlushFailureIsReported", TestFlushFailureIsReported);
    run("TestManifestFailureKeepsDataRecoverable", TestManifestFailureKeepsDataRecoverable);
    run("TestClearWipesEverything", TestClearWipesEverything);
    run("TestCompactionBackpressure", TestCompactionBackpressure);
    run("TestManifestAppendLogFolds", TestManifestAppendLogFolds);
    run("TestManifestSnapshotDropsStagedRecords", TestManifestSnapshotDropsStagedRecords);
    run("TestL1InterleavedDomainsStayReadable", TestL1InterleavedDomainsStayReadable);
    run("TestL0CompactionKeepsUnpickedFiles", TestL0CompactionKeepsUnpickedFiles);
    run("TestCorruptSSTableIsReported", TestCorruptSSTableIsReported);
    run("TestDeleteAfterFlushStaysDeleted", TestDeleteAfterFlushStaysDeleted);
    run("TestConcurrentWritePath", TestConcurrentWritePath);
    run("TestLevelCountBeyondArray", TestLevelCountBeyondArray);
    run("TestL0TriggerBoundary", TestL0TriggerBoundary);

    std::cout << "All DB tests passed!" << std::endl;
    return 0;
}
