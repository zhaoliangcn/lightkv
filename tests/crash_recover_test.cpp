// A crash must not hide the writes that landed after the last flush.
//
// Initialize() replays every surviving WAL and raises last_seq_ to cover each
// record it read. Then it loads MANIFEST and used to store manifest_.last_seq
// straight over that value. The manifest is only rewritten by a flush or a
// compaction, so everything written since the last flush exists solely in the
// WAL -- and the store put last_seq_ back below those records. Gets filter on
// `seq <= last_seq_`, so after a crash every write since the last flush read back
// as NotFound while sitting in the log. Measured before the fix: 5000 writes, a
// flush, 5000 more, crash, reopen -> 4999 of the second batch gone.
//
// A crash cannot be simulated from inside the process that is also asserting, so
// this binary re-executes itself with the "crash" argument: the child writes,
// forces a flush, writes again and calls _Exit() without running any destructor
// (a clean shutdown rewrites the manifest and would hide the bug entirely). The
// parent then reopens the directory and checks every key.
#include "test_paths.h"
#include "lightkv/db.h"
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <string>

namespace {

constexpr int kBeforeFlush = 5000;
constexpr int kAfterFlush = 5000;
const std::string kDbPath = LIGHTKV_TEST_TMP "/lightkv_crash_recover_db";
const std::string kBackupPath = LIGHTKV_TEST_TMP "/lightkv_crash_recover_bak";

lightkv::Options MakeOptions() {
    lightkv::Options options;
    options.db_path = kDbPath;
    options.create_if_missing = true;
    options.memtable_size = 64 * 1024;
    return options;
}

// Child: build the state a crash would leave behind, then die without cleanup.
int CrashPhase() {
    lightkv_remove_tree(kDbPath);
    lightkv_remove_tree(kBackupPath);

    lightkv::DB* db = nullptr;
    auto s = lightkv::DB::Open(MakeOptions(), &db);
    if (!s.ok()) {
        std::fprintf(stderr, "  open failed: %s\n", s.ToString().c_str());
        return 1;
    }

    lightkv::WriteOptions wo;
    for (int i = 0; i < kBeforeFlush; ++i) {
        db->Put(wo, "k" + std::to_string(i), "v" + std::to_string(i));
    }

    // Backup is the cheapest way to force a flush, and a successful flush is what
    // writes a MANIFEST carrying last_seq. Without a manifest there is nothing to
    // clobber, which is why this bug needs a flush first to show up.
    auto bs = db->Backup(kBackupPath);
    if (!bs.ok()) {
        std::fprintf(stderr, "  backup failed: %s\n", bs.ToString().c_str());
        return 1;
    }
    if (!std::filesystem::exists(kDbPath + "/MANIFEST")) {
        std::fprintf(stderr, "  no MANIFEST after a flush: this test would not exercise"
                             " the recovery path\n");
        return 1;
    }

    for (int i = kBeforeFlush; i < kBeforeFlush + kAfterFlush; ++i) {
        db->Put(wo, "k" + std::to_string(i), "v" + std::to_string(i));
    }

    std::fflush(nullptr);
    // Deliberately no delete db: a clean shutdown rewrites the manifest with the
    // current last_seq and the bug becomes invisible.
    std::_Exit(0);
}

}  // namespace

int main(int argc, char** argv) {
    std::setvbuf(stdout, nullptr, _IONBF, 0);

    if (argc > 1 && std::string(argv[1]) == "crash") return CrashPhase();

    // Re-run ourselves as the crashing writer.
    const std::string cmd = std::string("\"") + argv[0] + "\" crash";
    const int rc = std::system(cmd.c_str());
    if (rc != 0) {
        std::fprintf(stderr, "  crash phase exited with %d\n", rc);
    }
    assert(rc == 0);

    lightkv::DB* db = nullptr;
    auto s = lightkv::DB::Open(MakeOptions(), &db);
    if (!s.ok()) std::fprintf(stderr, "  reopen failed: %s\n", s.ToString().c_str());
    assert(s.ok());

    int recovered = 0;
    int missing_after_flush = 0;
    std::string value;
    for (int i = 0; i < kBeforeFlush + kAfterFlush; ++i) {
        value.clear();
        const bool ok = db->Get(lightkv::ReadOptions(), "k" + std::to_string(i), &value).ok();
        if (ok && value == "v" + std::to_string(i)) {
            ++recovered;
        } else if (i >= kBeforeFlush) {
            ++missing_after_flush;
        }
    }

    if (recovered != kBeforeFlush + kAfterFlush) {
        std::fprintf(stderr,
                     "  %d of %d acknowledged writes survived the crash; %d of the %d "
                     "written after the last flush were lost even though the WAL held them\n",
                     recovered, kBeforeFlush + kAfterFlush,
                     missing_after_flush, kAfterFlush);
    }
    assert(recovered == kBeforeFlush + kAfterFlush);

    delete db;
    lightkv_remove_tree(kDbPath);
    lightkv_remove_tree(kBackupPath);
    std::cout << "[Test] Writes made after the last flush survive a crash" << std::endl;
    return 0;
}
