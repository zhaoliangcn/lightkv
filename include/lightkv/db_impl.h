#pragma once

#include "db.h"
#include "memtable.h"
#include "wal.h"
#include "sstable.h"
#include "cache.h"
#include "compaction.h"
#include "manifest.h"
#include "options.h"
#include "slice.h"
#include "status.h"
#include "vlog.h"
#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <vector>

namespace lightkv {

class MergingIterator;  // forward decl, defined in iterator.cpp

struct DBStats {
    uint64_t total_writes = 0;
    uint64_t total_reads = 0;
    uint64_t total_deletes = 0;
    uint64_t total_flushes = 0;
    uint64_t total_compactions = 0;
    uint64_t memtable_size = 0;
    uint64_t imm_size = 0;
    uint64_t level_sizes[kNumLevels] = {0};
    uint64_t pending_deletes = 0;
};

class DBImpl : public DB {
public:
    // Number of levels the version set keeps: 0..kNumLevels-1.
    static constexpr int kNumLevels = ::lightkv::kNumLevels;

    DBImpl(const Options& options);
    ~DBImpl() override;

    Status Put(const WriteOptions& options, const Slice& key, const Slice& value) override;
    Status WriteImpl(const WriteOptions& options, const Slice& key,
                     const Slice& value);

    Status Delete(const WriteOptions& options, const Slice& key) override;

    Status DeleteRange(const WriteOptions& options, const Slice& begin_key,
                       const Slice& end_key, int64_t* deleted = nullptr) override;

    // v2.0: 原子批量写入 — 单次 WAL 提交，要么全部可见要么全部丢弃
    Status BatchWrite(const WriteOptions& options, const std::vector<WALRecord::BatchOp>& ops) override;

    Status Get(const ReadOptions& options, const Slice& key, std::string* value) override;

    bool Exists(const ReadOptions& options, const Slice& key) override;

    Status Scan(const ReadOptions& options, const Slice& prefix, int limit,
                std::vector<std::pair<std::string, std::string>>* results) override;

    Status Increment(const WriteOptions& options, const Slice& key, int64_t delta, int64_t* new_value) override;

    Status Rename(const WriteOptions& options, const Slice& src, const Slice& dst) override;

    // Produce a self-contained, restorable copy of the database at
    // backup_path. Everything committed before the call is included; writes
    // that land while it runs are not. On success the directory can be handed
    // straight to DB::Open with the same Options::db_path replaced.
    Status Backup(const std::string& backup_path) override;

    Status Clear() override;

    // The directory this database was opened from. Used by the HTTP /backup
    // endpoint to pick a default destination next to the live data.
    const std::string& db_path() const { return options_.db_path; }

    DBStats GetStats() const;

    // Blocks until the background thread has no queued work: no memtable
    // waiting to be flushed and no compaction round scheduled or running.
    // Returns false if that did not happen within timeout_ms.
    //
    // This exists because every other way of waiting is a guess about disk
    // speed. A flush costs two fsyncs (the table, then the MANIFEST delta), and
    // on a volume being scanned by antivirus a single one of those can take
    // seconds -- so "sleep 80 ms and assume the round is flushed" silently
    // becomes "nothing was flushed" and any assertion built on the number of
    // completed flushes turns into a coin flip. Waiting for the actual state
    // makes callers independent of that.
    bool WaitForBackgroundIdle(int timeout_ms = 10000);

    // v2.0: 暴露 last_seq_ 供 Watch revision 使用（详见设计草案 6）
    uint64_t LastSeq() const { return last_seq_.load(std::memory_order_acquire); }

    class Iterator {
    public:
        Iterator(const DBImpl* db, uint64_t snapshot_seq);
        ~Iterator();

        bool Valid() const;
        void SeekToFirst();
        void Seek(const Slice& target);
        void Next();
        Slice key() const;
        Slice value() const;
        void UpdateCurrent();

    private:
        const DBImpl* db_;
        uint64_t snapshot_seq_;
        bool valid_;
        std::string key_;
        std::string value_;
        std::unique_ptr<MergingIterator> merger_;
    };

private:
    friend class DB;

    // Push the current memtable out to an SSTable and block until the
    // background thread has written it. Used by Backup so the copy does not
    // have to replay a WAL.
    Status FlushForBackup();

    // Highest seq known to be durable in an SSTable. Mirrors
    // manifest_.flushed_seq but is readable without taking rw_mutex_.
    std::atomic<uint64_t> flushed_seq_{0};

    Status Initialize();

    void TriggerFlush();
    // Pause the calling writer while L0 is fuller than compaction can drain.
    Status WaitForCompactionCatchUp();

    void FlushMemTable();

    // Records a failed WriteLevel0Table attempt. Wakes the flush_cv_ waiters so
    // they report the failure instead of blocking on a predicate that will never
    // become true, and stops retrying once kMaxFlushAttempts is spent.
    void OnFlushFailed(const Status& s);

    // True when an immutable memtable is waiting for a flush we are still
    // willing to attempt. Caller holds mutex_. Deliberately NOT just has_imm_:
    // a flush that has exhausted its retries leaves has_imm_ set (the data is
    // still only in memory) but must not keep the background thread awake.
    bool FlushPendingLocked() const {
        return has_imm_ && flush_attempts_ < kMaxFlushAttempts;
    }

    // True when a flush_cv_ waiter has nothing left to wait for: either the
    // immutable memtable landed, or the flush gave up. Caller holds mutex_.
    bool FlushSettledLocked() const { return !has_imm_ || !FlushPendingLocked(); }

    // How many times one memtable may fail to reach an SSTable before the flush
    // is declared broken. Bounded on purpose: retrying forever is what turned a
    // single bad write into a hot spin plus a permanently blocked Backup/Clear.
    static constexpr int kMaxFlushAttempts = 3;

    void MaybeScheduleCompaction();

    // Takes rw_mutex_ shared internally: levels_ is only safe to read under it,
    // and callers reach here while holding mutex_, which must never be held
    // across an rw_mutex_ acquisition. Do NOT call this with rw_mutex_ held.
    CompactionScore PickCompaction();

    // Options::max_level clamped into [2, kNumLevels]. levels_ is a fixed-size
    // array and max_level is user-supplied, so every loop that walks levels_
    // derives its bound from this rather than from the raw option. Initialize()
    // clamps the option itself as well; this is the backstop for the code paths
    // that index the array.
    int EffectiveMaxLevel() const;

    void DoLevel0Compaction();

    void DoLevelCompaction(int level);

    void BackgroundCompaction();

    void BackgroundWork();

    void CleanupDeletedFiles() const;

    Status WriteLevel0Table(std::shared_ptr<MemTable> mem, uint64_t* file_id);

    Status SearchSSTable(int level, const Slice& key, std::string* value, uint64_t snapshot_seq) const;

    Status CheckDiskSpace() const;

    void UpdateManifest();

    // Snapshot management: tracks active snapshots for compaction safety
    mutable std::mutex snapshot_mutex_;
    std::vector<uint64_t> active_snapshots_;  // sorted list of active snapshot seq numbers
    uint64_t GetOldestSnapshot() const;

    Options options_;
    std::atomic<uint64_t> last_seq_;

    // The two live memtables.
    //
    // They are atomics, not plain shared_ptr, because readers pick them up under
    // rw_mutex_ (shared) while TriggerFlush / FlushMemTable swap them under
    // mutex_ -- two unrelated locks. A plain shared_ptr assignment is not atomic:
    // a reader could observe a half-written control block and follow a stale
    // pointer, and nothing would report it. Loading pins the MemTable, so a
    // flush that clears imm_ cannot destroy a memtable an iterator is walking.
    std::atomic<std::shared_ptr<MemTable>> mem_;
    std::atomic<std::shared_ptr<MemTable>> imm_;
    std::unique_ptr<WALWriter> wal_;
    std::string wal_path_;
    // The WAL that belongs to imm_. Rotated at memtable switch and deleted once
    // the flush owning it is durable. One WAL per memtable is what keeps a flush
    // from truncating records of writes that are not on disk yet.
    std::unique_ptr<WALWriter> imm_wal_;
    std::string imm_wal_path_;
    uint64_t next_wal_id_ = 0;
    // Read by Get/Scan without mutex_ (a large-value pointer is resolved on the
    // read path), replaced wholesale by Clear(). Atomic shared_ptr so a reader
    // either sees the old manager or the new one, never a dangling pointer.
    std::atomic<std::shared_ptr<VLogManager>> vlog_;
    std::unique_ptr<RateLimiter> compaction_rate_limiter_;  // v2.0 compaction 限速
    Manifest manifest_;
    // True once MANIFEST holds at least a snapshot. Until then UpdateManifest
    // writes a full snapshot; afterwards it appends deltas.
    bool manifest_written_ = false;
    // Serialises every touch of manifest_: both the in-place append in
    // UpdateManifest and the AddFile/RemoveFile edits that flush and compaction
    // make between appends. Without it the append can serialise a half-applied
    // edit, or two edits can interleave through the vector.
    mutable std::mutex manifest_mu_;

    mutable std::shared_mutex rw_mutex_;
    mutable std::mutex mutex_;
    std::condition_variable bg_cv_;
    std::condition_variable flush_cv_;
    // Signalled whenever a compaction drains L0, so writers parked by
    // WaitForCompactionCatchUp can re-check instead of sleeping out their whole
    // timeout.
    std::condition_variable compaction_cv_;
    uint64_t slow_writes_ = 0;   // writers that paused between the two thresholds
    uint64_t stalled_writes_ = 0;  // writers that had to block on the stop threshold

    // Serialises the two halves of a write that must stay together: the WAL
    // record and the memtable entry that record describes.
    //
    // This used to be mutex_, which also guards rotation bookkeeping, background
    // scheduling, stats and backup -- so every writer queued behind a statfs
    // call, an msync, or a Backup that was only reading state. None of that has
    // anything to do with a write. What a write actually needs is one thing: the
    // memtable it inserts into must be the one its WAL file belongs to.
    // TriggerFlush() takes this same lock to swap both, which is what provides
    // it.
    // A writers-only lock, held for a few microseconds at a time: one memcpy into
    // the WAL mapping plus one skip-list insert.
    //
    // std::mutex parks the thread on the second failure, and the park/wake pair
    // costs more than the wait itself: this path measured 606k ops/s on one
    // thread and 167k on eight, i.e. adding writers made it slower. Spinning
    // keeps the handoff in user space; because the lock is occasionally held for
    // real work (a rotation fsyncs the outgoing WAL, Clear unlinks files), the
    // spin is bounded and the fallback parks instead of burning a core.
    class WriteLock {
    public:
        void lock() {
            for (int spin = 0; spin < 512; ++spin) {
                if (!flag_.test_and_set(std::memory_order_acquire)) return;
            }
            // One waiter at a time re-tests the flag, holding queue_ while it
            // parks -- which is what makes the notify in unlock() impossible to
            // miss.
            std::unique_lock<std::mutex> q(queue_);
            while (flag_.test_and_set(std::memory_order_acquire)) {
                ready_.wait(q);
            }
        }
        void unlock() {
            flag_.clear(std::memory_order_release);
            std::lock_guard<std::mutex> q(queue_);
            ready_.notify_one();
        }

    private:
        std::atomic_flag flag_ = ATOMIC_FLAG_INIT;
        std::mutex queue_;
        std::condition_variable ready_;
    };

    WriteLock write_mu_;
    // Number of records appended to the ACTIVE WAL. Guarded by write_mu_, and
    // used by the group commit below as "how much is in the file".
    uint64_t wal_written_ = 0;

    // Group commit for WriteOptions::sync.
    //
    // One file cannot be fsynced twice at once, so N syncing writers used to
    // cost N fsyncs, one after another, each holding mutex_ for its whole
    // duration. `wal_synced_` records how much of the file a finished fsync
    // covers, so a single fsync can satisfy every writer whose record was
    // already in the file when it started -- and the rest skip theirs entirely.
    std::mutex wal_sync_mu_;
    std::condition_variable sync_cv_;   // signalled when an fsync finishes
    uint64_t wal_synced_ = 0;           // guarded by wal_sync_mu_
    // One writer at a time runs the fsync; the others wait for its result
    // instead of queueing their own. Guarded by wal_sync_mu_.
    bool syncing_ = false;
    // Number of fsyncs currently touching a WALWriter. Registered under
    // write_mu_ together with the file identity, and what Close()/rotation wait
    // on before destroying a file: an fsync must never run against a closed or
    // unmapped WAL. Guarded by wal_sync_mu_.
    int wal_sync_inflight_ = 0;
    // Bumped whenever the active WAL is replaced. An fsync that started against
    // the old file must not publish its position as durable for the new one.
    // Guarded by wal_sync_mu_.
    uint64_t wal_gen_ = 0;
    // Set the first time a writer asks for sync=true. Rotation syncs the outgoing
    // WAL so a writer that is overtaken by it still answers "durable" -- but a
    // database where nobody ever asks for sync should not pay an fsync per
    // memtable for a guarantee nobody wanted.
    std::atomic<bool> sync_requested_{false};

    // The version set.
    //
    // Locking contract -- the single most important invariant in this file:
    //
    //   * levels_ (and the pending_delete_ queue) is guarded by rw_mutex_:
    //     readers take it shared, flush/compaction take it exclusive while they
    //     add, detach or replace files.
    //   * mutex_ guards the memtable rotation bookkeeping (has_imm_, the WAL
    //     handoff) and is never taken while holding rw_mutex_.
    //   * write_mu_ guards the WAL append + memtable insert pair and the
    //     memtable/WAL swap that rotation performs. Nothing that is taken while
    //     holding it ever takes mutex_, so a writer never waits on bookkeeping.
    //   * wal_sync_mu_ guards wal_synced_, syncing_, wal_sync_inflight_ and
    //     wal_gen_, and is taken inside write_mu_. A sync that is *running* the
    //     fsync holds nothing at all -- that is what lets other writers append
    //     (and join the batch) while the disk is busy.
    //   * manifest_mu_ guards manifest_ and is taken innermost.
    //   * tomb_mu_ guards range_tombstones_ and is independent of the others.
    //
    // So the only legal order is
    //   rw_mutex_ -> mutex_ -> write_mu_ -> wal_sync_mu_ -> manifest_mu_.
    //
    // A thread about to close or unlink a WAL takes write_mu_ first and then
    // waits (releasing nothing) for wal_sync_inflight_ to drain: whoever is
    // fsyncing that file already dropped write_mu_, so the wait always ends.
    // Taking rw_mutex_ while holding mutex_ deadlocks against Clear(), which owns
    // rw_mutex_ exclusively and then waits for the background flush.
    std::vector<std::shared_ptr<SSTable>> levels_[kNumLevels];

    // Range tombstones that are still in effect, regardless of whether they live
    // in the memtable or have already been written into an SSTable.
    //
    // They have to be tracked here because a flushed tombstone is stored as a
    // synthetic entry ("\xff\xff\xff\xff" + begin_key) and a point lookup for a
    // covered key never touches it: without this table, Get() reports keys that
    // DeleteRange already removed as live. Guarded by its own mutex so reads do
    // not need to take rw_mutex_.
    struct RangeTombstoneInfo {
        std::string begin_key;
        std::string end_key;
        uint64_t seq = 0;
    };
    mutable std::shared_mutex tomb_mu_;
    std::vector<RangeTombstoneInfo> range_tombstones_;

    // True when `seq` is shadowed by a visible range tombstone.
    bool CoveredByRangeTombstone(const Slice& key, uint64_t seq, uint64_t snapshot) const;
    // Adds a tombstone to the DB-wide list. Called on every path that records a
    // range deletion, including WAL replay.
    void RegisterRangeTombstone(uint64_t seq, const Slice& begin_key, const Slice& end_key);
    // Rebuilds the list from the tombstone entries that flush wrote into SSTables.
    // Without this a restart forgets every DELRANGE that has already been
    // compacted out of the WAL, and the covered keys come back as live.
    void LoadRangeTombstonesFromSSTables();

    // Allocated IDs are handed to both the flush and the compaction worker, and
    // FlushForBackup can drive a flush from the caller's thread while the
    // background thread compacts. fetch_add is what keeps the two from handing
    // out the same file id (which overwrote an SSTable).
    std::atomic<uint64_t> next_file_id_;
    mutable std::vector<std::string> pending_delete_;

    std::unique_ptr<BlockCache> block_cache_;

    std::thread bg_thread_;
    std::atomic<bool> shutting_down_;
    mutable std::atomic<int> active_iterators_{0};
    std::atomic<uint64_t> stats_writes_{0};
    std::atomic<uint64_t> stats_reads_{0};
    std::atomic<uint64_t> stats_deletes_{0};
    std::atomic<uint64_t> stats_flushes_{0};
    std::atomic<uint64_t> stats_compactions_{0};
    // Written under mutex_, but BackgroundWork tests them after dropping it to
    // decide whether there is work to pick up. Plain bools there is a data race
    // that the compiler is entitled to hoist into a busy-spin.
    std::atomic<bool> bg_scheduled_;
    std::atomic<bool> has_imm_;

    // Consecutive failed attempts to flush the memtable currently in imm_.
    // Guarded by mutex_. Reset by a successful flush and by every rotation, so
    // it always describes the memtable that has_imm_ is pointing at. When it
    // reaches kMaxFlushAttempts the flush is given up on -- the records stay in
    // the WAL (FlushMemTable only unlinks it on success), so a reopen recovers
    // every acknowledged write, but this process stops accepting new ones rather
    // than filling memory forever with a memtable that can never be rotated.
    int flush_attempts_ = 0;
    // Last failure from WriteLevel0Table, kept so a waiter on flush_cv_ can say
    // why the flush never happened. Guarded by mutex_.
    Status flush_error_;
    // Counted by writers to decide when to look at the memtable size and the
    // free space. Atomic because the write path no longer runs under mutex_;
    // only one writer per 1024 can see a given boundary value, so the checks
    // still happen at the intended frequency.
    std::atomic<uint32_t> write_count_{0};

    // Waits until everything appended to the active WAL at or before `wal_index`
    // is on disk, doing the fsync itself if nobody else already covered it.
    // The fsync runs with no lock held, so a caller that is about to destroy a
    // WAL must first call WaitForWalSyncIdle().
    Status SyncWALTo(uint64_t wal_index);
    // Blocks until no fsync is touching a WALWriter. Call while holding
    // write_mu_ (so no new fsync can register) before closing or unlinking one.
    void WaitForWalSyncIdle();
    // Records that at least one writer wants sync durability; rotation consults
    // it before paying for an fsync on the outgoing file.
    void NoteSyncRequest(bool need_sync);
    // The tail every write shares: group commit, then the periodic free-space
    // and memtable-size checks.
    Status FinishWrite(bool need_sync, uint64_t wal_index);
};

} // namespace lightkv
