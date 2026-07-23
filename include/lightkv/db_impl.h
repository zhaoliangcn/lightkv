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
    uint64_t level_sizes[7] = {0};
    uint64_t pending_deletes = 0;
};

class DBImpl : public DB {
public:
    DBImpl(const Options& options);
    ~DBImpl() override;

    Status Put(const WriteOptions& options, const Slice& key, const Slice& value) override;

    Status Delete(const WriteOptions& options, const Slice& key) override;

    Status DeleteRange(const WriteOptions& options, const Slice& begin_key, const Slice& end_key) override;

    // v2.0: 原子批量写入 — 单次 WAL 提交，要么全部可见要么全部丢弃
    Status BatchWrite(const WriteOptions& options, const std::vector<WALRecord::BatchOp>& ops) override;

    Status Get(const ReadOptions& options, const Slice& key, std::string* value) override;

    bool Exists(const ReadOptions& options, const Slice& key) override;

    Status Scan(const ReadOptions& options, const Slice& prefix, int limit,
                std::vector<std::pair<std::string, std::string>>* results) override;

    Status Increment(const WriteOptions& options, const Slice& key, int64_t delta, int64_t* new_value) override;

    Status Rename(const WriteOptions& options, const Slice& src, const Slice& dst) override;

    Status Backup(const std::string& backup_path);

    DBStats GetStats() const;

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

    Status Initialize();

    void TriggerFlush();

    void FlushMemTable();

    void MaybeScheduleCompaction();

    CompactionScore PickCompaction();

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

    std::shared_ptr<MemTable> mem_;
    std::shared_ptr<MemTable> imm_;
    std::unique_ptr<WALWriter> wal_;
    std::unique_ptr<VLogManager> vlog_;  // v2.0 大 Value 分离存储
    Manifest manifest_;

    mutable std::shared_mutex rw_mutex_;
    mutable std::mutex mutex_;
    std::condition_variable bg_cv_;
    std::condition_variable flush_cv_;

    std::vector<std::shared_ptr<SSTable>> levels_[7];

    uint64_t next_file_id_;
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
    bool bg_scheduled_;
    bool has_imm_;
    uint32_t write_count_;
};

} // namespace lightkv