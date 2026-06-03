#pragma once

#include "db.h"
#include "memtable.h"
#include "wal.h"
#include "sstable.h"
#include "cache.h"
#include "compaction.h"
#include "options.h"
#include "slice.h"
#include "status.h"
#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace lightkv {

class DBImpl : public DB {
public:
    DBImpl(const Options& options);
    ~DBImpl() override;

    Status Put(const WriteOptions& options, const Slice& key, const Slice& value) override;

    Status Delete(const WriteOptions& options, const Slice& key) override;

    Status Get(const ReadOptions& options, const Slice& key, std::string* value) override;

    class Iterator {
    public:
        Iterator(const DBImpl* db, uint64_t snapshot_seq);
        ~Iterator() = default;

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
        std::vector<std::shared_ptr<SSTable>>::const_iterator level_iter_;
        int current_level_;
    };

private:
    friend class DB;

    Status Initialize();

    void TriggerFlush();

    void FlushMemTable();

    void MaybeScheduleCompaction();

    void BackgroundCompaction();

    void BackgroundWork();

    Status WriteLevel0Table(std::shared_ptr<MemTable> mem, uint64_t* file_id);

    Status SearchSSTable(int level, const Slice& key, std::string* value, uint64_t snapshot_seq) const;

    Options options_;
    std::atomic<uint64_t> last_seq_;

    std::shared_ptr<MemTable> mem_;
    std::shared_ptr<MemTable> imm_;
    std::unique_ptr<WALWriter> wal_;

    mutable std::mutex mutex_;
    std::condition_variable bg_cv_;
    std::condition_variable flush_cv_;

    std::vector<std::shared_ptr<SSTable>> levels_[7];

    uint64_t next_file_id_;

    std::unique_ptr<BlockCache> block_cache_;

    std::thread bg_thread_;
    std::atomic<bool> shutting_down_;
    bool bg_scheduled_;
    bool has_imm_;
    uint32_t write_count_;
};

} // namespace lightkv