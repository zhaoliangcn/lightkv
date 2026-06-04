#include "lightkv/db_impl.h"
#include "lightkv/table_builder.h"
#include "lightkv/wal.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <unistd.h>
#include <cstring>
#include <sstream>

namespace lightkv {

DBImpl::DBImpl(const Options& options)
    : options_(options),
      last_seq_(0),
      next_file_id_(1),
      shutting_down_(false),
      bg_scheduled_(false),
      has_imm_(false),
      write_count_(0) {
    block_cache_ = std::make_unique<BlockCache>(options.block_cache_size);
}

DBImpl::~DBImpl() {
    shutting_down_ = true;

    // Trigger flush of current memtable if it has data
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (mem_ && !has_imm_) {
            imm_ = mem_;
            has_imm_ = true;
            mem_ = std::make_shared<MemTable>();
            bg_cv_.notify_one();
        }
    }

    // Wake up background thread to process flush and exit
    {
        std::lock_guard<std::mutex> lock(mutex_);
        bg_cv_.notify_all();
    }

    // Wait for background thread to finish
    if (bg_thread_.joinable()) {
        bg_thread_.join();
    }

    // Sync and close WAL
    if (wal_) {
        wal_->Sync();
        wal_->Close();
    }
}

Status DBImpl::Initialize() {
    if (::mkdir(options_.db_path.c_str(), 0755) < 0 && errno != EEXIST) {
        if (!options_.create_if_missing) {
            return Status::IOError("database directory does not exist");
        }
    }

    mem_ = std::make_shared<MemTable>();
    std::string wal_path = options_.db_path + "/wal.log";

    // Try to recover from existing WAL first (before creating new one)
    struct stat wal_stat;
    bool wal_exists = (::stat(wal_path.c_str(), &wal_stat) == 0 && wal_stat.st_size > 0);
    if (wal_exists) {
        WALReader reader(wal_path);
        auto rs = reader.Open();
        if (rs.ok()) {
            WALRecord record;
            while (reader.ReadRecord(&record)) {
                if (record.type == WALRecord::kTypeDeletion) {
                    mem_->InsertDeletion(record.seq, record.key);
                } else {
                    mem_->Insert(record.seq, record.key, record.value);
                }
                uint64_t expected = record.seq;
                last_seq_.compare_exchange_strong(expected, record.seq + 1);
            }
            reader.Close();
        }
    }

    // Now create the fresh WAL
    wal_ = std::make_unique<WALWriter>(wal_path);
    auto s = wal_->Open();
    if (!s.ok()) return s;

    // Scan for existing SSTable files
    for (int i = 0; ; ++i) {
        std::stringstream ss;
        ss << options_.db_path << "/" << i << ".sst";
        std::string fname = ss.str();
        if (::access(fname.c_str(), F_OK) != 0) break;
        auto table = std::make_shared<SSTable>(options_, fname, static_cast<uint64_t>(i));
        s = table->Open();
        if (!s.ok()) return s;
        levels_[0].push_back(std::move(table));
        next_file_id_ = std::max(next_file_id_, static_cast<uint64_t>(i) + 1);
    }

    // Start background thread
    bg_thread_ = std::thread(&DBImpl::BackgroundWork, this);

    return Status::OK();
}

Status DBImpl::Put(const WriteOptions& options, const Slice& key, const Slice& value) {
    stats_writes_.fetch_add(1, std::memory_order_relaxed);
    uint64_t seq = last_seq_.fetch_add(1, std::memory_order_relaxed);
    std::lock_guard<std::mutex> lock(mutex_);

    Status s = wal_->Append(seq, WALRecord::kTypeValue, key, value);
    if (!s.ok()) {
        // Retry on WAL write failure
        for (int retry = 0; retry < 3; ++retry) {
            s = wal_->Append(seq, WALRecord::kTypeValue, key, value);
            if (s.ok()) break;
        }
        if (!s.ok()) return s;
    }
    mem_->Insert(seq, key, value);

    if (options.sync) {
        wal_->Sync();
    }

    ++write_count_;
    if ((write_count_ & 1023) == 0) {
        // Check disk space periodically
        s = CheckDiskSpace();
        if (!s.ok()) return s;

        if (mem_->ApproximateMemoryUsage() > options_.memtable_size) {
            TriggerFlush();
        }
    }

    return Status::OK();
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
    stats_deletes_.fetch_add(1, std::memory_order_relaxed);
    uint64_t seq = last_seq_.fetch_add(1, std::memory_order_relaxed);
    std::lock_guard<std::mutex> lock(mutex_);

    wal_->Append(seq, WALRecord::kTypeDeletion, key, Slice());
    mem_->InsertDeletion(seq, key);

    if (options.sync) {
        wal_->Sync();
    }

    ++write_count_;
    if ((write_count_ & 1023) == 0 && mem_->ApproximateMemoryUsage() > options_.memtable_size) {
        TriggerFlush();
    }

    return Status::OK();
}

Status DBImpl::Get(const ReadOptions& options, const Slice& key, std::string* value) {
    stats_reads_.fetch_add(1, std::memory_order_relaxed);
    uint64_t snapshot = last_seq_.load(std::memory_order_acquire);

    // Use shared_lock for concurrent reads
    std::shared_lock<std::shared_mutex> read_lock(rw_mutex_);

    if (mem_->Get(key, value, snapshot)) {
        return Status::OK();
    }
    if (imm_ && imm_->Get(key, value, snapshot)) {
        return Status::OK();
    }

    read_lock.unlock();

    // Search SSTable levels (will re-acquire shared_lock in SearchSSTable)
    for (int level = 0; level < 7; ++level) {
        Status s = SearchSSTable(level, key, value, snapshot);
        if (s.IsNotFound()) continue;
        return s;
    }

    return Status::NotFound();
}

DBStats DBImpl::GetStats() const {
    DBStats stats;
    stats.total_writes = stats_writes_.load(std::memory_order_relaxed);
    stats.total_reads = stats_reads_.load(std::memory_order_relaxed);
    stats.total_deletes = stats_deletes_.load(std::memory_order_relaxed);
    stats.total_flushes = stats_flushes_.load(std::memory_order_relaxed);
    stats.total_compactions = stats_compactions_.load(std::memory_order_relaxed);
    stats.pending_deletes = pending_delete_.size();
    {
        std::shared_lock<std::shared_mutex> lock(rw_mutex_);
        stats.memtable_size = mem_->ApproximateMemoryUsage();
        if (imm_) stats.imm_size = imm_->ApproximateMemoryUsage();
        for (int i = 0; i < 7; ++i) {
            for (const auto& f : levels_[i]) {
                stats.level_sizes[i] += f->FileSize();
            }
        }
    }
    return stats;
}

Status DBImpl::SearchSSTable(int level, const Slice& key, std::string* value, uint64_t /*snapshot_seq*/) const {
    std::shared_lock<std::shared_mutex> lock(rw_mutex_);
    const auto& files = levels_[level];
    for (const auto& table : files) {
        if (table->MayMatch(key)) {
            uint64_t seq;
            Status s = table->Get(key, value, &seq);
            if (s.ok()) {
                return Status::OK();
            }
        }
    }
    return Status::NotFound();
}

Status DBImpl::CheckDiskSpace() const {
    struct statvfs vfs;
    if (::statvfs(options_.db_path.c_str(), &vfs) < 0) {
        return Status::IOError("cannot stat filesystem");
    }

    uint64_t free_bytes = static_cast<uint64_t>(vfs.f_bavail) * vfs.f_frsize;
    uint64_t memtable_bytes = mem_->ApproximateMemoryUsage();

    // Need at least memtable_size * 2 free space (one Flush + one Compaction)
    if (free_bytes < memtable_bytes * 2) {
        return Status::IOError("insufficient disk space");
    }

    return Status::OK();
}

void DBImpl::TriggerFlush() {
    if (has_imm_) return;
    imm_ = mem_;
    has_imm_ = true;
    mem_ = std::make_shared<MemTable>();

    bg_cv_.notify_one();
    bg_scheduled_ = true;
}

void DBImpl::FlushMemTable() {
    stats_flushes_.fetch_add(1, std::memory_order_relaxed);
    std::shared_ptr<MemTable> mem_to_flush;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!has_imm_) return;
        mem_to_flush = imm_;
    }

    uint64_t file_id;
    Status s = WriteLevel0Table(mem_to_flush, &file_id);
    if (!s.ok()) return;

    {
        std::lock_guard<std::mutex> lock(mutex_);
        imm_.reset();
        has_imm_ = false;
        bg_scheduled_ = false;

        // Truncate WAL to remove flushed data (saves disk space and speeds recovery)
        wal_->Truncate();

        flush_cv_.notify_all();
    }
}

void DBImpl::CleanupDeletedFiles() const {
    // Only delete files when no active iterators exist
    if (active_iterators_.load(std::memory_order_acquire) > 0) return;
    
    std::unique_lock<std::shared_mutex> lock(rw_mutex_);
    for (const auto& path : pending_delete_) {
        std::remove(path.c_str());
    }
    pending_delete_.clear();
}

Status DBImpl::WriteLevel0Table(std::shared_ptr<MemTable> mem, uint64_t* file_id) {
    *file_id = next_file_id_++;
    std::stringstream ss;
    ss << options_.db_path << "/" << *file_id << ".sst";
    std::string fname = ss.str();

    TableBuilder builder(options_, fname);

    auto iter = mem->SeekToFirst();
    while (iter.Valid()) {
        builder.Add(Slice(iter.key()), Slice(iter.value()));
        iter.Next();
    }

    builder.Finish();

    auto table = std::make_shared<SSTable>(options_, fname, *file_id);
    auto s = table->Open();
    if (!s.ok()) return s;

    {
        std::unique_lock<std::shared_mutex> lock(rw_mutex_);
        levels_[0].push_back(std::move(table));
    }

    MaybeScheduleCompaction();

    return Status::OK();
}

void DBImpl::MaybeScheduleCompaction() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (bg_scheduled_ || has_imm_) return;

    auto score = PickCompaction();
    if (score.level >= 0 && score.score > 1.0) {
        bg_scheduled_ = true;
        bg_cv_.notify_one();
    }
}

CompactionScore DBImpl::PickCompaction() {
    // Check L0: file count
    if (levels_[0].size() >= options_.l0_file_num_trigger) {
        return {0, static_cast<double>(levels_[0].size()) / options_.l0_file_num_trigger};
    }

    // Check L1-L6: total size vs target
    uint64_t base_size = static_cast<uint64_t>(options_.memtable_size) * options_.level_multiplier;
    uint64_t cumulative = base_size;
    for (int i = 1; i < static_cast<int>(options_.max_level); ++i) {
        uint64_t level_size = 0;
        for (const auto& file : levels_[i]) {
            // Approximate size from file ID tracking, or just use number of files
            level_size += file->FileSize();
        }
        uint64_t target_size = cumulative * options_.level_multiplier;
        if (level_size > target_size) {
            return {i, static_cast<double>(level_size) / target_size};
        }
        cumulative += level_size;
    }

    return {-1, 0.0};
}

void DBImpl::DoLevel0Compaction() {
    // Collect all L0 files and overlapping L1 files
    std::vector<std::shared_ptr<SSTable>> inputs;
    std::string l0_min_key, l0_max_key;

    {
        std::unique_lock<std::shared_mutex> lock(rw_mutex_);
        if (levels_[0].empty()) return;

        // Compute overall key range of L0
        for (auto& file : levels_[0]) {
            if (inputs.empty()) {
                l0_min_key = file->SmallestKey();
                l0_max_key = file->LargestKey();
            } else {
                if (!file->SmallestKey().empty() && file->SmallestKey() < l0_min_key)
                    l0_min_key = file->SmallestKey();
                if (!file->LargestKey().empty() && file->LargestKey() > l0_max_key)
                    l0_max_key = file->LargestKey();
            }
            inputs.push_back(file);
        }

        // Find overlapping L1 files
        if (!levels_[1].empty() && !l0_min_key.empty()) {
            for (auto& file : levels_[1]) {
                // Check if key ranges overlap
                if (file->LargestKey() >= l0_min_key && file->SmallestKey() <= l0_max_key) {
                    inputs.push_back(file);
                }
            }
        }

        // Clear all selected files from their levels
        levels_[0].clear();
        if (!levels_[1].empty()) {
            auto it = std::remove_if(levels_[1].begin(), levels_[1].end(),
                [&l0_min_key, &l0_max_key](const std::shared_ptr<SSTable>& f) {
                    return f->LargestKey() >= l0_min_key && f->SmallestKey() <= l0_max_key;
                });
            levels_[1].erase(it, levels_[1].end());
        }
    }

    // Do the compaction (no lock needed - working with local copies)
    std::vector<uint64_t> new_file_ids;
    CompactionWorker worker(options_, options_.db_path, &next_file_id_);
    auto s = worker.DoCompaction(inputs, 1, &new_file_ids);
    if (!s.ok()) return;

    // Apply VersionEdit: add new files to L1
    {
        std::unique_lock<std::shared_mutex> lock(rw_mutex_);
        const auto& added = worker.edit().added_files();
        for (auto& meta : added) {
            std::stringstream ss;
            ss << options_.db_path << "/" << meta.file_id << ".sst";
            auto table = std::make_shared<SSTable>(options_, ss.str(), meta.file_id);
            if (table->Open().ok()) {
                levels_[meta.level].push_back(std::move(table));
            }
        }
    }

    // Deferred deletion: add old files to pending_delete_
    {
        std::unique_lock<std::shared_mutex> lock(rw_mutex_);
        for (auto& file : inputs) {
            pending_delete_.push_back(file->filename());
        }
    }
    CleanupDeletedFiles();
}

void DBImpl::DoLevelCompaction(int level) {
    // Pick one file from level and overlapping files from level+1
    std::vector<std::shared_ptr<SSTable>> inputs;
    std::string selected_min, selected_max;

    {
        std::unique_lock<std::shared_mutex> lock(rw_mutex_);
        if (levels_[level].empty()) return;

        // Pick the first file (simplified: could use a more sophisticated policy)
        auto& picked = levels_[level].front();
        selected_min = picked->SmallestKey();
        selected_max = picked->LargestKey();
        inputs.push_back(picked);

        // Find overlapping files in level+1
        int next_level = level + 1;
        if (next_level < 7 && !levels_[next_level].empty()) {
            for (auto& file : levels_[next_level]) {
                if (file->LargestKey() >= selected_min && file->SmallestKey() <= selected_max) {
                    inputs.push_back(file);
                }
            }
        }

        // Remove selected files from their levels
        auto remove_from_level = [this](int lvl, const std::string& min_key, const std::string& max_key) {
            auto& files = levels_[lvl];
            auto it = std::remove_if(files.begin(), files.end(),
                [&min_key, &max_key](const std::shared_ptr<SSTable>& f) {
                    return f->LargestKey() >= min_key && f->SmallestKey() <= max_key;
                });
            files.erase(it, files.end());
        };
        remove_from_level(level, selected_min, selected_max);
        if (next_level < 7) {
            remove_from_level(next_level, selected_min, selected_max);
        }
    }

    // Do the compaction
    std::vector<uint64_t> new_file_ids;
    CompactionWorker worker(options_, options_.db_path, &next_file_id_);
    auto s = worker.DoCompaction(inputs, level + 1, &new_file_ids);
    if (!s.ok()) return;

    // Apply VersionEdit: add new files
    {
        std::unique_lock<std::shared_mutex> lock(rw_mutex_);
        const auto& added = worker.edit().added_files();
        for (auto& meta : added) {
            std::stringstream ss;
            ss << options_.db_path << "/" << meta.file_id << ".sst";
            auto table = std::make_shared<SSTable>(options_, ss.str(), meta.file_id);
            if (table->Open().ok()) {
                levels_[meta.level].push_back(std::move(table));
            }
        }
    }

    // Deferred deletion
    {
        std::unique_lock<std::shared_mutex> lock(rw_mutex_);
        for (auto& file : inputs) {
            pending_delete_.push_back(file->filename());
        }
    }
    CleanupDeletedFiles();
}

void DBImpl::BackgroundCompaction() {
    stats_compactions_.fetch_add(1, std::memory_order_relaxed);
    CompactionScore score;
    {
        std::shared_lock<std::shared_mutex> lock(rw_mutex_);
        score = PickCompaction();
    }
    if (score.level < 0 || score.score <= 1.0) return;

    if (score.level == 0) {
        DoLevel0Compaction();
    } else {
        DoLevelCompaction(score.level);
    }
}

void DBImpl::BackgroundWork() {
    while (true) {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            bg_cv_.wait(lock, [this] {
                return shutting_down_ || has_imm_ || bg_scheduled_;
            });

            // If shutting down and no work pending, exit
            if (shutting_down_ && !has_imm_ && !bg_scheduled_) break;
        }

        if (has_imm_) {
            FlushMemTable();
            // After Flush, re-evaluate compaction
            MaybeScheduleCompaction();
        }

        if (bg_scheduled_ && !has_imm_) {
            BackgroundCompaction();
            // Reset bg_scheduled_ and check if more compaction is needed
            std::lock_guard<std::mutex> lock(mutex_);
            bg_scheduled_ = false;
            // Re-check compaction after completing one round
            auto score = PickCompaction();
            if (score.level >= 0 && score.score > 1.0) {
                bg_scheduled_ = true;
                bg_cv_.notify_one();
            }
        }
    }
}

Status DB::Open(const Options& options, DB** dbptr) {
    auto* impl = new DBImpl(options);
    Status s = impl->Initialize();
    if (s.ok()) {
        *dbptr = impl;
    } else {
        delete impl;
        *dbptr = nullptr;
    }
    return s;
}

} // namespace lightkv