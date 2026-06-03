#include "lightkv/db_impl.h"
#include "lightkv/table_builder.h"
#include "lightkv/wal.h"
#include <fcntl.h>
#include <sys/stat.h>
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
    {
        std::lock_guard<std::mutex> lock(mutex_);
        bg_cv_.notify_all();
    }
    if (bg_thread_.joinable()) {
        bg_thread_.join();
    }
    if (wal_) {
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
    uint64_t seq = last_seq_.fetch_add(1, std::memory_order_relaxed);
    std::lock_guard<std::mutex> lock(mutex_);

    wal_->Append(seq, WALRecord::kTypeValue, key, value);
    mem_->Insert(seq, key, value);

    if (options.sync) {
        wal_->Sync();
    }

    ++write_count_;
    if ((write_count_ & 1023) == 0 && mem_->ApproximateMemoryUsage() > options_.memtable_size) {
        TriggerFlush();
    }

    return Status::OK();
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
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
    uint64_t snapshot = last_seq_.load(std::memory_order_acquire);

    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (mem_->Get(key, value, snapshot)) {
            return Status::OK();
        }
        if (imm_ && imm_->Get(key, value, snapshot)) {
            return Status::OK();
        }
    }

    for (int level = 0; level < 7; ++level) {
        Status s = SearchSSTable(level, key, value, snapshot);
        if (s.IsNotFound()) continue;
        return s;
    }

    return Status::NotFound();
}

Status DBImpl::SearchSSTable(int level, const Slice& key, std::string* value, uint64_t /*snapshot_seq*/) const {
    std::lock_guard<std::mutex> lock(mutex_);
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

void DBImpl::TriggerFlush() {
    if (has_imm_) return;
    imm_ = mem_;
    has_imm_ = true;
    mem_ = std::make_shared<MemTable>();

    bg_cv_.notify_one();
    bg_scheduled_ = true;
}

void DBImpl::FlushMemTable() {
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

        wal_->Close();
        std::string wal_path = options_.db_path + "/wal.log";
        wal_ = std::make_unique<WALWriter>(wal_path);
        wal_->Open();
    }
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
        std::lock_guard<std::mutex> lock(mutex_);
        levels_[0].push_back(std::move(table));
    }

    MaybeScheduleCompaction();

    return Status::OK();
}

void DBImpl::MaybeScheduleCompaction() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (bg_scheduled_) return;

    if (levels_[0].size() >= options_.l0_file_num_trigger) {
        bg_scheduled_ = true;
        bg_cv_.notify_one();
    }
}

void DBImpl::BackgroundCompaction() {
    std::vector<std::shared_ptr<SSTable>> l0_files;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (levels_[0].size() < options_.l0_file_num_trigger) return;
        l0_files = std::move(levels_[0]);
        levels_[0].clear();
    }

    CompactionWorker worker(options_);
    worker.ScheduleCompaction(0, std::move(l0_files), {});
}

void DBImpl::BackgroundWork() {
    while (!shutting_down_) {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            bg_cv_.wait(lock, [this] {
                return shutting_down_ || has_imm_ || bg_scheduled_;
            });
        }

        if (shutting_down_) break;

        if (has_imm_) {
            FlushMemTable();
        }

        if (bg_scheduled_ && !has_imm_) {
            BackgroundCompaction();
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