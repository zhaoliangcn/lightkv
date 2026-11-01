#include "lightkv/db_impl.h"
#include "lightkv/table_builder.h"
#include "lightkv/wal.h"
#include "lightkv/encoding.h"
#include "lightkv/platform.h"
#include <algorithm>
#include <cstring>
#include <cstdio>
#include <sstream>
#include <filesystem>

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
        auto mem = mem_.load(std::memory_order_acquire);
        if (mem && !has_imm_) {
            imm_.store(mem, std::memory_order_release);
            has_imm_ = true;
            flush_attempts_ = 0;
            flush_error_ = Status::OK();
            mem_.store(std::make_shared<MemTable>(), std::memory_order_release);
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

    // Sync and close WAL. No writer can be inside an append any more (the
    // background thread is joined and the caller owns the handle), but an fsync
    // started earlier may still be holding a pointer to these objects.
    {
        std::lock_guard<WriteLock> wl(write_mu_);
        WaitForWalSyncIdle();
        if (wal_) {
            wal_->Sync();
            wal_->Close();
        }
        if (imm_wal_) {
            imm_wal_->Sync();
            imm_wal_->Close();
        }
    }

    // Write final manifest
    //
    // A destructor cannot return an error, so this is the one place that has to
    // log and carry on. It is also the least dangerous one: every flush that
    // reached an SSTable has already persisted a manifest naming it (a flush
    // only clears has_imm_ after WriteLevel0Table returns OK), so what can be
    // lost here is the counters -- last_seq, next_file_id -- not the file set.
    if (Status ms = UpdateManifest(); !ms.ok()) {
        std::fprintf(stderr,
                     "[LightKV] WARNING: failed to persist MANIFEST on close: %s\n"
                     "          The file set on disk is the one from the last successful\n"
                     "          update; anything written since is in the WAL and will be\n"
                     "          replayed on the next open.\n",
                     ms.ToString().c_str());
    }
}

Status DBImpl::Initialize() {
    // Clamp the level count before anything indexes levels_.
    //
    // Options::max_level is a plain user-supplied number (the config file sets it
    // directly), while levels_ is a fixed-size array of kNumLevels. A value of 9
    // made PickCompaction walk into levels_[7] and levels_[8] -- reading two
    // vectors past the end of the member, then handing the garbage score to
    // DoLevelCompaction. Clamping here keeps every consumer looking at the same
    // number; EffectiveMaxLevel() is the belt-and-braces bound at the index site.
    if (options_.max_level < 2) {
        std::fprintf(stderr,
                     "[LightKV] options.max_level=%zu is below the minimum of 2; using 2.\n",
                     options_.max_level);
        options_.max_level = 2;
    } else if (options_.max_level > static_cast<size_t>(kNumLevels)) {
        std::fprintf(stderr,
                     "[LightKV] options.max_level=%zu exceeds the %d levels this build "
                     "supports; using %d.\n",
                     options_.max_level, kNumLevels, kNumLevels);
        options_.max_level = static_cast<size_t>(kNumLevels);
    }

    // Create the database directory if it doesn't exist.
    // Verify explicitly rather than trusting the error code: a silently skipped
    // creation would otherwise surface much later as a confusing
    // "cannot open WAL file" from a completely different layer.
    {
        std::error_code ec;
        std::filesystem::create_directories(options_.db_path, ec);
        std::error_code check_ec;
        if (!std::filesystem::is_directory(options_.db_path, check_ec)) {
            return Status::IOError("cannot create database directory '" +
                                   options_.db_path + "': " +
                                   (ec ? ec.message() : check_ec.message()));
        }
    }

    // No other thread exists yet, so a local handle is enough for replay; the
    // atomic member is published once recovery has filled it in.
    auto boot_mem = std::make_shared<MemTable>();

    // Replay every WAL left behind, not just "wal.log". Each memtable owns its
    // own WAL (see TriggerFlush), so a crash can leave the active one plus the one
    // belonging to an unflushed immutable memtable; older databases used the
    // single name "wal.log". Replaying all of them and then removing them keeps
    // recovery complete without replaying the same records twice.
    std::vector<std::string> wal_files;
    {
        std::error_code ec;
        for (const auto& entry : std::filesystem::directory_iterator(options_.db_path, ec)) {
            const std::string name = entry.path().filename().string();
            if (name.rfind("wal", 0) == 0 && name.size() > 4 &&
                name.compare(name.size() - 4, 4, ".log") == 0) {
                wal_files.push_back(entry.path().string());
            }
        }
    }
    std::sort(wal_files.begin(), wal_files.end());

    for (const auto& path : wal_files) {
        struct stat wal_stat;
        if (::stat(path.c_str(), &wal_stat) != 0 || wal_stat.st_size == 0) continue;

        WALReader reader(path);
        auto rs = reader.Open();
        if (rs.ok()) {
            WALRecord record;
            while (reader.ReadRecord(&record)) {
                if (record.type == WALRecord::kTypeDeletion) {
                    boot_mem->InsertDeletion(record.seq, record.key);
                } else if (record.type == WALRecord::kTypeRangeDeletion) {
                    boot_mem->InsertRangeDeletion(record.seq, record.begin_key, record.end_key);
                    RegisterRangeTombstone(record.seq, record.begin_key, record.end_key);
                } else if (record.type == WALRecord::kTypeBatch) {
                    // v2.0: 批量记录展开 — 所有子操作共用同一 seq
                    for (const auto& op : record.batch_ops) {
                        if (op.type == WALRecord::kTypeDeletion) {
                            boot_mem->InsertDeletion(record.seq, op.key);
                        } else if (op.type == WALRecord::kTypeRangeDeletion) {
                            boot_mem->InsertRangeDeletion(record.seq, op.begin_key, op.end_key);
                            RegisterRangeTombstone(record.seq, op.begin_key, op.end_key);
                        } else {
                            boot_mem->Insert(record.seq, op.key, op.value);
                        }
                    }
                } else {
                    boot_mem->Insert(record.seq, record.key, record.value);
                }
                // Atomic max, NOT "bump if the counter happens to equal this
                // record's seq". Recovery replays every surviving WAL from the
                // oldest one, so the first records carry sequence numbers far
                // BELOW the boundary MANIFEST already knows about; a
                // compare_exchange gated on exact equality then never fires for
                // any of them, last_seq_ stays stuck at the manifest value, and
                // every record above it becomes invisible to a default-snapshot
                // Get -- acknowledged writes that read back as NotFound.
                const uint64_t next_seq = record.seq + 1;
                uint64_t seen = last_seq_.load(std::memory_order_relaxed);
                while (seen < next_seq &&
                       !last_seq_.compare_exchange_weak(seen, next_seq,
                                                        std::memory_order_release,
                                                        std::memory_order_relaxed)) {
                }
            }
            reader.Close();
        }
    }

    mem_.store(boot_mem, std::memory_order_release);

    // The records live in the memtable now; leaving the files behind would make
    // the next start replay them a second time.
    for (const auto& path : wal_files) {
        std::error_code ec;
        std::filesystem::remove(path, ec);
    }

    // Now create the fresh WAL, numbered so each memtable gets its own file.
    next_wal_id_ = static_cast<uint64_t>(wal_files.size()) + 1;
    wal_path_ = options_.db_path + "/wal-" + std::to_string(next_wal_id_++) + ".log";
    wal_ = std::make_unique<WALWriter>(wal_path_);
    auto s = wal_->Open();
    if (!s.ok()) return s;

    // v2.0: Initialize VLog manager for large value separation
    auto boot_vlog = std::make_shared<VLogManager>(options_.db_path, options_.vlog_file_size_limit);
    s = boot_vlog->Initialize();
    vlog_.store(boot_vlog, std::memory_order_release);
    if (!s.ok()) return s;

    // v2.0: Initialize compaction rate limiter (0 = 不限速)
    if (options_.compaction_rate_limit > 0) {
        compaction_rate_limiter_ = std::make_unique<RateLimiter>(options_.compaction_rate_limit);
    }

    // Try to load MANIFEST for metadata recovery
    bool has_manifest = (::access((options_.db_path + "/MANIFEST").c_str(), F_OK) == 0);
    if (has_manifest) {
        auto ms = manifest_.ReadFromFile(options_.db_path);
        if (!ms.ok()) {
            // Refusing to open beats opening into a half-known state: a
            // corrupted manifest means we cannot tell which SSTables are live,
            // and silently carrying on would look like data loss later.
            return ms;
        }
        manifest_written_ = true;
        // Restore metadata from manifest
        //
        // last_seq_ is a MAX, not a plain assignment. The replay above already
        // raised it to cover every record it read, and those records are exactly
        // the ones the manifest has never seen: UpdateManifest only runs on a
        // flush or a compaction, so everything written since the last flush lives
        // solely in the WAL. Assigning manifest_.last_seq unconditionally lowered
        // last_seq_ back below them, and a default-snapshot Get filters on
        // `seq <= last_seq_` -- measured: 5000 writes before a flush plus 5000
        // after, crash, reopen, 4999 of the second batch read back as NotFound
        // while sitting right there in the WAL.
        const uint64_t replayed = last_seq_.load(std::memory_order_acquire);
        last_seq_.store(std::max(manifest_.last_seq, replayed), std::memory_order_release);
        // Everything the manifest describes is in SSTables by definition; this is
        // the durable prefix Backup measures against.
        flushed_seq_.store(manifest_.flushed_seq, std::memory_order_release);
        next_file_id_.store(manifest_.next_file_id, std::memory_order_release);

        // Load SSTable files from manifest
        for (int level = 0; level < kNumLevels; ++level) {
            for (const auto& meta : manifest_.files[level]) {
                std::stringstream ss;
                ss << options_.db_path << "/" << meta.file_id << ".sst";
                auto table = std::make_shared<SSTable>(options_, ss.str(), meta.file_id);
                s = table->Open();
                if (!s.ok()) return s;
                levels_[level].push_back(std::move(table));
            }
        }
    } else {
        // Fallback: scan for existing SSTable files
        for (int i = 0; ; ++i) {
            std::stringstream ss;
            ss << options_.db_path << "/" << i << ".sst";
            std::string fname = ss.str();
            if (::access(fname.c_str(), F_OK) != 0) break;
            auto table = std::make_shared<SSTable>(options_, fname, static_cast<uint64_t>(i));
            s = table->Open();
            if (!s.ok()) return s;
            levels_[0].push_back(std::move(table));
            // Still single-threaded here (the background thread has not been
            // started), so a plain load/store is all this needs.
            if (next_file_id_.load(std::memory_order_relaxed) <= static_cast<uint64_t>(i)) {
                next_file_id_.store(static_cast<uint64_t>(i) + 1, std::memory_order_relaxed);
            }
        }
    }

    // Range tombstones that reached an SSTable are no longer in a memtable or in
    // the WAL (a flush truncates it). Rebuild them here or every covered key
    // looks live again after a restart.
    LoadRangeTombstonesFromSSTables();

    // Start background thread
    bg_thread_ = std::thread(&DBImpl::BackgroundWork, this);

    return Status::OK();
}

Status DBImpl::Put(const WriteOptions& options, const Slice& key, const Slice& value) {
    // Backpressure goes first, before a sequence number is taken or the WAL is
    // touched: a writer already past the stop threshold is exactly what keeps
    // piling files onto the level that is too full.
    Status bs = WaitForCompactionCatchUp();
    if (!bs.ok()) return bs;
    return WriteImpl(options, key, value);
}

Status DBImpl::WriteImpl(const WriteOptions& options, const Slice& key,
                         const Slice& value) {
    stats_writes_.fetch_add(1, std::memory_order_relaxed);
    NoteSyncRequest(options.sync);
    // Claimed before the write lock: nothing below depends on the order writers
    // arrive in, only on sequence numbers being unique and increasing.
    const uint64_t seq = last_seq_.fetch_add(1, std::memory_order_relaxed);

    uint64_t wal_index = 0;
    {
        // The whole of a write happens in here, and nothing else does.
        //
        // The memtable this record lands in has to be the one its WAL file
        // belongs to; TriggerFlush() swaps the pair under this same lock, which
        // is what makes that true. The DB-wide mutex_ used to provide it, but
        // mutex_ also guards rotation bookkeeping, background scheduling, stats
        // and backup -- every writer queued behind all of that.
        std::lock_guard<WriteLock> wl(write_mu_);

        // v2.0: Large value separation — value over threshold goes to vlog,
        // MemTable stores a tagged pointer instead. Tagged format:
        //   \x01 + 8B file_id + 8B offset + 8B length  (vlog pointer)
        //   \x00 + original bytes                         (inline value)
        std::string mem_value;
        // Pin the manager for the duration of the append: Clear() swaps it out
        // wholesale, and takes this lock to do it, so the manager an append runs
        // against cannot be closed and unlinked underneath it.
        auto vlog = vlog_.load(std::memory_order_acquire);
        if (vlog && value.size() > options_.value_threshold) {
            uint64_t file_id, offset, length;
            Status vs = vlog->Append(value, &file_id, &offset, &length);
            if (!vs.ok()) return vs;
            mem_value.reserve(1 + 24);
            mem_value.push_back('\x01');
            mem_value.append(reinterpret_cast<const char*>(&file_id), 8);
            mem_value.append(reinterpret_cast<const char*>(&offset), 8);
            mem_value.append(reinterpret_cast<const char*>(&length), 8);
        } else {
            mem_value.push_back('\x00');
            mem_value.append(value.data(), value.size());
        }
        Slice stored_value(mem_value);

        Status s = wal_->Append(seq, WALRecord::kTypeValue, key, stored_value);
        if (!s.ok()) return s;
        mem_.load(std::memory_order_acquire)->Insert(seq, key, stored_value);
        wal_index = ++wal_written_;
    }

    return FinishWrite(options.sync, wal_index);
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
    stats_deletes_.fetch_add(1, std::memory_order_relaxed);
    NoteSyncRequest(options.sync);
    const uint64_t seq = last_seq_.fetch_add(1, std::memory_order_relaxed);

    uint64_t wal_index = 0;
    {
        std::lock_guard<WriteLock> wl(write_mu_);
        Status s = wal_->Append(seq, WALRecord::kTypeDeletion, key, Slice());
        if (!s.ok()) return s;
        mem_.load(std::memory_order_acquire)->InsertDeletion(seq, key);
        wal_index = ++wal_written_;
    }

    return FinishWrite(options.sync, wal_index);
}

Status DBImpl::DeleteRange(const WriteOptions& options, const Slice& begin_key,
                           const Slice& end_key, int64_t* deleted_count) {
    if (!(begin_key < end_key)) {
        return Status::InvalidArgument("begin_key must be less than end_key");
    }
    if (deleted_count) *deleted_count = 0;

    // Count the live keys the tombstone is about to shadow, so callers can report
    // a real number instead of a constant. Iterating the engine's own view is
    // exact: SkipDeleted already hides tombstones and keys covered by earlier
    // range tombstones.
    if (deleted_count) {
        Iterator it(this, last_seq_.load(std::memory_order_acquire));
        it.Seek(begin_key);
        while (it.Valid()) {
            Slice k = it.key();
            if (!(k < end_key)) break;
            ++*deleted_count;
            it.Next();
        }
    }

    stats_deletes_.fetch_add(1, std::memory_order_relaxed);
    NoteSyncRequest(options.sync);
    const uint64_t seq = last_seq_.fetch_add(1, std::memory_order_relaxed);

    uint64_t wal_index = 0;
    {
        std::lock_guard<WriteLock> wl(write_mu_);
        Status s = wal_->AppendRangeDelete(seq, begin_key, end_key);
        if (!s.ok()) return s;
        mem_.load(std::memory_order_acquire)->InsertRangeDeletion(seq, begin_key, end_key);
        wal_index = ++wal_written_;
    }

    // Record it in the DB-wide list as well. The memtable copy disappears at the
    // next flush, and Get() never looks at the tombstone entry that lands in the
    // SSTable, so this is the only place a covered key can be recognised as gone.
    RegisterRangeTombstone(seq, begin_key, end_key);

    return FinishWrite(options.sync, wal_index);
}

Status DBImpl::BatchWrite(const WriteOptions& options, const std::vector<WALRecord::BatchOp>& ops) {
    if (ops.empty()) return Status::OK();
    stats_writes_.fetch_add(static_cast<uint64_t>(ops.size()), std::memory_order_relaxed);
    NoteSyncRequest(options.sync);
    // 所有操作共用同一个 seq — 作为单一原子单元
    const uint64_t seq = last_seq_.fetch_add(1, std::memory_order_relaxed);

    uint64_t wal_index = 0;
    {
        std::lock_guard<WriteLock> wl(write_mu_);

        // v2.0: 大 Value 分离 — 在写 WAL 前先把每个 op 的 value 走 vlog 路径
        // 复用 Put 的 tag 格式（\x00 原值 / \x01 vlog 指针）
        std::vector<WALRecord::BatchOp> prepared;
        prepared.reserve(ops.size());
        auto vlog = vlog_.load(std::memory_order_acquire);
        for (const auto& op : ops) {
            WALRecord::BatchOp p = op;
            if (op.type == WALRecord::Type::kTypeValue && vlog &&
                op.value.size() > options_.value_threshold) {
                uint64_t file_id, offset, length;
                Status vs = vlog->Append(Slice(op.value), &file_id, &offset, &length);
                if (!vs.ok()) return vs;
                p.value.clear();
                p.value.push_back('\x01');
                p.value.append(reinterpret_cast<const char*>(&file_id), 8);
                p.value.append(reinterpret_cast<const char*>(&offset), 8);
                p.value.append(reinterpret_cast<const char*>(&length), 8);
            } else if (op.type == WALRecord::Type::kTypeValue) {
                p.value.insert(p.value.begin(), '\x00');
            }
            prepared.emplace_back(std::move(p));
        }

        // 原子写入：单次 WAL AppendBatch，crash 后要么全部可见要么全部丢弃
        Status s = wal_->AppendBatch(seq, prepared);
        if (!s.ok()) return s;

        // 逐步更新 MemTable（原子性由 WAL 保证；隔离性见设计草案 5.1）
        auto batch_mem = mem_.load(std::memory_order_acquire);
        for (const auto& op : prepared) {
            if (op.type == WALRecord::Type::kTypeValue) {
                batch_mem->Insert(seq, Slice(op.key), Slice(op.value));
            } else if (op.type == WALRecord::Type::kTypeDeletion) {
                batch_mem->InsertDeletion(seq, Slice(op.key));
            } else if (op.type == WALRecord::Type::kTypeRangeDeletion) {
                batch_mem->InsertRangeDeletion(seq, Slice(op.begin_key), Slice(op.end_key));
            }
        }
        wal_index = ++wal_written_;

        // Registered before the lock is dropped so the range deletes are already
        // visible when the batch becomes durable; tomb_mu_ is independent of
        // write_mu_, so this cannot invert the order.
        for (const auto& op : prepared) {
            if (op.type == WALRecord::Type::kTypeRangeDeletion) {
                RegisterRangeTombstone(seq, op.begin_key, op.end_key);
            }
        }
    }

    return FinishWrite(options.sync, wal_index);
}

void DBImpl::NoteSyncRequest(bool need_sync) {
    // Recorded before the record is appended, so a rotation that happens after
    // the append can see it: the rotating thread holds write_mu_, which we hold
    // when the store below becomes visible to it.
    if (need_sync) sync_requested_.store(true, std::memory_order_relaxed);
}

void DBImpl::WaitForWalSyncIdle() {
    std::unique_lock<std::mutex> sl(wal_sync_mu_);
    sync_cv_.wait(sl, [this] { return wal_sync_inflight_ == 0; });
}

Status DBImpl::SyncWALTo(uint64_t wal_index) {
    std::unique_lock<std::mutex> sl(wal_sync_mu_);
    for (;;) {
        // Cheap pass first: an fsync that finished after our append already
        // covers it, and with several writers in flight that is the common case.
        if (wal_synced_ >= wal_index) return Status::OK();
        // Somebody else is already flushing this file. Their fsync cannot cover
        // us (our record is newer than the extent they captured), but waiting
        // for it is what makes this a *group* commit: without the wait, every
        // writer issues its own fsync and the batch is always one.
        if (!syncing_) break;
        sync_cv_.wait(sl);
    }
    syncing_ = true;
    sl.unlock();

    // Pin what this fsync is allowed to claim: the file, its write position and
    // its generation. All three are read under write_mu_, the lock appends and
    // rotations use to move them, and the fsync counter is registered here too,
    // so a Close()/rotation that also holds write_mu_ is ordered either wholly
    // before this registration or wholly after it -- never in the middle.
    WALWriter* target = nullptr;
    uint64_t covered = 0;
    uint64_t gen = 0;
    {
        std::lock_guard<WriteLock> wl(write_mu_);
        std::lock_guard<std::mutex> sl2(wal_sync_mu_);
        target = wal_.get();
        covered = wal_written_;
        gen = wal_gen_;
        ++wal_sync_inflight_;
    }

    // The fsync itself holds no database lock.
    //
    // It used to hold write_mu_ for its whole duration, because "everything
    // appended so far" is only a stable set while no append and no rotation can
    // slip past. True -- but the set was captured above, so the lock is not
    // needed to keep it stable, only to keep it a *snapshot*. Holding it turned
    // group commit into a queue: nobody could append while the disk was busy, so
    // nobody could ever join the next batch, and 8 syncing writers still paid 8
    // fsyncs. Now records that arrive during the flush simply are not covered by
    // it, and one of them leads the next round.
    Status s = target->Sync();

    sl.lock();
    --wal_sync_inflight_;
    syncing_ = false;
    // Publishing the position is only honest while it still describes the file
    // we synced: a rotation in the meantime reset both counters and synced the
    // outgoing file itself (see TriggerFlush), so the records up to `covered`
    // are durable either way, but they are no longer this file's business.
    if (s.ok() && gen == wal_gen_ && covered > wal_synced_) {
        wal_synced_ = covered;
    }
    sl.unlock();
    sync_cv_.notify_all();
    return s;
}

Status DBImpl::FinishWrite(bool need_sync, uint64_t wal_index) {
    if (need_sync) {
        Status s = SyncWALTo(wal_index);
        if (!s.ok()) return s;
    }

    // Every 1024th write: free space, then memtable size.
    //
    // Both used to run under mutex_, so the writer that happened to cross the
    // boundary put a statfs syscall (and a possible flush) inside the section
    // every other writer was queued on. CheckDiskSpace touches no shared state
    // at all, and the memtable decision needs mutex_ for the length of one
    // comparison.
    const uint32_t ordinal = write_count_.fetch_add(1, std::memory_order_relaxed) + 1;
    if ((ordinal & 1023) == 0) {
        Status s = CheckDiskSpace();
        if (!s.ok()) return s;

        std::lock_guard<std::mutex> lock(mutex_);
        if (mem_.load(std::memory_order_acquire)->ApproximateMemoryUsage() >
            options_.memtable_size) {
            // A flush that gave up leaves has_imm_ set for good, so no rotation
            // can ever happen and this memtable would grow until the process ran
            // out of memory. Refuse the write instead: the records already
            // accepted are safe in the WAL, and a caller that can still see the
            // error is better off than one that gets OOM-killed.
            if (has_imm_ && flush_attempts_ >= kMaxFlushAttempts) {
                return Status::IOError("write refused: background flush failed: " +
                                       std::string(flush_error_.ToString()));
            }
            TriggerFlush();
        }
    }

    return Status::OK();
}

void DBImpl::RegisterRangeTombstone(uint64_t seq, const Slice& begin_key, const Slice& end_key) {
    std::unique_lock<std::shared_mutex> lock(tomb_mu_);
    range_tombstones_.push_back({begin_key.ToString(), end_key.ToString(), seq});
}

void DBImpl::LoadRangeTombstonesFromSSTables() {
    // A flushed tombstone is an entry whose user key is "\xff\xff\xff\xff" +
    // begin_key. 0xFF is the largest byte, so all of them sort after every
    // ordinary key: one seek into that prefix reaches the first tombstone, and
    // the rest follow contiguously. No full-table scan is needed.
    static const char kMarker[4] = {'\xff', '\xff', '\xff', '\xff'};
    const Slice marker(kMarker, 4);
    const std::string seek_target = SeekInternalKey(marker);

    for (int level = 0; level < kNumLevels; ++level) {
        for (const auto& table : levels_[level]) {
            auto iter = table->NewIterator();
            iter.Seek(Slice(seek_target));
            while (iter.Valid()) {
                Slice user_key = iter.key();
                if (user_key.size() < 4 ||
                    std::memcmp(user_key.data(), kMarker, 4) != 0) {
                    break;  // past the tombstone run
                }
                RegisterRangeTombstone(iter.seq(),
                                       Slice(user_key.data() + 4, user_key.size() - 4),
                                       iter.value());
                iter.Next();
            }
        }
    }
}

bool DBImpl::CoveredByRangeTombstone(const Slice& key, uint64_t seq, uint64_t snapshot) const {
    std::shared_lock<std::shared_mutex> lock(tomb_mu_);
    for (const auto& t : range_tombstones_) {
        // The tombstone has to be BOTH newer than this version (a write after the
        // DELRANGE stays live) and visible to the snapshot (a DELRANGE issued
        // after the snapshot was taken must not hide anything from it).
        if (t.seq <= seq || t.seq > snapshot) continue;
        if (!(key < Slice(t.begin_key)) && key < Slice(t.end_key)) return true;
    }
    return false;
}

Status DBImpl::Get(const ReadOptions& options, const Slice& key, std::string* value) {
    stats_reads_.fetch_add(1, std::memory_order_relaxed);

    // Snapshot reads are supported: SSTables persist a sequence number per entry
    // (LevelDB-style internal keys), so the version visible at snapshot_seq can
    // be identified in the memtable and in every SSTable.
    const uint64_t snapshot = options.snapshot_seq > 0
                                  ? options.snapshot_seq
                                  : last_seq_.load(std::memory_order_acquire);

    // Pin the manager for the whole lookup. Clear() replaces vlog_ under
    // mutex_, which a reader does not hold -- an atomic load is what keeps the
    // object alive while this call resolves a large-value pointer.
    auto vlog = vlog_.load(std::memory_order_acquire);

    // v2.0: tagged value decoder — unwraps vlog pointer (\x01...) or strips inline tag (\x00...)
    // Returns OK and leaves `value` untouched if not found (caller checks Status).
    auto unwrap_vlog = [&vlog](std::string* v) -> Status {
        if (v->empty()) return Status::OK();
        char tag = (*v)[0];
        if (tag == '\x00') {
            v->erase(0, 1);  // strip inline tag
            return Status::OK();
        }
        if (tag == '\x01' && v->size() == 1 + 24) {
            const char* p = v->data() + 1;
            uint64_t file_id, offset, length;
            ::memcpy(&file_id, p, 8);
            ::memcpy(&offset,   p + 8, 8);
            ::memcpy(&length,   p + 16, 8);
            if (!vlog) return Status::Corruption("vlog pointer without manager");
            std::string raw;
            Status s = vlog->Read(file_id, offset, length, &raw);
            if (!s.ok()) return s;
            *v = std::move(raw);
            return Status::OK();
        }
        // Unknown tag → treat as legacy value without tag
        return Status::OK();
    };

    // Hold shared_lock for the entire read to ensure snapshot consistency
    std::shared_lock<std::shared_mutex> read_lock(rw_mutex_);

    // Load once and use the pinned handle for both memtables. Reading through
    // mem_/imm_ twice could straddle a flush and look at two different tables.
    //
    // mem_ and imm_ are strictly newer than everything in levels_, so a hit in
    // either is authoritative: it decides the key outright, including when the
    // newest version is a tombstone. Falling through on a tombstone is what made
    // a deleted key reappear from the SSTable that still holds the old value.
    auto mem = mem_.load(std::memory_order_acquire);
    auto imm = imm_.load(std::memory_order_acquire);
    const std::shared_ptr<MemTable> tables[2] = {mem, imm};
    for (const auto& table : tables) {
        if (!table) continue;
        uint64_t seq = 0;
        MemTable::Lookup lr = table->Find(key, value, snapshot, &seq);
        if (lr == MemTable::Lookup::NotFound) continue;
        if (lr == MemTable::Lookup::Deleted) return Status::NotFound();
        // A DELRANGE recorded outside this memtable (or already flushed to an
        // SSTable) still shadows the version we just found.
        if (CoveredByRangeTombstone(key, seq, snapshot)) return Status::NotFound();
        return unwrap_vlog(value);
    }

    // Search SSTable levels under the same lock.
    //
    // L0 files may overlap, so a key can be present in several of them. Each file
    // yields its own newest visible version; the winner is the one with the
    // highest sequence number. (Newest file first is a cheap short-circuit: the
    // first hit is very often the winner.)
    bool found = false;
    uint64_t best_seq = 0;
    uint8_t best_type = kTypeValue;
    std::string best_value;

    // Returns a non-OK status only for real trouble (a damaged file). NotFound is
    // not an error here -- it just means this file does not hold the key.
    auto consider = [&](const std::shared_ptr<SSTable>& table) -> Status {
        if (!table->MayMatch(key)) return Status::OK();
        uint64_t s = 0;
        uint8_t t = kTypeValue;
        std::string v;
        Status ts = table->Get(key, snapshot, &v, &s, &t);
        // "This file does not hold the key" is an ordinary miss, not damage: a
        // Bloom false positive lands here on a file that merely might match, and
        // returning it as fatal aborted the whole lookup before the file that
        // really holds the value was ever consulted.
        if (ts.IsNotFound()) return Status::OK();
        if (!ts.ok()) return ts;
        if (found && s <= best_seq) return Status::OK();
        found = true;
        best_seq = s;
        best_type = t;
        best_value = std::move(v);
        return Status::OK();
    };

    for (auto it = levels_[0].rbegin(); it != levels_[0].rend(); ++it) {
        // Propagate damage instead of skipping the file: a read that cannot
        // tell "absent" from "unreadable" will report data loss for a disk
        // problem, which is the opposite of what the caller needs.
        Status cs = consider(*it);
        if (!cs.ok()) return cs;
        if (found && best_seq == last_seq_.load(std::memory_order_acquire)) break;
    }

    // L1+ files are non-overlapping and sorted — binary search
    for (int level = 1; level < kNumLevels; ++level) {
        const auto& files = levels_[level];
        if (files.empty()) continue;
        // Binary search: find first file whose LargestKey() >= key
        auto it = std::lower_bound(files.begin(), files.end(), key,
            [](const std::shared_ptr<SSTable>& f, const Slice& k) {
                return Slice(f->LargestKey()) < k;
            });
        if (it != files.end() && (*it)->SmallestKey().compare(key.ToString()) <= 0) {
            Status cs = consider(*it);
            if (!cs.ok()) return cs;
        }
    }

    if (!found || best_type == kTypeDeletion) {
        return Status::NotFound();
    }

    // The SSTable path above finds point versions only. A DELRANGE that has
    // already been flushed leaves a synthetic entry a point lookup never reaches,
    // so without this check a deleted key comes back as live.
    if (CoveredByRangeTombstone(key, best_seq, snapshot)) {
        return Status::NotFound();
    }

    *value = std::move(best_value);
    return unwrap_vlog(value);
}

bool DBImpl::Exists(const ReadOptions& options, const Slice& key) {
    std::string value;
    return Get(options, key, &value).ok();
}

Status DBImpl::Scan(const ReadOptions& options, const Slice& prefix, int limit,
                    std::vector<std::pair<std::string, std::string>>* results) {
    uint64_t snapshot = options.snapshot_seq > 0
                            ? options.snapshot_seq
                            : last_seq_.load(std::memory_order_acquire);
    Iterator iter(this, snapshot);
    auto vlog = vlog_.load(std::memory_order_acquire);

    if (prefix.size() == 0) {
        iter.SeekToFirst();
    } else {
        iter.Seek(prefix);
    }

    while (iter.Valid() && static_cast<int>(results->size()) < limit) {
        std::string key(iter.key().data(), iter.key().size());
        // Check prefix match
        if (prefix.size() > 0 && key.compare(0, prefix.size(), prefix.data(), prefix.size()) != 0) {
            break;
        }
        std::string value(iter.value().data(), iter.value().size());
        // v2.0: unwrap vlog tag on Scan出口（与 Get 路径保持一致）
        //   \x00 + 原值       → strip tag
        //   \x01 + 24B 指针    → 从 vlog 读出真实 value
        //   空串/无 tag        → 原样返回（兼容历史数据）
        if (!value.empty()) {
            char tag = value[0];
            if (tag == '\x00') {
                value.erase(0, 1);
            } else if (tag == '\x01' && value.size() == 1 + 24 && vlog) {
                const char* p = value.data() + 1;
                uint64_t file_id, offset, length;
                ::memcpy(&file_id, p, 8);
                ::memcpy(&offset,   p + 8, 8);
                ::memcpy(&length,   p + 16, 8);
                std::string raw;
                if (vlog->Read(file_id, offset, length, &raw).ok()) {
                    value = std::move(raw);
                }
            }
        }
        results->emplace_back(std::move(key), std::move(value));
        iter.Next();
    }

    return Status::OK();
}

Status DBImpl::Increment(const WriteOptions& options, const Slice& key, int64_t delta, int64_t* new_value) {
    std::string current_value;
    Status s = Get(ReadOptions(), key, &current_value);

    int64_t current = 0;
    if (s.ok()) {
        try {
            current = std::stoll(current_value);
        } catch (...) {
            return Status::InvalidArgument("value is not an integer");
        }
    } else if (!s.IsNotFound()) {
        return s;
    }

    *new_value = current + delta;
    return Put(options, key, std::to_string(*new_value));
}

Status DBImpl::Rename(const WriteOptions& options, const Slice& src, const Slice& dst) {
    std::string value;
    Status s = Get(ReadOptions(), src, &value);
    if (!s.ok()) return s;

    s = Put(options, dst, value);
    if (!s.ok()) return s;

    return Delete(options, src);
}

DBStats DBImpl::GetStats() const {
    DBStats stats;
    stats.total_writes = stats_writes_.load(std::memory_order_relaxed);
    stats.total_reads = stats_reads_.load(std::memory_order_relaxed);
    stats.total_deletes = stats_deletes_.load(std::memory_order_relaxed);
    stats.total_flushes = stats_flushes_.load(std::memory_order_relaxed);
    stats.total_compactions = stats_compactions_.load(std::memory_order_relaxed);
    {
        std::shared_lock<std::shared_mutex> lock(rw_mutex_);
        stats.pending_deletes = pending_delete_.size();
        auto mem = mem_.load(std::memory_order_acquire);
        auto imm = imm_.load(std::memory_order_acquire);
        if (mem) stats.memtable_size = mem->ApproximateMemoryUsage();
        if (imm) stats.imm_size = imm->ApproximateMemoryUsage();
        for (int i = 0; i < kNumLevels; ++i) {
            for (const auto& f : levels_[i]) {
                stats.level_sizes[i] += f->FileSize();
            }
        }
    }
    return stats;
}

bool DBImpl::WaitForBackgroundIdle(int timeout_ms) {
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(timeout_ms);
    for (;;) {
        {
            // Both flags are atomic, but read them under mutex_ anyway: the two
            // are cleared together in that lock, and a probe that caught the
            // half-open window would report "idle" one instant before a
            // compaction round is queued.
            std::lock_guard<std::mutex> lock(mutex_);
            if (!has_imm_ && !bg_scheduled_) return true;
        }
        if (std::chrono::steady_clock::now() >= deadline) return false;
        // Poll rather than wait on a condition variable: "idle" is the absence
        // of two independent flags and both are cleared on paths that do not
        // signal any single cv. A 2 ms poll is cheap next to the fsync this is
        // waiting on.
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
}

uint64_t DBImpl::GetOldestSnapshot() const {
    std::lock_guard<std::mutex> lock(snapshot_mutex_);
    if (active_snapshots_.empty()) return UINT64_MAX;
    return active_snapshots_.front();  // already sorted
}

Status DBImpl::SearchSSTable(int level, const Slice& key, std::string* value,
                            uint64_t snapshot_seq) const {
    // level arrives as a parameter and indexes a fixed-size array.
    if (level < 0 || level >= kNumLevels) {
        return Status::InvalidArgument("level " + std::to_string(level) + " out of range");
    }
    std::shared_lock<std::shared_mutex> lock(rw_mutex_);
    const auto& files = levels_[level];
    const uint64_t snap = snapshot_seq != 0 ? snapshot_seq
                                            : last_seq_.load(std::memory_order_acquire);
    bool found = false;
    uint64_t best_seq = 0;
    uint8_t best_type = kTypeValue;
    std::string best_value;

    for (const auto& table : files) {
        if (!table->MayMatch(key)) continue;
        uint64_t seq = 0;
        uint8_t type = kTypeValue;
        std::string v;
        if (!table->Get(key, snap, &v, &seq, &type).ok()) continue;
        if (found && seq <= best_seq) continue;
        found = true;
        best_seq = seq;
        best_type = type;
        best_value = std::move(v);
    }

    if (!found || best_type == kTypeDeletion) {
        return Status::NotFound();
    }
    *value = std::move(best_value);
    return Status::OK();
}

Status DBImpl::CheckDiskSpace() const {
    platform_statvfs_t vfs;
    if (platform_statvfs(options_.db_path.c_str(), &vfs) < 0) {
        return Status::IOError("cannot stat filesystem");
    }

    uint64_t free_bytes = static_cast<uint64_t>(vfs.f_bavail) * vfs.f_frsize;
    uint64_t memtable_bytes = mem_.load(std::memory_order_acquire)->ApproximateMemoryUsage();

    // Need at least memtable_size * 2 free space (one Flush + one Compaction)
    if (free_bytes < memtable_bytes * 2) {
        return Status::IOError("insufficient disk space");
    }

    return Status::OK();
}

// Hold a writer off while L0 holds more than compaction can drain.
//
// Past the slow threshold a writer pauses and re-checks; past the stop
// threshold it keeps waiting for the background compaction to clear the
// backlog. The wait is a bounded poll rather than a condition-variable wait:
// waking on compaction_cv_ depends on every completion path signalling, and a
// single missed notify turns into a writer parked for its whole timeout -- and
// if the wait were unbounded, into a hang. Polling costs at most one extra
// millisecond per round and cannot deadlock.
Status DBImpl::WaitForCompactionCatchUp() {
    const size_t trigger = options_.l0_file_num_trigger;
    if (trigger == 0) return Status::OK();

    const size_t slow = options_.l0_slowdown_trigger ? options_.l0_slowdown_trigger
                                                      : trigger;
    const size_t stop = options_.l0_stop_trigger ? options_.l0_stop_trigger
                                                  : trigger * 2;

    // Only the stop threshold is worth a real wait; past it we hold the writer
    // until the background compaction drains the backlog. Bounded, so a wedged
    // background thread costs latency rather than deadlocking every writer.
    const auto deadline =
        std::chrono::steady_clock::now() +
        std::chrono::milliseconds(options_.compaction_backpressure_timeout_ms);

    bool counted_slow = false;
    // Rounds to allow the scheduler to pick up the compaction before deciding
    // nobody is going to. Kept short on purpose: a compaction of real files does
    // not finish in a couple of milliseconds, so longer polling just multiplies
    // the wait onto every write for no throughput gained.
    int idle_rounds_ = 0;
    constexpr int kIdleRoundsBeforeGivingUp = 5;
    // Last time THIS writer actually paused for the slow threshold. Thread-local
    // and outliving the call: a plain local would be re-initialised every time,
    // so every write would still pause.
    static thread_local std::chrono::steady_clock::time_point last_slowdown{};
    for (;;) {
        bool need_schedule = false;
        bool stopped = false;
        // levels_ belongs to rw_mutex_, not to mutex_: reading its size under the
        // writer lock was a plain data race against a flush pushing a file in
        // (vector growth can reallocate while we read size()).
        size_t l0_files = 0;
        {
            std::shared_lock<std::shared_mutex> rl(rw_mutex_);
            l0_files = levels_[0].size();
        }
        if (l0_files < slow) return Status::OK();

        {
            std::lock_guard<std::mutex> lock(mutex_);
            stopped = (l0_files >= stop);
            if (stopped) {
                ++stalled_writes_;
                // Nothing scheduled means nothing is draining the backlog.
                need_schedule = !bg_scheduled_;
            } else if (!counted_slow) {
                ++slow_writes_;
                counted_slow = true;
            }
        }

        // Outside the lock on purpose: MaybeScheduleCompaction takes mutex_
        // itself, so calling it while already holding it self-deadlocks on a
        // non-recursive mutex.
        if (need_schedule) MaybeScheduleCompaction();

if (!stopped) {
            // This is the "slow down" case, not "stop": pause briefly and let the
            // write through. Waiting out the whole budget here would add that
            // budget to EVERY write while L0 merely sat over the slow
            // threshold -- mild pressure turning into a throughput collapse,
            // which is the opposite of what backpressure is for.
            //
            // Throttle by wall clock, not per write. Sleeping 1ms inside a
            // write loop costs 1ms of every write; a steady 200k writes/s
            // collapses to under 500. Requiring a minimum gap between pauses
            // keeps the signal while letting a burst pass at full speed.
            const auto now = std::chrono::steady_clock::now();
            if (now - last_slowdown >= std::chrono::microseconds(500)) {
                last_slowdown = now;
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            return Status::OK();
        }

        if (std::chrono::steady_clock::now() >= deadline) {
            // Do not fail the write: dropping a Put because compaction is slow
            // is worse than letting L0 grow a little further. The next writer
            // re-checks, so a persistently stuck compaction surfaces as
            // permanently slow writes rather than as data loss.
            return Status::OK();
        }

        // Parked, but is anyone actually draining? Give the scheduler a few
        // rounds to pick the work up, then stop waiting. Sitting out the whole
        // budget with nothing scheduled just converts a background problem into
        // front-end latency on every single write.
        if (++idle_rounds_ > kIdleRoundsBeforeGivingUp) {
            bool running;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                running = bg_scheduled_;
            }
            if (!running) return Status::OK();
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}
void DBImpl::TriggerFlush() {
    // Note: caller must hold mutex_
    if (has_imm_) return;

    {
        // The record/memtable pairing is decided right here. Writers hold
        // write_mu_ for the append+insert pair, so swapping both under it means
        // a record can never land in the new memtable while its bytes went to the
        // old WAL -- which is how a flush ends up deleting a log that still held
        // unflushed records.
        std::lock_guard<WriteLock> wl(write_mu_);

        // Publish the new immutable table before clearing the active one, so a
        // reader that loads the two pointers separately can never see neither.
        imm_.store(mem_.load(std::memory_order_acquire), std::memory_order_release);
        has_imm_ = true;
        // A different memtable is on the block now, so the previous one's failure
        // count and error say nothing about it.
        flush_attempts_ = 0;
        flush_error_ = Status::OK();
        mem_.store(std::make_shared<MemTable>(), std::memory_order_release);

        // Rotate the WAL together with the memtable.
        //
        // One WAL per memtable is what makes FlushMemTable safe: it can drop the
        // file belonging to imm_ as soon as imm_ is durable, because writes that
        // arrived during the flush went to the NEW WAL and the new memtable.
        // Sharing a single file meant the flush truncated the records of writes
        // that were only in memory, so a crash lost writes Put had already
        // returned OK for.
        if (wal_) {
            std::string next_path = options_.db_path + "/wal-" +
                                    std::to_string(next_wal_id_++) + ".log";
            auto next = std::make_unique<WALWriter>(next_path);
            if (next->Open().ok()) {
                // Force the outgoing file out first. A writer with sync=true may
                // be about to find its record rotated out from under it, and
                // since it can no longer fsync this file itself, this is what
                // lets it still answer "durable" instead of lying or hanging.
                // Skipped entirely when nothing has ever asked for sync.
                if (sync_requested_.load(std::memory_order_relaxed)) wal_->Sync();

                imm_wal_ = std::move(wal_);
                imm_wal_path_ = wal_path_;
                wal_ = std::move(next);
                wal_path_ = next_path;

                // Both counters belong to the file, not to the database, and
                // the generation tells a sync that started against the outgoing
                // file not to publish its position for this one.
                {
                    std::lock_guard<std::mutex> sl(wal_sync_mu_);
                    wal_synced_ = 0;
                    wal_written_ = 0;
                    ++wal_gen_;
                }
            }
            // If the new file cannot be created we keep appending to the current
            // one. Not rotating means not truncating, so nothing acknowledged is
            // lost -- the WAL just keeps growing until the next successful
            // rotation.
        }
    }

    bg_cv_.notify_one();
    bg_scheduled_ = true;
}

void DBImpl::FlushMemTable() {
    stats_flushes_.fetch_add(1, std::memory_order_relaxed);

    std::shared_ptr<MemTable> mem_to_flush;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!has_imm_) return;
        mem_to_flush = imm_.load(std::memory_order_acquire);
        // A retry starts here: clear the previous attempt's error so a waiter
        // cannot read a stale one while this attempt is still in flight.
        flush_error_ = Status::OK();
    }

    if (!mem_to_flush) {
        // has_imm_ set with no imm_ to show for it is bookkeeping corruption,
        // not something to spin on. Clear the flag so the background thread and
        // every waiter can make progress; there is nothing to lose because there
        // is nothing there.
        std::fprintf(stderr,
                     "[LightKV] WARNING: has_imm_ was set with no immutable memtable; "
                     "clearing the flag\n");
        std::lock_guard<std::mutex> lock(mutex_);
        has_imm_ = false;
        bg_scheduled_ = false;
        flush_attempts_ = 0;
        flush_cv_.notify_all();
        return;
    }

    uint64_t file_id = 0;
    Status s = WriteLevel0Table(mem_to_flush, &file_id);
    if (!s.ok()) {
        OnFlushFailed(s);
        return;
    }

    {
        std::lock_guard<std::mutex> lock(mutex_);
        imm_.store(nullptr, std::memory_order_release);
        has_imm_ = false;
        bg_scheduled_ = false;
        flush_attempts_ = 0;
        flush_error_ = Status::OK();

        // imm_ is in an SSTable now, so the WAL that carried it is redundant.
        // Only THIS file is dropped: the active WAL keeps the records written
        // while the flush was running, and it is not touched here.
        //
        // A sync registered against this file before it was rotated out may still
        // be running (it holds no database lock while it fsyncs), so let it
        // finish first. No new registration can name this file: a sync captures
        // wal_, and this one is imm_wal_ from the moment it was rotated.
        WaitForWalSyncIdle();
        if (imm_wal_) {
            imm_wal_->Close();
            imm_wal_.reset();
        }
        if (!imm_wal_path_.empty()) {
            std::error_code ec;
            std::filesystem::remove(imm_wal_path_, ec);
            imm_wal_path_.clear();
        }

        flush_cv_.notify_all();
    }
}

void DBImpl::OnFlushFailed(const Status& s) {
    int attempts = 0;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        attempts = ++flush_attempts_;
        flush_error_ = s;

        // has_imm_ deliberately stays TRUE. The memtable's records exist only in
        // memory and in imm_wal_, so clearing the flag would tell every reader
        // the data is durable, let the next rotation reuse the slot and drop the
        // pointer -- data loss dressed up as error handling.
        //
        // What must NOT stay quiet is everyone waiting on this flush. The old
        // code returned here without notifying, which left FlushForBackup and
        // Clear blocked on a predicate (!has_imm_) that could never become true,
        // while BackgroundWork re-entered FlushMemTable in a tight loop because
        // its wake predicate (has_imm_) was instantly true -- a core pinned at
        // 100% for the rest of the process's life, and a destructor that could
        // never join.
        flush_cv_.notify_all();

        if (attempts >= kMaxFlushAttempts) {
            // Give up rather than retry forever. Nothing acknowledged is lost:
            // FlushMemTable only unlinks imm_wal_ on success, so the records are
            // still on disk and the next Open() replays them. What we refuse to
            // do is keep the process alive in a state where the active memtable
            // can never rotate (FinishWrite starts rejecting writes for exactly
            // that reason).
            bg_scheduled_ = false;
            std::fprintf(stderr,
                         "[LightKV] FATAL: memtable flush failed %d times, giving up: %s\n"
                         "          Acknowledged writes are still in the WAL and will be\n"
                         "          replayed on the next open, but this process can no\n"
                         "          longer accept writes.\n",
                         attempts, std::string(s.ToString()).c_str());
            return;
        }
    }

    // Back off outside the lock: a broken path that is retried with no delay
    // turns this thread into a spin loop, and writers must never queue behind a
    // sleep they have nothing to do with.
    std::this_thread::sleep_for(std::chrono::milliseconds(50 * attempts));
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
    // fetch_add, not ++: a Backup-driven flush can allocate an id on the
    // caller's thread at the same moment the background thread compacts. Two
    // callers reading the same value meant two tables written to one path.
    *file_id = next_file_id_.fetch_add(1, std::memory_order_relaxed);
    std::stringstream ss;
    ss << options_.db_path << "/" << *file_id << ".sst";
    std::string fname = ss.str();

    TableBuilder builder(options_, fname);

    // Collect entries and sort them into internal-key order:
    //   user_key ASC, then seq DESC (newest first), then type DESC.
    //
    // The memtable's own iteration order is NOT relied upon. It happens to be
    // newest-first per key because SkipList::Insert splices new nodes in front
    // of existing duplicates, but that is an undocumented accident — sorting
    // here makes the on-disk order an explicit, checked invariant instead.
    struct Entry {
        std::string internal_key;
        std::string value;
    };
    std::vector<Entry> entries;
    entries.reserve(1024);

    auto iter = mem->SeekToFirst();
    while (iter.Valid()) {
        const uint8_t type = iter.IsDeleted() ? kTypeDeletion : kTypeValue;
        entries.push_back({MakeInternalKey(Slice(iter.key()), iter.seq(), type),
                           iter.value()});
        iter.Next();
    }

    // Range tombstones are stored as synthetic entries with key
    // "\xff\xff\xff\xff" + begin_key and value = end_key.
    for (const auto& tombstone : mem->GetRangeTombstones()) {
        std::string tombstone_key;
        tombstone_key.append("\xff\xff\xff\xff", 4);
        tombstone_key.append(tombstone.begin_key);
        entries.push_back({MakeInternalKey(Slice(tombstone_key), tombstone.seq, kTypeValue),
                           tombstone.end_key});
    }

    // Sort with the internal-key comparator, not plain byte order: one user key
    // can be a prefix of another ("key1" / "key10"), and the trailer bytes would
    // otherwise invert the intended user-key ordering.
    std::sort(entries.begin(), entries.end(),
              [](const Entry& a, const Entry& b) {
                  return CompareInternalKeys(Slice(a.internal_key),
                                             Slice(b.internal_key)) < 0;
              });

    for (const auto& e : entries) {
        const uint8_t type = ExtractType(Slice(e.internal_key));
        Status add_status = builder.Add(ExtractUserKey(Slice(e.internal_key)), Slice(e.value),
                                        ExtractSeq(Slice(e.internal_key)), type);
        if (!add_status.ok()) {
            // A partially written table must not be installed in the manifest.
            builder.Abandon();
            return add_status;
        }
    }

    Status fin = builder.Finish();
    if (!fin.ok()) {
        builder.Abandon();
        return fin;
    }

    auto table = std::make_shared<SSTable>(options_, fname, *file_id);
    auto s = table->Open();
    if (!s.ok()) {
        // Finish() already closed the file, so Abandon() is not in play. The
        // table is on disk, unusable, and in no manifest: file ids are never
        // reused, so nothing would ever point at it or delete it again.
        std::remove(fname.c_str());
        return s;
    }

    // Key range for the manifest, in USER keys (trailers stripped). The entries
    // vector is already sorted by internal key, so its first and last user keys
    // are the bounds.
    std::string smallest, largest;
    if (!entries.empty()) {
        smallest = ExtractUserKey(Slice(entries.front().internal_key)).ToString();
        largest = ExtractUserKey(Slice(entries.back().internal_key)).ToString();
    }

    {
        std::unique_lock<std::shared_mutex> lock(rw_mutex_);
        levels_[0].push_back(std::move(table));
    }

    {
        // The manifest is edited from here and from the compaction path, and
        // UpdateManifest appends to the same file. Both need the same lock, or a
        // delta can serialise a half-applied edit.
        std::lock_guard<std::mutex> ml(manifest_mu_);
        manifest_.AddFile(0, *file_id, smallest, largest,
                          std::filesystem::file_size(fname));

        // Everything at or below this seq is now on disk. Record it so a consumer
        // (Backup) can tell exactly where the durable prefix ends:
        // UpdateManifest() below overwrites last_seq with the live counter, which
        // runs ahead of the entries this table actually contains.
        manifest_.flushed_seq = mem->MaxSeq();
    }
    const uint64_t prev_flushed_seq = flushed_seq_.load(std::memory_order_acquire);
    flushed_seq_.store(mem->MaxSeq(), std::memory_order_release);

    Status ms = UpdateManifest();
    if (!ms.ok()) {
        // The table is complete, but nothing on disk names it: the manifest
        // still describes the file set from before this flush. Two things must
        // not happen next.
        //
        // First, this must not count as a successful flush. FlushMemTable unlinks
        // imm_wal_ on success, and while the manifest does not name the table the
        // WAL is the only copy of these records that a restart would find.
        //
        // Second, the table must not stay registered. The retry would write a
        // second table holding the same records, and file ids are never reused,
        // so nothing could ever collect the first one -- every attempt would
        // strand one more file.
        {
            std::unique_lock<std::shared_mutex> lock(rw_mutex_);
            auto& l0 = levels_[0];
            l0.erase(std::remove_if(l0.begin(), l0.end(),
                                    [id = *file_id](const std::shared_ptr<SSTable>& f) {
                                        return f->file_id() == id;
                                    }),
                     l0.end());
            // Same path as a compacted-away input: an iterator may still hold the
            // file open, and unlinking it under a live mapping fails on Windows.
            pending_delete_.push_back(fname);
        }
        {
            std::lock_guard<std::mutex> ml(manifest_mu_);
            manifest_.RemoveFile(0, *file_id);
        }
        CleanupDeletedFiles();
        // The durable prefix did not move: those records are back to being
        // covered by the WAL only.
        flushed_seq_.store(prev_flushed_seq, std::memory_order_release);
        return Status::IOError("flush: cannot persist MANIFEST: " + ms.ToString());
    }

    MaybeScheduleCompaction();

    return Status::OK();
}

void DBImpl::MaybeScheduleCompaction() {
    // A manifest that could not be persisted means the on-disk file set is
    // behind this process. Compaction not only cannot retire its inputs in that
    // state (they are what the manifest still names), it also cannot make
    // progress: every round would merge more files into outputs that only memory
    // knows about, and the orphan count grows with each one. Wait for the next
    // successful UpdateManifest -- a flush always calls it, so one memtable
    // rotation repairs this without any new retry machinery.
    if (manifest_dirty_.load(std::memory_order_acquire)) return;

    // Cheap pre-check first: with a round already queued there is nothing to
    // decide, and PickCompaction walks every level.
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (bg_scheduled_ || has_imm_) return;
    }

    // PickCompaction takes rw_mutex_ shared on its own. It must be called
    // WITHOUT mutex_ held: rw_mutex_ is only ever acquired before mutex_, never
    // after (see the contract in db_impl.h), and PickCompaction here runs on the
    // writer path where mutex_ is already hot.
    auto score = PickCompaction();
    if (!score.needs_compaction()) return;

    std::lock_guard<std::mutex> lock(mutex_);
    if (bg_scheduled_ || has_imm_) return;
    bg_scheduled_ = true;
    bg_cv_.notify_one();
}

int DBImpl::EffectiveMaxLevel() const {
    // Two is the floor: with fewer levels there is nowhere for L1 to compact to
    // and the loop bound below would go empty, quietly disabling compaction.
    if (options_.max_level < 2) return 2;
    if (options_.max_level > static_cast<size_t>(kNumLevels)) return kNumLevels;
    return static_cast<int>(options_.max_level);
}

CompactionScore DBImpl::PickCompaction() {
    // levels_ is guarded by rw_mutex_; the caller does not hold it (and must not,
    // it would invert the lock order), so take it here. Every access to levels_
    // below is under this lock.
    std::shared_lock<std::shared_mutex> lock(rw_mutex_);

    // Check L0: file count.
    //
    // The comparison is `>=`, so a level holding exactly the configured number
    // of files is over budget and scores exactly 1.0 -- see
    // CompactionScore::needs_compaction, which treats 1.0 as "go". Using `>` here
    // would make l0_file_num_trigger mean N+1.
    if (levels_[0].size() >= options_.l0_file_num_trigger) {
        return {0, static_cast<double>(levels_[0].size()) / options_.l0_file_num_trigger};
    }

    // Check L1-L6: total size vs target.
    //
    // Stops one level short of the deepest: a file in the last level has nowhere
    // to go, and asking for a level below it wrote one past the end of levels_.
    //
    // The bound is kNumLevels, NOT options_.max_level. levels_ is a fixed-size
    // array; max_level comes from the config file and a value of 9 walked this
    // loop straight into levels_[7] and levels_[8]. Any level index has to be
    // clamped to the array size before it is used, and Initialize() clamps the
    // option as well so the two agree everywhere else.
    uint64_t base_size = static_cast<uint64_t>(options_.memtable_size) * options_.level_multiplier;
    uint64_t cumulative = base_size;
    const int max_level = EffectiveMaxLevel();
    for (int i = 1; i + 1 < max_level; ++i) {
        uint64_t level_size = 0;
        for (const auto& file : levels_[i]) {
            // Approximate size from file ID tracking, or just use number of files
            level_size += file->FileSize();
        }
        uint64_t target_size = cumulative * options_.level_multiplier;
        if (level_size > target_size) {
            return {i, static_cast<double>(level_size) / target_size};
        }
        // Carry the TARGET forward, not what the level actually holds. Adding
        // the observed size let every deeper level's budget grow with the data
        // already stored, so a level sitting well over its fair share stopped
        // looking like it -- compaction drifted towards never running.
        cumulative = target_size;
    }

    return {-1, 0.0};
}

Status DBImpl::DoLevel0Compaction() {
    // Pick a bounded BATCH of L0 files, not all of them.
    //
    // Merging the whole level made the total work quadratic: with N files piled
    // up, each round re-read everything still down there, so the bytes touched
    // went N + (N-1) + ... and a level that momentarily ran behind -- exactly
    // what backpressure reacts to -- then took ever longer to drain, which is how
    // it failed to keep up in the first place.
    //
    // levels_[0] is in flush order, so taking the OLDEST prefix both bounds the
    // round and pushes the stalest data down first. Anything left behind stays
    // in the level for the next round.
    std::vector<CompactionWorker::InputFile> inputs;
    std::string l0_min_key, l0_max_key;
    size_t l0_taken = 0;

    {
        std::unique_lock<std::shared_mutex> lock(rw_mutex_);
        if (levels_[0].empty()) return Status::OK();

        size_t batch = options_.l0_compaction_max_files;
        if (batch == 0) batch = options_.l0_file_num_trigger * 4;
        if (batch < 4) batch = 4;

        // Always drain at least half. Below that the extra rounds cost more in
        // setup (worker, output files, manifest update) than the bounded merge
        // saves -- measured flat, in fact slower. Half keeps the amortised total
        // linear in bytes written without splitting the work needlessly.
        size_t take = levels_[0].size();
        if (take > batch) take = std::max(batch, take / 2);
        l0_taken = take;

        for (size_t i = 0; i < take; ++i) {
            const auto& file = levels_[0][i];
            if (i == 0) {
                l0_min_key = file->SmallestKey();
                l0_max_key = file->LargestKey();
            } else {
                if (!file->SmallestKey().empty() && file->SmallestKey() < l0_min_key)
                    l0_min_key = file->SmallestKey();
                if (!file->LargestKey().empty() && file->LargestKey() > l0_max_key)
                    l0_max_key = file->LargestKey();
            }
            inputs.push_back({0, file});
        }

        // Find overlapping L1 files
        if (!levels_[1].empty() && !l0_min_key.empty()) {
            for (auto& file : levels_[1]) {
                // Check if key ranges overlap
                if (file->LargestKey() >= l0_min_key && file->SmallestKey() <= l0_max_key) {
                    inputs.push_back({1, file});
                }
            }
        }

        // Deliberately NOT removed here. The merge runs with rw_mutex_ released,
        // and a file that has left levels_ but is not yet in the output is
        // invisible to every reader in between -- Get answers NotFound for data
        // that is still sitting on disk. They are detached after the merge
        // succeeds, below.
    }

    // Do the compaction (no lock needed - working with local copies)
    std::vector<uint64_t> new_file_ids;
    CompactionWorker worker(options_, options_.db_path, &next_file_id_);
    worker.SetOldestSnapshot(GetOldestSnapshot());
    worker.SetSnapshotProvider([this]() { return GetOldestSnapshot(); });
    worker.SetRateLimiter(compaction_rate_limiter_.get());  // v2.0 限速
    auto s = worker.DoCompaction(inputs, 1, &new_file_ids);
    if (!s.ok()) return s;

// Apply VersionEdit: add new files to L1
        {
            std::unique_lock<std::shared_mutex> lock(rw_mutex_);

            // Only now, with the outputs present, are the inputs safe to drop.
            // Flushes during the merge appended to the end of levels_[0], so the
            // prefix that was picked is still the prefix.
            if (l0_taken <= levels_[0].size()) {
                levels_[0].erase(levels_[0].begin(),
                                 levels_[0].begin() + static_cast<long>(l0_taken));
            } else {
                // Should not happen, but never drop more than we can account for.
                levels_[0].clear();
            }
            if (!levels_[1].empty()) {
                auto it = std::remove_if(levels_[1].begin(), levels_[1].end(),
                    [&l0_min_key, &l0_max_key](const std::shared_ptr<SSTable>& f) {
                        return f->LargestKey() >= l0_min_key &&
                               f->SmallestKey() <= l0_max_key;
                    });
                levels_[1].erase(it, levels_[1].end());
            }

            const auto& added = worker.edit().added_files();
        for (auto& meta : added) {
            // meta.level comes out of the VersionEdit, i.e. from another
            // component. Indexing levels_ with it unchecked is how a future
            // output_level change becomes levels_[7] at runtime instead of a
            // compile error.
            if (meta.level < 0 || meta.level >= kNumLevels) {
                std::fprintf(stderr,
                             "[LightKV] dropping compaction output %llu at invalid level %d\n",
                             (unsigned long long)meta.file_id, meta.level);
                continue;
            }
            std::stringstream ss;
            ss << options_.db_path << "/" << meta.file_id << ".sst";
            auto table = std::make_shared<SSTable>(options_, ss.str(), meta.file_id);
            if (table->Open().ok()) {
                levels_[meta.level].push_back(std::move(table));
            }
        }
        // Manifest edits: one lock around add+remove so a concurrent
        // UpdateManifest cannot append between the two halves of the swap.
        {
            std::lock_guard<std::mutex> ml(manifest_mu_);
            for (auto& meta : added) {
                manifest_.AddFile(meta.level, meta.file_id,
                                  meta.smallest_key, meta.largest_key,
                                  meta.file_size);
            }
            for (auto& input : inputs) {
                manifest_.RemoveFile(input.level, input.table->file_id());
            }
        }
    }

    Status ms = UpdateManifest();
    if (!ms.ok()) {
        // Do NOT retire the inputs. The merge succeeded and levels_ already
        // reflects it, but the manifest on disk still names the INPUTS and knows
        // nothing about the outputs -- which is the state a restart would come up
        // in. Unlinking them now deletes the very files that state points at, so
        // the data would be gone with no WAL to bring it back (compaction only
        // touches records that are already in an SSTable). Leaving them keeps
        // disk recoverable at the cost of some orphaned output files, and the
        // next successful UpdateManifest writes them out of the picture.
        return Status::IOError("compaction: cannot persist MANIFEST: " + ms.ToString());
    }

    // Deferred deletion: add old files to pending_delete_
    {
        std::unique_lock<std::shared_mutex> lock(rw_mutex_);
        for (auto& input : inputs) {
            pending_delete_.push_back(input.table->filename());
        }
    }
    CleanupDeletedFiles();
    return Status::OK();
}

Status DBImpl::DoLevelCompaction(int level) {
    // The deepest level has nowhere to compact into. PickCompaction no longer
    // returns it, and this guard makes the assumption explicit: without it the
    // outputs were pushed at levels_[kNumLevels], one past the end of the array.
    if (level < 0 || level + 1 >= kNumLevels) return Status::OK();

    // Pick one file from `level` plus the overlapping files in level+1.
    std::vector<CompactionWorker::InputFile> inputs;
    std::string selected_min, selected_max;

    {
        std::unique_lock<std::shared_mutex> lock(rw_mutex_);
        if (levels_[level].empty()) return Status::OK();

        // Take the oldest (first-written) file so compaction drains in order
        // rather than always revisiting the same one.
        const auto& picked = levels_[level].front();
        selected_min = picked->SmallestKey();
        selected_max = picked->LargestKey();
        inputs.push_back({level, picked});

        // Find overlapping files in level+1
        int next_level = level + 1;
        if (next_level < kNumLevels && !levels_[next_level].empty()) {
            for (auto& file : levels_[next_level]) {
                if (file->LargestKey() >= selected_min && file->SmallestKey() <= selected_max) {
                    inputs.push_back({next_level, file});
                }
            }
        }

        // NOT removed here. The merge runs with rw_mutex_ released, and a file
        // that has left levels_ but is not yet in the output is invisible to
        // readers in between -- the same window F-42 closed for L0. They are
        // detached after the merge succeeds, below.
    }

    // Do the compaction
    std::vector<uint64_t> new_file_ids;
    CompactionWorker worker(options_, options_.db_path, &next_file_id_);
    worker.SetOldestSnapshot(GetOldestSnapshot());
    worker.SetSnapshotProvider([this]() { return GetOldestSnapshot(); });
    worker.SetRateLimiter(compaction_rate_limiter_.get());
    auto s = worker.DoCompaction(inputs, level + 1, &new_file_ids);
    if (!s.ok()) return s;

    // Apply VersionEdit: add new files
    {
        std::unique_lock<std::shared_mutex> lock(rw_mutex_);

        // Detach the inputs by identity, now that their replacements exist.
        // Matching on key range instead would sweep out any file that merely
        // overlaps -- including one this round never read, whose contents would
        // then be neither referenced nor merged.
        for (const auto& input : inputs) {
            auto& files = levels_[input.level];
            const uint64_t drop_id = input.table->file_id();
            auto it = std::remove_if(files.begin(), files.end(),
                [drop_id](const std::shared_ptr<SSTable>& f) {
                    return f->file_id() == drop_id;
                });
            files.erase(it, files.end());
        }

        const auto& added = worker.edit().added_files();
        for (auto& meta : added) {
            // Same reason as the L0 path: the level is data from the edit, not a
            // constant this function controls.
            if (meta.level < 0 || meta.level >= kNumLevels) {
                std::fprintf(stderr,
                             "[LightKV] dropping compaction output %llu at invalid level %d\n",
                             (unsigned long long)meta.file_id, meta.level);
                continue;
            }
            std::stringstream ss;
            ss << options_.db_path << "/" << meta.file_id << ".sst";
            auto table = std::make_shared<SSTable>(options_, ss.str(), meta.file_id);
            if (table->Open().ok()) {
                levels_[meta.level].push_back(std::move(table));
            }
        }
        // Manifest edits: one lock around add+remove, same reasoning as the L0
        // path -- a concurrent UpdateManifest must not append mid-swap.
        {
            std::lock_guard<std::mutex> ml(manifest_mu_);
            for (auto& meta : added) {
                manifest_.AddFile(meta.level, meta.file_id,
                                  meta.smallest_key, meta.largest_key,
                                  meta.file_size);
            }
            for (auto& input : inputs) {
                manifest_.RemoveFile(input.level, input.table->file_id());
            }
        }
    }

    Status ms = UpdateManifest();
    if (!ms.ok()) {
        // Same reasoning as the L0 path: the inputs are what the manifest on disk
        // still names, so they must survive until a manifest that names the
        // outputs is on disk.
        return Status::IOError("compaction: cannot persist MANIFEST: " + ms.ToString());
    }

    // Deferred deletion
    {
        std::unique_lock<std::shared_mutex> lock(rw_mutex_);
        for (auto& input : inputs) {
            pending_delete_.push_back(input.table->filename());
        }
    }
    CleanupDeletedFiles();
    return Status::OK();
}

Status DBImpl::BackgroundCompaction() {
    stats_compactions_.fetch_add(1, std::memory_order_relaxed);
    // PickCompaction locks rw_mutex_ itself.
    CompactionScore score = PickCompaction();
    if (!score.needs_compaction()) return Status::OK();

    if (score.level == 0) {
        return DoLevel0Compaction();
    }
    return DoLevelCompaction(score.level);
}

void DBImpl::BackgroundWork() {
    while (true) {
        bool flush_pending = false;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            bg_cv_.wait(lock, [this] {
                // FlushPendingLocked(), not has_imm_: a flush that has spent all
                // its retries leaves has_imm_ set, and waking on that went
                // straight back into FlushMemTable to fail again -- the thread
                // spun at 100% CPU doing nothing but retrying a broken device.
                return shutting_down_ || bg_scheduled_ || FlushPendingLocked();
            });

            flush_pending = FlushPendingLocked();

            // Idle shutdown. A memtable whose flush gave up counts as idle here:
            // its records are intact in the WAL and the next Open() replays them,
            // whereas refusing to break would block the destructor's join()
            // forever.
            if (shutting_down_ && !bg_scheduled_ && !flush_pending) break;
        }

        if (flush_pending) {
            FlushMemTable();
            // After Flush, re-evaluate compaction
            MaybeScheduleCompaction();
        } else if (bg_scheduled_) {
            Status cs = BackgroundCompaction();
            // Re-check outside mutex_: PickCompaction takes rw_mutex_, and
            // mutex_ -> rw_mutex_ is the deadlocking direction. The scheduling
            // flag stays set across the gap, so a concurrent writer cannot
            // double-schedule in the meantime.
            auto score = PickCompaction();
            std::string report;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                bg_scheduled_ = false;
                // Re-check compaction after completing ONE SUCCESSFUL round. A
                // round that failed must not re-arm itself: the backlog it could
                // not drain is still there, so re-arming turns one broken round
                // into an immediate retry, and each retry is a full merge. The
                // next write that finds L0 over the stop threshold, or the next
                // flush, schedules the next attempt -- that is the pacing a
                // persistent failure should have, and a transient one is over by
                // then.
                if (cs.ok() && score.needs_compaction()) {
                    bg_scheduled_ = true;
                    bg_cv_.notify_one();
                }
                if (cs.ok()) {
                    last_compaction_error_.clear();
                } else if (std::string msg = cs.ToString(); msg != last_compaction_error_) {
                    // Print a given failure once. A wedged compaction is retried
                    // by every parked writer, and one thread's retry is enough to
                    // describe the problem -- repeating it a thousand times a
                    // second buries the first line and can fill a log volume.
                    last_compaction_error_ = msg;
                    report = std::move(msg);
                }
                // L0 just shrank, or at least did not grow. Writers parked on the
                // backpressure stop threshold are waiting on precisely this, so
                // without the signal they would sit out their whole timeout after
                // the work is already done.
                compaction_cv_.notify_all();
            }
            if (!report.empty()) {
                std::fprintf(stderr,
                             "[LightKV] ERROR: compaction round failed: %s\n"
                             "          Their inputs and outputs are both still on disk;\n"
                             "          the version set the MANIFEST names is unchanged.\n",
                             report.c_str());
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

Status DBImpl::UpdateManifest() {
    // The append path mutates the file in place, so two callers can interleave
    // whole or partial records and corrupt the log. WriteToFile never needed
    // this: it renamed a fully written temp file over the target, which is
    // atomic no matter how many threads race. flush() and the background
    // compaction both call in here, and they genuinely do overlap.
    std::lock_guard<std::mutex> manifest_lock(manifest_mu_);

    manifest_.last_seq = last_seq_.load(std::memory_order_acquire);
    manifest_.next_file_id = next_file_id_.load(std::memory_order_relaxed);

    // The very first update has to lay down a snapshot: an append log with no
    // snapshot in front of it relies entirely on replay, and the fallback scan
    // for a directory that never had a MANIFEST would not agree with it.
    //
    // A previous failure forces a snapshot too. AppendChanges writes frames in
    // place, so a failed append can leave a truncated frame at the tail of the
    // file, and replay stops at the first record it cannot read -- appending
    // again behind that tail makes the whole manifest read as Corruption and the
    // database refuse to open. WriteToFile builds a new file and renames it over
    // the old one, which discards the damaged tail.
    Status s;
    if (!manifest_written_ || manifest_dirty_.load(std::memory_order_relaxed)) {
        s = WriteManifestSnapshotLocked();
    } else {
        // Everything after the first snapshot is a delta. Rewriting the whole
        // file made each flush cost O(total SSTables) -- precisely when flushes
        // are frequent.
        s = manifest_.AppendChanges(options_.db_path);
        if (!s.ok()) {
            manifest_dirty_.store(true, std::memory_order_release);
            s = WriteManifestSnapshotLocked();
        }
    }

    // Do not swallow this. A failed manifest write means the set of live
    // SSTables is no longer described on disk, and the next Open() will come up
    // with a file set that disagrees with this process: either missing files
    // (data loss) or files this process already compacted away (Open fails).
    // The caller decides what staying safe means here; this function only
    // reports.
    return s;
}

Status DBImpl::WriteManifestSnapshotLocked() {
    // WriteToFile also drops the staged append records: the snapshot carries them
    // already, and replaying them on top would resurrect files it folded away.
    Status s = manifest_.WriteToFile(options_.db_path);
    if (!s.ok()) return s;

    manifest_written_ = true;
    manifest_dirty_.store(false, std::memory_order_release);
    return Status::OK();
}

Status DBImpl::FlushForBackup() {
    // Two rounds are enough, but loop rather than assume: the first round may
    // find a flush already in flight for the PREVIOUS memtable and only wait for
    // that one. The memtable holding our data is then still active -- which the
    // identity check below sees -- and the second round flushes it.
    //
    // The witness for "our data is durable" is where the memtable OBJECT lives
    // now, not MaxSeq() and not "did mem_ change". MaxSeq() was being used as an
    // "empty memtable" sentinel and is not one: the first write to a fresh
    // database gets sequence number 0, so a database holding exactly one write
    // read as empty, Backup returned OK without flushing it, and since a backup
    // ships no WAL the write was simply absent from the copy.
    std::shared_ptr<MemTable> pending;
    uint64_t wanted_seq = 0;
    for (int round = 0; round < 3; ++round) {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            pending = mem_.load(std::memory_order_acquire);
            wanted_seq = pending ? pending->MaxSeq() : 0;
            if (!has_imm_) {
                // Writers can append between this check and the flush actually
                // running, which is fine: those writes land in the NEXT memtable
                // and are correctly excluded from the copy.
                if (!pending || !pending->HasContent()) return Status::OK();
                TriggerFlush();  // moves `pending` to imm_, installs a new mem_
            }
            // else: a flush is already running. Wait it out and look again --
            // it proves nothing about the memtable that holds our writes.
        }

        {
            std::unique_lock<std::mutex> lock(mutex_);
            // Wait for the flush to SETTLE, not merely to succeed. Waiting on
            // !has_imm_ alone blocked forever the moment WriteLevel0Table failed,
            // because a failed flush deliberately leaves has_imm_ set.
            flush_cv_.wait(lock, [this] { return FlushSettledLocked(); });

            // "mem_ != pending" is NOT a durability witness: TriggerFlush swaps
            // mem_ unconditionally, so that comparison was already true before
            // the flush had written a single byte. It made a failed flush look
            // successful -- Backup returned OK holding a copy that was missing
            // acknowledged writes, and the IOError at the bottom of this function
            // could never be reached. Where the memtable sits right now is what
            // answers the question:
            //   * still the active memtable  -> never rotated, keep going
            //   * still the immutable one    -> the flush of OUR data failed
            //   * neither                    -> it is inside an SSTable
            const bool still_active = mem_.load(std::memory_order_acquire) == pending;
            const bool still_immutable = imm_.load(std::memory_order_acquire) == pending;
            if (!still_active && !still_immutable) return Status::OK();

            // Settled with an immutable memtable still waiting means the flush
            // gave up -- possibly on OUR memtable, possibly on the one ahead of
            // it, which blocks ours just the same. Either way there is nothing to
            // wait for and no honest backup to produce.
            if (!FlushPendingLocked() && has_imm_) {
                return Status::IOError("backup: memtable flush failed: " +
                                       std::string(flush_error_.ToString()));
            }
            // Only the active memtable: this round waited for somebody else's
            // flush, so go around and push ours out.
        }
        // Still the active memtable: this round only waited for another flush.
    }

    // Every round saw somebody else's flush in the way, so ours never ran. Give
    // up and say so -- a backup missing acknowledged writes is worse than a
    // backup that refused to be made.
    return Status::IOError(
        "backup: memtable flush did not complete after 3 rounds (wanted seq " +
        std::to_string(wanted_seq) + ", durable up to " +
        std::to_string(flushed_seq_.load(std::memory_order_acquire)) + ")");
}

Status DBImpl::Clear() {
    // Let an in-flight flush land BEFORE taking rw_mutex_ exclusively.
    //
    // Waiting while holding it deadlocks: the background flush needs rw_mutex_
    // to publish its SSTable, while we sit here waiting for that same flush to
    // clear has_imm_. mutex_ is the only lock needed to observe it, and mutex_
    // is never held by a thread that is waiting for rw_mutex_ in the opposite
    // direction, so this ordering is safe.
    //
    // The writes in the in-flight memtable were already acknowledged, so a flush
    // that fails has to surface rather than be quietly discarded.
    {
        std::unique_lock<std::mutex> lock(mutex_);
        if (has_imm_) {
            auto imm = imm_.load(std::memory_order_acquire);
            const uint64_t boundary = imm ? imm->MaxSeq() : 0;
            // Settled, not "succeeded": a flush that gave up leaves has_imm_ set
            // and would otherwise park this wait forever.
            flush_cv_.wait(lock, [this] { return FlushSettledLocked(); });
            if (has_imm_ || flushed_seq_.load(std::memory_order_acquire) < boundary) {
                return Status::IOError(
                    "Clear: in-flight flush did not complete (wanted seq " +
                    std::to_string(boundary) + ", durable up to " +
                    std::to_string(flushed_seq_.load(std::memory_order_acquire)) +
                    (flush_error_.ok() ? "" : ": " + std::string(flush_error_.ToString())));
            }
        }
    }

    // rw_mutex_ exclusively now. It is what WriteLevel0Table and the compaction
    // paths hold while they add or replace files, so once we own it nothing can
    // repopulate levels_ behind our back. A flush that starts in the window
    // above only lands in levels_, which this call is about to clear anyway.
    std::unique_lock<std::shared_mutex> rw(rw_mutex_);
    std::unique_lock<std::mutex> lock(mutex_);
    // Writers are stopped from here on: the WAL is closed and unlinked below, and
    // a write that slipped in would append to a file whose records were just
    // dropped, or to a vlog the next lines delete.
    std::lock_guard<WriteLock> wl(write_mu_);

    bg_scheduled_ = false;

    // Collect what we are about to orphan.
    std::vector<std::string> stale_files;
    for (int level = 0; level < kNumLevels; ++level) {
        for (const auto& table : levels_[level]) {
            stale_files.push_back(table->filename());
        }
        levels_[level].clear();
    }
    for (const auto& path : pending_delete_) stale_files.push_back(path);
    pending_delete_.clear();

    mem_.store(std::make_shared<MemTable>(), std::memory_order_release);
    imm_.store(nullptr, std::memory_order_release);
    has_imm_ = false;
    write_count_ = 0;

    {
        std::lock_guard<std::mutex> ml(manifest_mu_);
        manifest_ = Manifest();
        manifest_written_ = false;
    }
    last_seq_.store(0, std::memory_order_release);
    next_file_id_.store(1, std::memory_order_release);
    flushed_seq_.store(0, std::memory_order_release);
    {
        std::lock_guard<std::mutex> snap_lock(snapshot_mutex_);
        active_snapshots_.clear();
    }

    // Rotate the WAL: records for data we just dropped must not be replayed the
    // next time this database is opened. Both files have to go -- the active one
    // and the one belonging to imm_ -- and the stale ones are unlinked right here
    // rather than deferred, since a leftover WAL would resurrect the cleared data
    // on the next start.
    //
    // write_mu_ is held, so no fsync can register from here on; wait for the ones
    // that registered earlier. They run against these very objects.
    WaitForWalSyncIdle();
    if (wal_) {
        wal_->Close();
        std::error_code ec;
        std::filesystem::remove(wal_path_, ec);
    }
    if (imm_wal_) {
        imm_wal_->Close();
        imm_wal_.reset();
        std::error_code ec;
        std::filesystem::remove(imm_wal_path_, ec);
        imm_wal_path_.clear();
    }
    {
        std::error_code ec;
        for (const auto& entry : std::filesystem::directory_iterator(options_.db_path, ec)) {
            const std::string name = entry.path().filename().string();
            if (name.rfind("wal", 0) == 0 && name.size() > 4 &&
                name.compare(name.size() - 4, 4, ".log") == 0) {
                std::error_code rm_ec;
                std::filesystem::remove(entry.path(), rm_ec);
            }
        }
    }

    wal_path_ = options_.db_path + "/wal-" + std::to_string(next_wal_id_++) + ".log";
    wal_ = std::make_unique<WALWriter>(wal_path_);
    Status s = wal_->Open();
    if (!s.ok()) return s;

    // Fresh file, fresh counters: the tickets writers hold are meaningless once
    // the WAL they were appended to no longer exists.
    {
        std::lock_guard<std::mutex> sl(wal_sync_mu_);
        wal_written_ = 0;
        wal_synced_ = 0;
        ++wal_gen_;
    }

    // vlog files hold the large values the discarded SSTables pointed at.
    if (auto old_vlog = vlog_.load(std::memory_order_acquire)) old_vlog->Close();
    {
        std::error_code ec;
        for (const auto& entry : std::filesystem::directory_iterator(options_.db_path, ec)) {
            const std::string name = entry.path().filename().string();
            if (name.rfind("vlog-", 0) == 0) stale_files.push_back(entry.path().string());
        }
    }
    auto fresh_vlog = std::make_shared<VLogManager>(options_.db_path, options_.vlog_file_size_limit);
    s = fresh_vlog->Initialize();
    if (!s.ok()) return s;
    // Swap the pointer rather than resetting in place: a reader that already
    // loaded the old manager keeps it alive until it is done with it.
    vlog_.store(fresh_vlog, std::memory_order_release);

    // Publish the empty manifest before unlinking anything: if this write fails,
    // the old file set is still described on disk and we can back out without
    // having destroyed data the manifest still references.
    Status ms = UpdateManifest();
    if (!ms.ok()) {
        // Nothing has been unlinked yet, so every file the old manifest names is
        // still there and a restart still finds the data. Say so and stop: going
        // on to unlink them in this state would delete exactly the files that
        // manifest points at, and this process is in no position to claim the
        // clear was recorded.
        return Status::IOError(
            "Clear: cannot persist the empty MANIFEST, so the previous file set is "
            "still on disk and will come back on the next open: " + ms.ToString());
    }

    // Defer the unlink while any iterator still holds an open handle. Windows
    // refuses to delete a mapped file, and CleanupDeletedFiles() already uses
    // the same guard.
    if (active_iterators_.load(std::memory_order_acquire) > 0) {
        for (const auto& path : stale_files) pending_delete_.push_back(path);
    } else {
        for (const auto& path : stale_files) std::remove(path.c_str());
    }

    rw.unlock();
    lock.unlock();
    return Status::OK();
}

Status DBImpl::Backup(const std::string& backup_path) {
    if (backup_path.empty()) {
        return Status::InvalidArgument("backup_path must not be empty");
    }
    if (backup_path == options_.db_path) {
        return Status::InvalidArgument("backup_path must differ from db_path");
    }

    // 1. Push the memtable out first.
    //
    //    The previous implementation relied on the WAL for anything not yet in
    //    an SSTable, but it copied the whole wal.log: a 64MB preallocated mmap
    //    file whose tail is uninitialised padding, copied without a Sync. The
    //    backup is now a standalone image and ships no WAL at all -- DB::Open
    //    creates a fresh one and takes last_seq from the manifest.
    Status s = FlushForBackup();
    if (!s.ok()) return s;

    // 2. Block compaction and file cleanup for the whole copy.
    //
    //    The old code took a *shared* lock and then read and rewrote the
    //    manifest outside it. A compaction running concurrently could add a
    //    file the manifest names but the copy never made (restore fails), or
    //    record a file that compaction had already unlinked (restore loses it).
    std::unique_lock<std::shared_mutex> lock(rw_mutex_);

    const uint64_t snap_seq = flushed_seq_.load(std::memory_order_acquire);

    // 3. Stage into a sibling directory and swap it in at the end, so a failure
    //    halfway through never leaves a half-written backup where a good one
    //    used to be.
    const std::string staging = backup_path + ".tmp";
    {
        std::error_code ec;
        std::filesystem::remove_all(staging, ec);
        ec.clear();
        std::filesystem::create_directories(staging, ec);
        if (ec) {
            return Status::IOError("cannot create backup staging directory: " + ec.message());
        }
    }

    auto copy_one = [&](const std::string& src, const std::string& dst) -> Status {
        std::error_code ec;
        std::filesystem::copy_file(src, dst, std::filesystem::copy_options::overwrite_existing, ec);
        if (ec) {
            return Status::IOError("backup: cannot copy " + src + ": " + ec.message());
        }
        return Status::OK();
    };

    // 4. Copy every SSTable the manifest names.
    for (int level = 0; level < kNumLevels; ++level) {
        for (const auto& table : levels_[level]) {
            s = copy_one(table->filename(),
                         staging + "/" + std::filesystem::path(table->filename()).filename().string());
            if (!s.ok()) return s;
        }
    }

    // 5. Copy the vlog files.
    //
    //    Values above options_.value_threshold are stored out of line and the
    //    SSTable holds only a (file_id, offset, length) pointer. The old code
    //    never touched vlog-*.log, so any backup of a database holding a large
    //    value was silently unreadable.
    {
        std::error_code ec;
        // Sync once, before copying anything: Append writes through an mmap, so
        // the files may still have unwritten tails.
        if (auto vlog = vlog_.load(std::memory_order_acquire)) {
            Status vs = vlog->Sync();
            if (!vs.ok()) return vs;
        }
        for (const auto& entry : std::filesystem::directory_iterator(options_.db_path, ec)) {
            const std::string name = entry.path().filename().string();
            if (name.rfind("vlog-", 0) != 0) continue;
            s = copy_one(entry.path().string(), staging + "/" + name);
            if (!s.ok()) return s;
        }
    }

    // 6. Write a manifest describing exactly the files we just copied.
    //
    //    last_seq is the flush boundary, not the live counter: everything above
    //    it is still only in the (now swapped out) memtable, and a restored DB
    //    that started allocating from there would reuse sequence numbers it has
    //    already handed out.
    Manifest backup_manifest;
    {
        // rw_mutex_ (held) keeps levels_ still, but manifest_ has its own lock:
        // the background flush edits it through AddFile outside rw_mutex_.
        std::lock_guard<std::mutex> ml(manifest_mu_);
        backup_manifest.format_version = manifest_.format_version;
        backup_manifest.last_seq = snap_seq;
        backup_manifest.flushed_seq = snap_seq;
        backup_manifest.next_file_id = next_file_id_.load(std::memory_order_relaxed);
        for (int level = 0; level < kNumLevels; ++level) {
            for (const auto& meta : manifest_.files[level]) {
                backup_manifest.AddFile(level, meta.file_id, meta.smallest_key,
                                        meta.largest_key, meta.file_size);
            }
        }
    }
    s = backup_manifest.WriteToFile(staging);
    if (!s.ok()) return s;

    // 7. Verify what we produced before publishing it.
    for (int level = 0; level < kNumLevels; ++level) {
        for (const auto& meta : backup_manifest.files[level]) {
            const std::string path = staging + "/" + std::to_string(meta.file_id) + ".sst";
            SSTable verify_table(options_, path, meta.file_id);
            Status vs = verify_table.Open();
            if (!vs.ok()) {
                return Status::Corruption("backup verification failed: " + vs.ToString());
            }
        }
    }

    // 8. Publish atomically. Windows cannot rename onto a non-empty directory
    //    (MoveFileEx returns ERROR_ACCESS_DENIED), so the old tree has to go
    //    first -- there is a brief window where neither path exists, which is
    //    still far better than serving a partially written backup.
    lock.unlock();
    {
        std::error_code ec;
        std::filesystem::remove_all(backup_path, ec);
        ec.clear();
        std::filesystem::rename(staging, backup_path, ec);
        if (ec) {
            std::error_code cleanup_ec;
            std::filesystem::remove_all(staging, cleanup_ec);
            return Status::IOError("cannot publish backup to " + backup_path + ": " + ec.message());
        }
    }
    return Status::OK();
}

} // namespace lightkv
