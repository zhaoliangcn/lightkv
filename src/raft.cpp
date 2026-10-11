#include "lightkv/raft.h"
#include "lightkv/platform.h"
#include <sstream>
#include <algorithm>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <chrono>
#include <cstdio>

// ─── Raft 调试日志宏 ───
#define RAFT_LOG(fmt, ...)  fprintf(stderr, "[Raft] " fmt "\n", ##__VA_ARGS__)
#define RAFT_WARN(fmt, ...) fprintf(stderr, "[Raft WARN] " fmt "\n", ##__VA_ARGS__)
#ifdef NDEBUG
#define RAFT_DEBUG(fmt, ...) ((void)0)
#else
#define RAFT_DEBUG(fmt, ...) fprintf(stderr, "[Raft DEBUG] " fmt "\n", ##__VA_ARGS__)
#endif

namespace lightkv {

// ═══════════════════════════════════════════════════
// 序列化辅助
// ═══════════════════════════════════════════════════

namespace raft_encoding {

void PutFixed32(std::string* dst, uint32_t v) {
    dst->push_back(static_cast<char>(v & 0xff));
    dst->push_back(static_cast<char>((v >> 8) & 0xff));
    dst->push_back(static_cast<char>((v >> 16) & 0xff));
    dst->push_back(static_cast<char>((v >> 24) & 0xff));
}

void PutFixed64(std::string* dst, uint64_t v) {
    for (int i = 0; i < 8; ++i) {
        dst->push_back(static_cast<char>(v & 0xff));
        v >>= 8;
    }
}

uint32_t GetFixed32(const Slice& src, size_t* offset) {
    uint32_t v = 0;
    for (int i = 3; i >= 0; --i) {
        v = (v << 8) | static_cast<uint8_t>(src.data()[(*offset) + i]);
    }
    *offset += 4;
    return v;
}

uint64_t GetFixed64(const Slice& src, size_t* offset) {
    uint64_t v = 0;
    for (int i = 7; i >= 0; --i) {
        v = (v << 8) | static_cast<uint8_t>(src.data()[(*offset) + i]);
    }
    *offset += 8;
    return v;
}

} // namespace raft_encoding

// ═══════════════════════════════════════════════════
// RaftLogEntry 序列化
// ═══════════════════════════════════════════════════

std::string RaftLogEntry::Serialize() const {
    std::string buf;
    // format: index(8) | term(8) | type(1) | data_len(4) | data
    raft_encoding::PutFixed64(&buf, index);
    raft_encoding::PutFixed64(&buf, term);
    buf.push_back(static_cast<uint8_t>(type));
    raft_encoding::PutFixed32(&buf, static_cast<uint32_t>(data.size()));
    buf.append(data);
    return buf;
}

RaftLogEntry RaftLogEntry::Deserialize(const Slice& slice) {
    RaftLogEntry entry;
    size_t off = 0;
    entry.index = raft_encoding::GetFixed64(slice, &off);
    entry.term = raft_encoding::GetFixed64(slice, &off);
    entry.type = static_cast<RaftEntryType>(slice.data()[off++]);
    uint32_t data_len = raft_encoding::GetFixed32(slice, &off);
    entry.data.assign(slice.data() + off, data_len);
    return entry;
}

// ═══════════════════════════════════════════════════
// Raft 构造函数
// ═══════════════════════════════════════════════════

Raft::Raft(const RaftOptions& opts, RaftStateMachine* state_machine, RaftRPC* rpc)
    : opts_(opts),
      state_machine_(state_machine),
      rpc_(rpc),
      rng_(std::random_device{}()) {

    // 初始化 leader 易失状态（分配好大小）
    next_index_.resize(opts_.peers.size(), 1);
    match_index_.resize(opts_.peers.size(), 0);

    // 日志初始包含一条 term=0 的 dummy entry（index=0）
    RaftLogEntry dummy;
    dummy.index = 0;
    dummy.term = 0;
    dummy.type = RaftEntryType::kNoOp;
    log_.push_back(dummy);
}

Raft::~Raft() {
    Stop();
}

// ═══════════════════════════════════════════════════
// 初始化 & 生命周期
// ═══════════════════════════════════════════════════

Status Raft::Initialize(const std::string& db_path) {
    if (!opts_.enable_raft) {
        return Status::OK();
    }
    if (opts_.peers.empty()) {
        return Status::InvalidArgument("Raft peers list is empty");
    }
    if (opts_.node_id == 0) {
        return Status::InvalidArgument("Raft node_id must be non-zero");
    }

    // The state and snapshot files live next to the DB, but Raft must not assume
    // the directory already exists: if it does not, fopen() on the temp file
    // fails and every save is silently discarded.
    std::error_code ec;
    std::filesystem::create_directories(db_path, ec);
    if (ec && !std::filesystem::is_directory(db_path)) {
        return Status::IOError("cannot create raft directory: " + db_path);
    }

    // 尝试从持久化状态恢复
    db_path_ = db_path;
    Status loaded = LoadPersistentState(db_path);
    if (!loaded.ok()) return loaded;

    // 设置随机选举超时
    std::uniform_int_distribution<int> dist(
        opts_.election_timeout_min_ms,
        opts_.election_timeout_max_ms);
    election_timeout_ms_ = dist(rng_);
    last_heartbeat_ = std::chrono::steady_clock::now();

    // 验证本节点存在于 peers 中
    bool found_self = false;
    for (const auto& peer : opts_.peers) {
        if (peer.id == opts_.node_id) {
            found_self = true;
            break;
        }
    }
    if (!found_self) {
        return Status::InvalidArgument("Raft node_id not found in peers list");
    }

    initialized_ = true;
    return Status::OK();
}

void Raft::Start() {
    if (!opts_.enable_raft || running_.exchange(true)) {
        return;
    }

    // 选举超时检测线程
    election_thread_ = std::thread([this]() {
        while (running_) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));

            if (!running_) break;

            // 只有 follower 和 candidate 需要检测超时
            RaftRole current_role;
            {
                std::lock_guard<std::mutex> lock(mu_);
                current_role = role_;
            }

            if (current_role == RaftRole::kLeader) {
                continue;
            }

            auto now = std::chrono::steady_clock::now();
            std::chrono::steady_clock::time_point last_hb;
            {
                std::lock_guard<std::mutex> lock(mu_);
                last_hb = last_heartbeat_;
            }
            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                now - last_hb).count();

            if (elapsed >= election_timeout_ms_) {
                // 超时，开始选举
                {
                    std::lock_guard<std::mutex> lock(mu_);
                    if (role_ != RaftRole::kLeader) {
                        HandleElectionTimeout();
                    }
                }
            }
        }
    });

    // 心跳 / 日志复制线程（仅 leader 活跃）
    replication_thread_ = std::thread([this]() {
        while (running_) {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(opts_.heartbeat_interval_ms));

            if (!running_) break;

            RaftRole current_role;
            {
                std::lock_guard<std::mutex> lock(mu_);
                current_role = role_;
            }

            if (current_role == RaftRole::kLeader) {
                ReplicateLog(false);
                // Log compaction is driven from here. Nothing else called
                // CheckAndCompactLog(), so snapshot_threshold_ never fired and
                // the leader's log grew without bound.
                CheckAndCompactLog();
            }
        }
    });
}

void Raft::Stop() {
    running_ = false;
    if (election_thread_.joinable()) {
        election_thread_.join();
    }
    if (replication_thread_.joinable()) {
        replication_thread_.join();
    }
    // Only persist once the node has actually been initialized. A Raft object
    // that was constructed but never Initialize()d has db_path_ == "" and
    // would write to "./raft_state.dat" in the caller's CWD -- polluting the
    // repo root on a test scaffold that never ran.
    if (initialized_) SavePersistentState();
}

// ═══════════════════════════════════════════════════
// 客户端接口
// ═══════════════════════════════════════════════════

int64_t Raft::Propose(const std::string& data) {
    std::lock_guard<std::mutex> lock(mu_);

    if (role_ != RaftRole::kLeader) {
        return -1;  // 非 leader 拒绝
    }

    // 创建日志条目
    RaftLogEntry entry;
    entry.index = GetLastLogIndex() + 1;
    entry.term = current_term_;
    entry.type = RaftEntryType::kCommand;
    entry.data = data;

    log_.push_back(entry);

    // 持久化状态（日志已变更）
    SavePersistentState();

    return static_cast<int64_t>(entry.index);
}

// ═══════════════════════════════════════════════════
// 同步提交
// ═══════════════════════════════════════════════════

bool Raft::ProposeSync(const std::string& data, int timeout_ms, std::string* result) {
    // 获取当前日志索引，用于等待提交确认
    uint64_t propose_idx;
    {
        std::lock_guard<std::mutex> lock(mu_);
        propose_idx = GetLastLogIndex() + 1;
    }

    // Register a slot for the apply result BEFORE proposing: the apply can run
    // immediately after the entry is appended on a single-node cluster, so
    // registering afterwards would race with ApplyCommitted().
    std::shared_ptr<PendingResult> slot;
    if (result) {
        slot = std::make_shared<PendingResult>();
        std::lock_guard<std::mutex> lock(mu_);
        pending_results_[propose_idx] = slot;
    }

    // 先异步提交
    int64_t idx = Propose(data);
    if (idx < 0) {
        if (result) {
            std::lock_guard<std::mutex> lock(mu_);
            pending_results_.erase(propose_idx);
        }
        return false;
    }

    // 等待日志条目被提交 (commit_index >= propose_idx)
    //
    // Wait on mu_, NOT a separate mutex: commit_index_ is written under mu_
    // (see AdvanceCommitIndex) and commit_cv_ is notified while mu_ is held.
    // Waiting on an unrelated mutex means no happens-before edge and lost
    // wakeups, which made every ProposeSync() stall for the full timeout and
    // then report failure even though the entry had committed.
    bool committed;
    {
        std::unique_lock<std::mutex> lock(mu_);
        if (timeout_ms <= 0) timeout_ms = 5000;
        committed = commit_cv_.wait_for(lock, std::chrono::milliseconds(timeout_ms),
                                        [this, propose_idx]() {
                                            return commit_index_ >= propose_idx;
                                        });
        if (committed && result) {
            // The apply may still be in flight; wait for the slot to fill.
            auto it = pending_results_.find(propose_idx);
            if (it != pending_results_.end()) {
                std::shared_ptr<PendingResult> s = it->second;
                commit_cv_.wait_for(lock, std::chrono::milliseconds(timeout_ms),
                                    [&s]() {
                                        std::lock_guard<std::mutex> sl(s->mu);
                                        return s->done;
                                    });
            }
        }
        if (result) {
            pending_results_.erase(propose_idx);
        }
    }

    if (committed && result && slot) {
        std::lock_guard<std::mutex> sl(slot->mu);
        if (slot->done) {
            *result = slot->value;
        }
    }
    return committed;
}

// ═══════════════════════════════════════════════════
// 状态查询
// ═══════════════════════════════════════════════════

RaftRole Raft::GetRole() const {
    std::lock_guard<std::mutex> lock(mu_);
    return role_;
}

uint64_t Raft::GetLeaderId() const {
    std::lock_guard<std::mutex> lock(mu_);
    return leader_id_;
}

std::string Raft::GetLeaderAddr() const {
    std::lock_guard<std::mutex> lock(mu_);
    uint64_t lid = leader_id_;
    if (lid == 0 || lid == opts_.node_id) return "";
    for (const auto& peer : opts_.peers) {
        if (peer.id == lid) {
            return peer.host + ":" + std::to_string(peer.port);
        }
    }
    return "";
}

uint64_t Raft::GetCurrentTerm() const {
    std::lock_guard<std::mutex> lock(mu_);
    return current_term_;
}

// ═══════════════════════════════════════════════════
// 角色变更
// ═══════════════════════════════════════════════════

void Raft::BecomeFollower(uint64_t term) {
    current_term_ = term;
    role_ = RaftRole::kFollower;
    voted_for_ = 0;
    last_heartbeat_ = std::chrono::steady_clock::now();
    RAFT_LOG("Node %lu became FOLLOWER, term=%lu", opts_.node_id, term);

    // 重置选举超时
    std::uniform_int_distribution<int> dist(
        opts_.election_timeout_min_ms,
        opts_.election_timeout_max_ms);
    election_timeout_ms_ = dist(rng_);

    SavePersistentState();
}

void Raft::BecomeCandidate() {
    current_term_++;
    role_ = RaftRole::kCandidate;
    voted_for_ = opts_.node_id;
    leader_id_ = 0;
    last_heartbeat_ = std::chrono::steady_clock::now();
    RAFT_LOG("Node %lu became CANDIDATE, term=%lu", opts_.node_id, current_term_);

    // 重置选举超时
    std::uniform_int_distribution<int> dist(
        opts_.election_timeout_min_ms,
        opts_.election_timeout_max_ms);
    election_timeout_ms_ = dist(rng_);

    SavePersistentState();
}

void Raft::BecomeLeader() {
    role_ = RaftRole::kLeader;
    leader_id_ = opts_.node_id;
    last_heartbeat_ = std::chrono::steady_clock::now();
    RAFT_LOG("Node %lu became LEADER for term %lu", opts_.node_id, current_term_);

    // 初始化 leader 易失状态
    uint64_t last_log_index = GetLastLogIndex();
    for (size_t i = 0; i < opts_.peers.size(); ++i) {
        if (opts_.peers[i].id != opts_.node_id) {
            next_index_[i] = last_log_index + 1;
            match_index_[i] = 0;
        } else {
            match_index_[i] = last_log_index;
        }
    }

    // 新 leader 提交一条 no-op 日志（Raft 论文 8 节）
    RaftLogEntry noop;
    noop.index = last_log_index + 1;
    noop.term = current_term_;
    noop.type = RaftEntryType::kNoOp;
    noop.data = "";
    log_.push_back(noop);

    SavePersistentState();

    // 立即发送心跳宣告 leader 身份
    // 实际发送在 replication_thread 中进行
}

// ═══════════════════════════════════════════════════
// 选举
// ═══════════════════════════════════════════════════

void Raft::HandleElectionTimeout() {
    BecomeCandidate();
    StartElection();
}

void Raft::StartElection() {
    uint64_t term = current_term_;
    uint64_t last_log_index = GetLastLogIndex();
    uint64_t last_log_term = GetLastLogTerm();
    RAFT_LOG("Starting election term=%lu, last_log=%lu, last_term=%lu", term, last_log_index, last_log_term);

    // 统计得票（自己先投自己一票）
    uint64_t votes = 1;
    uint64_t peers_count = 0;
    for (const auto& peer : opts_.peers) {
        if (peer.is_voter) peers_count++;
    }
    uint64_t majority = peers_count / 2 + 1;

    // 向所有其他节点发送 RequestVote
    for (size_t i = 0; i < opts_.peers.size(); ++i) {
        if (opts_.peers[i].id == opts_.node_id) continue;
        if (!opts_.peers[i].is_voter) continue;

        RequestVoteRequest req;
        req.term = term;
        req.candidate_id = opts_.node_id;
        req.last_log_index = last_log_index;
        req.last_log_term = last_log_term;

        // RPC 调用（在锁外执行，避免死锁）
        mu_.unlock();
        auto resp = rpc_->SendRequestVote(opts_.peers[i].id, req);
        mu_.lock();

        // 检查任期变化（如果调用者已不再是 candidate 则放弃）
        if (role_ != RaftRole::kCandidate || current_term_ != term) {
            return;
        }

        if (resp.term > current_term_) {
            BecomeFollower(resp.term);
            return;
        }

        if (resp.vote_granted) {
            votes++;
            if (votes >= majority) {
                // 赢得选举
                BecomeLeader();
                return;
            }
        }
    }

    // The loop above skips our own peer entry, so a single-node cluster (where
    // we are the only voter and already hold its self-vote) never enters the
    // body and would campaign forever. Check the tally once more after the loop.
    if (votes >= majority) {
        BecomeLeader();
        return;
    }

    // 未赢得选举：重置超时，等待下一次选举
    last_heartbeat_ = std::chrono::steady_clock::now();
}

// ═══════════════════════════════════════════════════
// RPC 处理
// ═══════════════════════════════════════════════════

AppendEntriesResponse Raft::HandleAppendEntries(const AppendEntriesRequest& req) {
    std::lock_guard<std::mutex> lock(mu_);

    AppendEntriesResponse resp;
    resp.term = current_term_;
    resp.success = false;

    // 1. 如果请求任期小于当前任期，拒绝
    if (req.term < current_term_) {
        return resp;
    }

    // 2. 收到合法 leader 的心跳，重置超时并降级为 follower
    if (req.term >= current_term_) {
        BecomeFollower(req.term);
        leader_id_ = req.leader_id;
    }

    // Entries below this node's snapshot baseline no longer exist locally, so the
    // consistency check below would compare against compacted-away placeholders
    // and a stale leader could truncate the prefix that backs our snapshot.
    // Point the leader at the first index we can still serve.
    if (req.prev_log_index < last_snapshot_index_) {
        resp.conflict_index = last_snapshot_index_ + 1;
        resp.conflict_term = 0;
        return resp;
    }

    // 3. 检查 prev_log_index 处的日志是否匹配
    if (req.prev_log_index > 0) {
        const size_t pos = LogPos(req.prev_log_index);
        if (pos == kNoPos) {
            // 我们已经没有这个位置了
            resp.conflict_index = LastLogIndex() + 1;
            resp.conflict_term = 0;
            return resp;
        }

        const auto& existing = log_[pos];
        if (existing.term != req.prev_log_term) {
            // 任期冲突：返回冲突 term 的第一个 index
            resp.conflict_term = existing.term;
            resp.conflict_index = req.prev_log_index;
            // 向前扫描到该 term 的第一个 index（不能越过日志基址）
            size_t scan = pos;
            while (resp.conflict_index > log_base_ &&
                   log_[scan - 1].term == existing.term) {
                resp.conflict_index--;
                --scan;
            }
            return resp;
        }
    }

    // 4. 处理新日志条目
    if (!req.entries.empty()) {
        for (const auto& entry : req.entries) {
            const size_t pos = LogPos(entry.index);
            if (pos != kNoPos) {
                if (log_[pos].term == entry.term) {
                    // 已经有完全相同的一条，跳过
                    continue;
                }
                // 检查是否冲突 —— 截断冲突部分，保留其下的条目
                log_.resize(pos);
            }
            // AppendEntries 必须携带连续的一段；否则 log_ 会与绝对索引错位。
            if (entry.index != log_base_ + log_.size()) {
                RAFT_LOG("Rejecting non-contiguous AppendEntries: expected index %lu, got %lu",
                         log_base_ + log_.size(), entry.index);
                resp.success = false;
                resp.conflict_index = log_base_ + log_.size();
                return resp;
            }
            log_.push_back(entry);
        }
        SavePersistentState();
    }

    // 5. 更新 commit_index
    if (req.leader_commit > commit_index_) {
        commit_index_ = std::min(req.leader_commit, GetLastLogIndex());
        commit_cv_.notify_all();  // 通知等待 ProposeSync 的线程
        // 应用已提交日志
        ApplyCommitted();
    }

    resp.term = current_term_;
    resp.success = true;
    return resp;
}

RequestVoteResponse Raft::HandleRequestVote(const RequestVoteRequest& req) {
    std::lock_guard<std::mutex> lock(mu_);

    RequestVoteResponse resp;
    resp.term = current_term_;
    resp.vote_granted = false;

    // 1. 如果请求任期小于当前任期，拒绝
    if (req.term < current_term_) {
        return resp;
    }

    // 2. 如果请求任期大于当前任期，降级为 follower
    if (req.term > current_term_) {
        BecomeFollower(req.term);
    }

    // 3. 检查是否已经投票给其他人
    if (voted_for_ != 0 && voted_for_ != req.candidate_id) {
        return resp;
    }

    // 4. 选举限制：candidate 的日志必须至少和自己一样新
    uint64_t last_log_index = GetLastLogIndex();
    uint64_t last_log_term = GetLastLogTerm();

    bool log_up_to_date = false;
    if (req.last_log_term > last_log_term) {
        log_up_to_date = true;
    } else if (req.last_log_term == last_log_term && req.last_log_index >= last_log_index) {
        log_up_to_date = true;
    }

    if (!log_up_to_date) {
        return resp;
    }

    // 5. 授予投票
    voted_for_ = req.candidate_id;
    last_heartbeat_ = std::chrono::steady_clock::now();
    resp.vote_granted = true;
    SavePersistentState();

    return resp;
}

InstallSnapshotResponse Raft::HandleInstallSnapshot(const InstallSnapshotRequest& req) {
    std::lock_guard<std::mutex> lock(mu_);

    InstallSnapshotResponse resp;
    resp.term = current_term_;
    resp.success = false;

    if (req.term < current_term_) {
        return resp;
    }

    if (req.term > current_term_) {
        BecomeFollower(req.term);
    }

    leader_id_ = req.leader_id;

    // An empty payload means the leader had nothing real to send. Installing it
    // would wipe this node's log without replacing its state, which is strictly
    // worse than staying behind. Reject and keep serving from our own log.
    if (req.data.empty()) {
        RAFT_LOG("Rejecting empty InstallSnapshot from leader %lu (index=%lu)",
                 req.leader_id, req.last_included_index);
        resp.success = false;
        return resp;
    }

    // A snapshot we have already applied would rewind our state machine to an
    // older image. Raft only ever retransmits a snapshot we already acked, so
    // treating it as success-and-ignore is both safe and idempotent.
    if (req.last_included_index <= last_applied_) {
        RAFT_DEBUG("Ignoring stale InstallSnapshot from leader %lu (index=%lu <= "
                   "last_applied=%lu)",
                   req.leader_id, req.last_included_index, last_applied_);
        resp.success = true;
        return resp;
    }

    // Apply the snapshot BEFORE touching our log or indices.
    //
    // If this fails we must leave the node exactly as it was: keeping the old
    // log means the leader can still ship us the entries, whereas truncating
    // first and then failing would leave us unable to reach any committed state
    // at all. Equally important, an unconditional success here would let the
    // leader advance next_index_/match_index_ for us and count this node toward
    // the majority while it never applied anything.
    if (state_machine_) {
        Status as = state_machine_->ApplySnapshot(req.data);
        if (!as.ok()) {
            RAFT_LOG("InstallSnapshot from leader %lu (index=%lu) failed to install: %s",
                     req.leader_id, req.last_included_index, as.ToString().c_str());
            resp.success = false;
            return resp;
        }
    }

    // Truncate the log to last_included_index and record the new baseline.
    CommitInstalledSnapshot(req.last_included_index, req.last_included_term,
                            req.data);

    resp.success = true;
    return resp;
}

// Shared tail of both install paths: record the baseline, truncate the log,
// persist. Only called once the full snapshot is in hand.
void Raft::CommitInstalledSnapshot(uint64_t cut_index, uint64_t cut_term,
                                   const std::string& data) {
    // The log is replaced by a single base entry carrying (cut_index, cut_term).
    // That is also the entry AppendEntries matches against via
    // prev_log_index == last_snapshot_index_.
    {
        RaftLogEntry base;
        base.index = cut_index;
        base.term = cut_term;
        base.type = RaftEntryType::kNoOp;
        log_.clear();
        log_.push_back(std::move(base));
        log_base_ = cut_index;
    }

    commit_index_ = cut_index;
    last_applied_ = cut_index;

    // Record the baseline we now own. Without this the node keeps no record
    // that its log starts at cut_index, so a later compaction or a promotion to
    // leader would ship a snapshot inconsistent with its own log.
    last_snapshot_index_ = cut_index;
    last_snapshot_term_ = cut_term;
    {
        std::lock_guard<std::mutex> snap_lock(snapshot_mutex_);
        snapshot_data_ = data;
    }
    SaveSnapshotData();
    SavePersistentState();
}

InstallSnapshotChunkResponse Raft::HandleInstallSnapshotChunk(
    const InstallSnapshotChunkRequest& req) {
    InstallSnapshotChunkResponse resp;
    std::lock_guard<std::mutex> lock(mu_);

    resp.term = current_term_;
    resp.rejected = true;

    if (req.term < current_term_) return resp;
    if (req.term > current_term_) {
        BecomeFollower(req.term);
        resp.term = current_term_;
    }
    if (req.data.empty()) return resp;

    // Bound the staging buffer up front. Without this a single leader decides
    // how much memory every follower may allocate.
    if (req.total_size == 0 || req.total_size > opts_.max_snapshot_size) {
        RAFT_WARN("Rejecting oversized snapshot from leader %lu: %llu bytes "
                  "(limit %llu)",
                  req.leader_id, (unsigned long long)req.total_size,
                  (unsigned long long)opts_.max_snapshot_size);
        return resp;
    }
    if (req.chunk_offset + req.data.size() > req.total_size) {
        RAFT_WARN("Rejecting inconsistent snapshot chunk from leader %lu: "
                  "offset=%llu + %zu exceeds total=%llu",
                  req.leader_id, (unsigned long long)req.chunk_offset,
                  req.data.size(), (unsigned long long)req.total_size);
        return resp;
    }

    {
        std::lock_guard<std::mutex> staging_lock(snapshot_staging_mu_);

        const bool identity_changed =
            snapshot_staging_.active &&
            (snapshot_staging_.last_included_index != req.last_included_index ||
             snapshot_staging_.last_included_term != req.last_included_term ||
             snapshot_staging_.expected_size != req.total_size);
        if (identity_changed) {
            // A new snapshot supersedes whatever we were half-way through.
            // Keeping the old bytes would splice two different images together.
            RAFT_LOG("Discarding partial snapshot from leader %lu (index %lu -> %lu)",
                     snapshot_staging_.leader_id,
                     (unsigned long long)snapshot_staging_.last_included_index,
                     (unsigned long long)req.last_included_index);
            snapshot_staging_ = SnapshotStaging();
        }

        if (req.chunk_offset > snapshot_staging_.buffer.size()) {
            // A gap means a chunk was lost. Refusing makes the leader restart
            // from a known offset instead of us silently building a corrupt
            // image out of two disjoint ranges.
            RAFT_WARN("Rejecting snapshot chunk with gap: offset=%llu but only "
                      "%zu bytes staged",
                      (unsigned long long)req.chunk_offset,
                      snapshot_staging_.buffer.size());
            snapshot_staging_ = SnapshotStaging();
            return resp;
        }
        if (req.chunk_offset < snapshot_staging_.buffer.size()) {
            // Re-sending a chunk we already hold: drop the tail and retake it,
            // so a retry after a torn write cannot duplicate bytes.
            snapshot_staging_.buffer.resize(req.chunk_offset);
        }

        if (!snapshot_staging_.active) {
            snapshot_staging_.active = true;
            snapshot_staging_.term = req.term;
            snapshot_staging_.leader_id = req.leader_id;
            snapshot_staging_.last_included_index = req.last_included_index;
            snapshot_staging_.last_included_term = req.last_included_term;
            snapshot_staging_.expected_size = req.total_size;
        }
        snapshot_staging_.buffer.append(req.data);
        resp.accepted_len = snapshot_staging_.buffer.size();

        if (snapshot_staging_.buffer.size() < req.total_size) {
            resp.rejected = false;  // accepted, more to come
            return resp;
        }

        const std::string full = std::move(snapshot_staging_.buffer);
        snapshot_staging_ = SnapshotStaging();
        leader_id_ = req.leader_id;

        // Install BEFORE touching the log or indices. If this fails the node must
        // be left exactly as it was: keeping the old log means the leader can
        // still ship us the entries, whereas truncating first and then failing
        // would leave us unable to reach any committed state at all.
        if (state_machine_) {
            Status as = state_machine_->ApplySnapshot(full);
            if (!as.ok()) {
                RAFT_LOG("InstallSnapshot chunk from leader %lu (index=%lu) "
                         "failed to install: %s",
                         req.leader_id, (unsigned long long)req.last_included_index,
                         as.ToString().c_str());
                return resp;
            }
        }

        CommitInstalledSnapshot(req.last_included_index, req.last_included_term, full);
        resp.complete = true;
        resp.rejected = false;
    }
    return resp;
}

// ═══════════════════════════════════════════════════
// 心跳 & 日志复制
// ═══════════════════════════════════════════════════

void Raft::SendHeartbeat() {
    ReplicateLog(true);
}

void Raft::ReplicateLog(bool is_heartbeat) {
    // 快速路径：非 leader 不复制
    std::unique_lock<std::mutex> lock(mu_);
    if (role_ != RaftRole::kLeader) return;

    uint64_t leader_commit = commit_index_;
    uint64_t current_term = current_term_;
    uint64_t leader_id = opts_.node_id;

    for (size_t i = 0; i < opts_.peers.size(); ++i) {
        if (opts_.peers[i].id == opts_.node_id) continue;
        if (!opts_.peers[i].is_voter) continue;

        uint64_t next_idx = next_index_[i];
        uint64_t prev_log_index = next_idx - 1;
        uint64_t prev_log_term = GetTermForIndex(prev_log_index);

        // The follower needs entries we have already compacted away. Walking its
        // next_index_ back would have to reach index 1, but those entries only
        // exist inside our snapshot, so ship the snapshot instead.
        //
        // Note the condition is `next_idx <= last_snapshot_index_` — the previous
        // `next_idx < log_.size()` check described "follower needs entries", not
        // "follower is too far behind", and so could never distinguish the two.
        if (next_idx <= last_snapshot_index_) {
            lock.unlock();
            SendSnapshotToFollower(opts_.peers[i].id);
            lock.lock();
            if (role_ != RaftRole::kLeader || current_term_ != current_term) break;
            continue;
        }

        // 构造 AppendEntries 请求
        AppendEntriesRequest req;
        req.term = current_term;
        req.leader_id = leader_id;
        req.prev_log_index = prev_log_index;
        req.prev_log_term = prev_log_term;
        req.leader_commit = leader_commit;

        const size_t first_pos = LogPos(next_idx);
        if (!is_heartbeat && first_pos != kNoPos) {
            // 发送从 next_idx 开始的日志条目（最多 max_log_entries_per_append 条）
            const size_t end = std::min(log_.size(),
                                        first_pos + opts_.max_log_entries_per_append);
            for (size_t j = first_pos; j < end; ++j) {
                req.entries.push_back(log_[j]);
            }
        }

        // 释放锁避免 RPC 死锁
        lock.unlock();

        auto resp = rpc_->SendAppendEntries(opts_.peers[i].id, req);

        lock.lock();

        // 检查状态是否仍然有效
        if (role_ != RaftRole::kLeader || current_term_ != current_term) {
            break;
        }

        if (resp.term > current_term_) {
            BecomeFollower(resp.term);
            break;
        }

        if (resp.success) {
            if (!req.entries.empty()) {
                uint64_t last_entry_index = req.entries.back().index;
                next_index_[i] = last_entry_index + 1;
                match_index_[i] = last_entry_index;
            }
        } else {
            // 快速回退：使用 conflict_term/conflict_index 优化
            if (resp.conflict_term > 0) {
                // 在日志中查找 conflict_term 的最后一个条目
                uint64_t conflict_last_idx = 0;
                for (size_t idx = 0; idx < log_.size(); ++idx) {
                    const uint64_t abs = log_base_ + idx;
                    if (log_[idx].term == resp.conflict_term) {
                        conflict_last_idx = abs;
                    } else if (log_[idx].term > resp.conflict_term) {
                        break;
                    }
                }
                if (conflict_last_idx > 0) {
                    next_index_[i] = conflict_last_idx + 1;
                } else {
                    next_index_[i] = resp.conflict_index;
                }
            } else if (resp.conflict_index > 0) {
                next_index_[i] = resp.conflict_index;
            } else {
                // 降级回退
                if (next_index_[i] > 1) {
                    next_index_[i]--;
                }
            }
        }
    }

    // 更新 commit index
    AdvanceCommitIndex();
}

// ═══════════════════════════════════════════════════
// 提交管理
// ═══════════════════════════════════════════════════

void Raft::AdvanceCommitIndex() {
    if (role_ != RaftRole::kLeader) return;

    // Raft 论文 5.3：找到 N 使得 majority 的 match_index >= N
    // 并且 log[N].term == current_term
    uint64_t last_log_idx = GetLastLogIndex();

    for (uint64_t n = commit_index_ + 1; n <= last_log_idx; ++n) {
        const size_t pos = LogPos(n);
        if (pos == kNoPos) {
            // 已被快照取代，无需再提交
            continue;
        }
        if (log_[pos].term != current_term_) {
            // Entry from an older term: Raft only commits entries from the
            // current term (paper §5.4.2).
            continue;
        }

        uint64_t replicated_count = 1;  // leader 自己
        for (size_t i = 0; i < opts_.peers.size(); ++i) {
            if (opts_.peers[i].id == opts_.node_id) continue;
            if (!opts_.peers[i].is_voter) continue;
            if (match_index_[i] >= n) {
                replicated_count++;
            }
        }

        // Count the voters exactly once.
        //
        // This used to start at 1 ("the leader itself") and then add every voter
        // peer WITHOUT skipping self, so the local node was counted twice. For a
        // single-node cluster that produced voters=2 / majority=2 while only one
        // replica existed, so nothing ever committed; for a 3-node cluster it
        // demanded all 3 replicas instead of 2, defeating majority commit and
        // fault tolerance entirely.
        uint64_t voters = 0;
        bool self_counted = false;
        for (const auto& peer : opts_.peers) {
            if (!peer.is_voter) continue;
            ++voters;
            if (peer.id == opts_.node_id) self_counted = true;
        }
        // A node absent from its own peer list still votes for itself.
        if (!self_counted) ++voters;
        if (voters == 0) ++voters;  // degenerate config: at least self
        const uint64_t majority = voters / 2 + 1;

        if (replicated_count >= majority) {
            commit_index_ = n;
            commit_cv_.notify_all();  // 通知等待 ProposeSync 的线程
        } else {
            break;
        }
    }

    // 应用新提交的日志
    ApplyCommitted();
}

void Raft::ApplyCommitted() {
    while (last_applied_ < commit_index_) {
        const uint64_t next = last_applied_ + 1;
        const size_t pos = LogPos(next);
        if (pos == kNoPos) {
            // 这段日志已被快照取代，状态机已经持有对应镜像，直接跳过。
            last_applied_ = next;
            continue;
        }
        const auto& entry = log_[pos];
        last_applied_ = next;

        if (entry.type == RaftEntryType::kCommand && state_machine_) {
            // Hand the apply a result slot if anyone is waiting for this index.
            std::shared_ptr<PendingResult> slot;
            {
                auto it = pending_results_.find(last_applied_);
                if (it != pending_results_.end()) slot = it->second;
            }

            std::string result;
            std::string* out = nullptr;
            if (slot) {
                std::lock_guard<std::mutex> sl(slot->mu);
                if (!slot->done) out = &result;
            }

            mu_.unlock();
            state_machine_->Apply(entry.data, out);
            mu_.lock();

            if (slot) {
                {
                    std::lock_guard<std::mutex> sl(slot->mu);
                    if (!slot->done) {
                        slot->value = result;
                        slot->done = true;
                    }
                }
                commit_cv_.notify_all();
            }
        } else if (entry.type == RaftEntryType::kConfChange) {
            // 配置变更（简化实现：留待后续完善）
        }
        // kNoOp 不应用到状态机
    }
}

// ═══════════════════════════════════════════════════
// 日志查询
// ═══════════════════════════════════════════════════

uint64_t Raft::GetLastLogIndex() const {
    return LastLogIndex();
}

uint64_t Raft::GetLastLogTerm() const {
    return log_.back().term;
}

RaftLogEntry Raft::GetLogEntry(uint64_t index) const {
    const size_t pos = LogPos(index);
    if (pos != kNoPos) {
        return log_[pos];
    }
    return RaftLogEntry();
}

uint64_t Raft::GetTermForIndex(uint64_t index) const {
    const size_t pos = LogPos(index);
    if (pos != kNoPos) {
        return log_[pos].term;
    }
    return 0;
}

// ═══════════════════════════════════════════════════
// 持久化
// ═══════════════════════════════════════════════════

std::string Raft::RaftStatePath() const {
    return db_path_.empty() ? "./raft_state.dat" : db_path_ + "/raft_state.dat";
}

std::string Raft::RaftSnapshotPath() const {
    return db_path_.empty() ? "./raft_snapshot.dat" : db_path_ + "/raft_snapshot.dat";
}

Status Raft::SaveSnapshotData() {
    std::string data;
    {
        std::lock_guard<std::mutex> snap_lock(snapshot_mutex_);
        if (snapshot_data_.empty()) return Status::OK();
        data = snapshot_data_;
    }

    // The payload carries its own baseline so it can never be replayed against
    // a different last_included_index.
    std::string buf;
    raft_encoding::PutFixed64(&buf, last_snapshot_index_);
    raft_encoding::PutFixed64(&buf, last_snapshot_term_);
    raft_encoding::PutFixed32(&buf, static_cast<uint32_t>(data.size()));
    buf.append(data);

    std::string path = RaftSnapshotPath();
    std::string tmp_path = path + ".tmp";
    FILE* fp = fopen(tmp_path.c_str(), "wb");
    if (!fp) return Status::IOError("cannot write raft snapshot");
    size_t written = fwrite(buf.data(), 1, buf.size(), fp);
    fclose(fp);
    if (written != buf.size()) {
        std::remove(tmp_path.c_str());
        return Status::IOError("short write to raft snapshot");
    }
    // Same reason as raft_state.dat: std::rename() does not replace an existing
    // file on Windows.
    if (platform_rename(tmp_path.c_str(), path.c_str()) != 0) {
        std::remove(tmp_path.c_str());
        return Status::IOError("cannot atomically replace raft snapshot");
    }
    return Status::OK();
}

void Raft::LoadSnapshotData() {
    const std::string path = RaftSnapshotPath();
    FILE* fp = fopen(path.c_str(), "rb");
    if (!fp) return;

    fseek(fp, 0, SEEK_END);
    long file_size = ftell(fp);
    rewind(fp);
    if (file_size <= 0) { fclose(fp); return; }

    std::string buf(static_cast<size_t>(file_size), '\0');
    size_t read_bytes = fread(&buf[0], 1, static_cast<size_t>(file_size), fp);
    fclose(fp);
    if (read_bytes != static_cast<size_t>(file_size)) return;

    // GetFixed* is not bounds checked, so validate the layout up front.
    constexpr size_t kHeaderSize = 8 + 8 + 4;
    if (file_size <= static_cast<long>(kHeaderSize)) return;

    Slice slice(buf);
    size_t off = 0;
    uint64_t snap_index = raft_encoding::GetFixed64(slice, &off);
    uint64_t snap_term = raft_encoding::GetFixed64(slice, &off);
    uint32_t data_len = raft_encoding::GetFixed32(slice, &off);
    if (data_len == 0 || off + data_len != buf.size()) return;

    // Cross-check against the log that was just loaded. A payload whose baseline
    // does not match the log means the two files came from different points in
    // time (e.g. we crashed between saving them); adopting it would let the
    // leader hand out a snapshot that disagrees with its own log.
    if (snap_index < log_base_ || snap_index > LastLogIndex() ||
        GetTermForIndex(snap_index) != snap_term) {
        RAFT_LOG("Discarding raft snapshot: baseline index=%lu term=%lu does not "
                 "match the persisted log (holding %lu..%lu)",
                 snap_index, snap_term, log_base_, LastLogIndex());
        return;
    }

    {
        std::lock_guard<std::mutex> snap_lock(snapshot_mutex_);
        snapshot_data_.assign(slice.data() + off, data_len);
    }
    last_snapshot_index_ = snap_index;
    last_snapshot_term_ = snap_term;
}

Status Raft::SavePersistentState() {
    if (!opts_.enable_raft) return Status::OK();

    // 持久化格式：
    // current_term(8) | voted_for(8) | last_snapshot_index(8) | last_snapshot_term(8)
    // | log_count(4) | [entry_size(4) + entry_data] * log_count
    std::string buf;
    raft_encoding::PutFixed64(&buf, current_term_);
    raft_encoding::PutFixed64(&buf, voted_for_);
    raft_encoding::PutFixed64(&buf, last_snapshot_index_);
    raft_encoding::PutFixed64(&buf, last_snapshot_term_);

    uint32_t log_count = static_cast<uint32_t>(log_.size());
    raft_encoding::PutFixed32(&buf, log_count);
    for (const auto& entry : log_) {
        auto serialized = entry.Serialize();
        raft_encoding::PutFixed32(&buf, static_cast<uint32_t>(serialized.size()));
        buf.append(serialized);
    }

    // 原子写入：先写临时文件，再 rename
    std::string path = RaftStatePath();
    std::string tmp_path = path + ".tmp";
    {
        FILE* fp = fopen(tmp_path.c_str(), "wb");
        if (!fp) return Status::IOError("cannot write raft state");
        size_t written = fwrite(buf.data(), 1, buf.size(), fp);
        fclose(fp);
        if (written != buf.size()) {
            std::remove(tmp_path.c_str());
            return Status::IOError("short write to raft state");
        }
    }
    // Atomic replace: platform_rename(), not std::rename(). MSVC's rename()
    // fails with EEXIST when the target exists, so the first save would be the
    // last one -- every later term/vote/log update would be silently discarded
    // (see docs/缺陷修复方案.md F-2).
    if (platform_rename(tmp_path.c_str(), path.c_str()) != 0) {
        std::remove(tmp_path.c_str());
        return Status::IOError("cannot atomically replace raft state");
    }
    return Status::OK();
}

Status Raft::LoadPersistentState(const std::string& db_path) {
    if (!opts_.enable_raft) return Status::OK();
    db_path_ = db_path;

    std::string path = RaftStatePath();
    FILE* fp = fopen(path.c_str(), "rb");
    if (!fp) {
        // 文件不存在：首次启动，使用初始状态
        return Status::OK();
    }

    // 获取文件大小
    fseek(fp, 0, SEEK_END);
    long file_size = ftell(fp);
    rewind(fp);
    if (file_size <= 0) { fclose(fp); return Status::OK(); }

    std::string buf(static_cast<size_t>(file_size), '\0');
    size_t read_bytes = fread(&buf[0], 1, static_cast<size_t>(file_size), fp);
    fclose(fp);
    if (read_bytes != static_cast<size_t>(file_size)) {
        return Status::Corruption("failed to read raft state");
    }

    Slice slice(buf);
    size_t off = 0;
    current_term_ = raft_encoding::GetFixed64(slice, &off);
    voted_for_ = raft_encoding::GetFixed64(slice, &off);
    /* last_snapshot_index = */ raft_encoding::GetFixed64(slice, &off);
    /* last_snapshot_term = */ raft_encoding::GetFixed64(slice, &off);

    uint32_t log_count = raft_encoding::GetFixed32(slice, &off);
    log_.clear();
    log_base_ = 0;
    for (uint32_t i = 0; i < log_count; ++i) {
        uint32_t entry_size = raft_encoding::GetFixed32(slice, &off);
        Slice entry_slice(slice.data() + off, entry_size);
        off += entry_size;
        log_.push_back(RaftLogEntry::Deserialize(entry_slice));
    }

    if (log_.empty()) {
        RaftLogEntry dummy;
        dummy.index = 0;
        dummy.term = 0;
        dummy.type = RaftEntryType::kNoOp;
        log_.push_back(dummy);
        log_base_ = 0;
    } else {
        // log_[i] holds absolute index log_base_ + i, so the base is derivable
        // from the last entry. Old state files (written before compaction kept a
        // base) end at index size-1 and correctly derive base 0.
        log_base_ = log_.back().index - (log_.size() - 1);
        if (log_[0].index != log_base_) {
            // Entries are not evenly spaced -- the file is inconsistent.
            RAFT_LOG("Corrupt raft log: first entry index %lu but base %lu",
                     log_[0].index, log_base_);
            log_.clear();
            RaftLogEntry dummy;
            dummy.index = 0;
            dummy.term = 0;
            dummy.type = RaftEntryType::kNoOp;
            log_.push_back(dummy);
            log_base_ = 0;
        }
    }

    // The state file's snapshot fields were historically hardcoded to 0, so the
    // payload file is what actually restores the baseline after a restart.
    LoadSnapshotData();

    return Status::OK();
}

// ═══════════════════════════════════════════════════
// 快照管理 (Phase C)
// ═══════════════════════════════════════════════════

bool Raft::TriggerSnapshot() {
    std::lock_guard<std::mutex> lock(mu_);

    if (last_applied_ == 0) {
        RAFT_DEBUG("Cannot trigger snapshot: no applied entries");
        return false;
    }
    if (last_snapshot_index_ >= last_applied_) {
        RAFT_DEBUG("Snapshot already up-to-date: %lu", last_snapshot_index_);
        return true;
    }

    uint64_t snap_index = last_applied_;
    uint64_t snap_term = GetLogEntry(snap_index).term;

    // 从状态机获取快照数据
    std::string snap_data;
    if (state_machine_) {
        snap_data = state_machine_->TakeSnapshot();
    }
    if (snap_data.empty()) {
        // The state machine cannot produce a snapshot.
        //
        // Compacting anyway would discard the ONLY copy of every committed entry
        // below snap_index: followers that fell behind could no longer be caught
        // up, and InstallSnapshot would hand them a placeholder instead of real
        // state. The previous code substituted a fake "snapshot_at_<index>"
        // string and compacted anyway, which is a silent divergence path.
        //
        // Keeping the log is strictly better: the cluster stays correct and only
        // the leader's memory/disk grows. A real TakeSnapshot/ApplySnapshot
        // implementation is required to fix this properly
        // (see docs/缺陷修复方案.md, Phase 2.2).
        RAFT_LOG("State machine produced an empty snapshot; refusing to compact "
                 "log (snapshot_index=%lu, log_size=%zu)",
                 snap_index, log_.size());
        return false;
    }

    const uint64_t prev_snap_index = last_snapshot_index_;
    const uint64_t prev_snap_term = last_snapshot_term_;
    std::string prev_snap_data;
    {
        std::lock_guard<std::mutex> snap_lock(snapshot_mutex_);
        prev_snap_data = snapshot_data_;
    }

    last_snapshot_index_ = snap_index;
    last_snapshot_term_ = snap_term;

    {
        std::lock_guard<std::mutex> snap_lock(snapshot_mutex_);
        snapshot_data_ = snap_data;
    }

    // Persist the payload BEFORE compacting. If we crash after this point the
    // log on disk is still the full log, and the snapshot file's baseline still
    // matches it; the other order would leave a payload with no baseline.
    //
    // A failed write must abort the snapshot entirely. Compacting after it would
    // discard committed entries whose only surviving copy is the payload we
    // just failed to store — the exact failure mode F-10 was about.
    Status persisted = SaveSnapshotData();
    if (!persisted.ok()) {
        RAFT_LOG("Refusing to compact log: snapshot could not be persisted (%s)",
                 persisted.ToString().c_str());
        last_snapshot_index_ = prev_snap_index;
        last_snapshot_term_ = prev_snap_term;
        {
            std::lock_guard<std::mutex> snap_lock(snapshot_mutex_);
            snapshot_data_ = prev_snap_data;
        }
        return false;
    }

    // 压缩日志
    CompactLog(snap_index);

    RAFT_LOG("Snapshot generated: index=%lu, term=%lu, log_size=%zu",
             snap_index, snap_term, log_.size());

    SavePersistentState();
    return true;
}

void Raft::CompactLog(uint64_t snap_index) {
    const size_t pos = LogPos(snap_index);
    if (snap_index == 0 || pos == kNoPos) return;

    // Keep exactly the base entry at snap_index. Everything below it now lives
    // only inside the snapshot, so dropping it is the whole point of
    // compaction -- and keeping the entry itself lets AppendEntries still match
    // prev_log_index == last_snapshot_index_.
    RaftLogEntry base = std::move(log_[pos]);
    log_.clear();
    log_.push_back(std::move(base));
    log_base_ = snap_index;

    RAFT_DEBUG("Log compacted: snap_index=%lu, live entries=%zu",
               snap_index, log_.size());
}

// Entries still on this node: everything below the snapshot baseline is either
// absent or a compacted-away placeholder and can never be served, so it must not
// count towards the compaction threshold or the reported log size.
size_t Raft::LiveLogSize() const {
    return log_.size();
}

void Raft::CheckAndCompactLog() {
    std::unique_lock<std::mutex> lock(mu_);
    // Followers inherit the leader's baseline through InstallSnapshot. A
    // follower that compacted on its own would drop entries its leader may
    // still need to ship to a third node.
    if (role_ != RaftRole::kLeader) return;
    const size_t live = LiveLogSize();
    if (live > snapshot_threshold_) {
        RAFT_LOG("Log size %zu exceeds threshold %lu, triggering snapshot",
                 live, snapshot_threshold_);
        lock.unlock();
        TriggerSnapshot();
    }
}

void Raft::SendSnapshotToFollower(uint64_t follower_id) {
    uint64_t term = 0;
    uint64_t leader_id = 0;
    uint64_t snap_index = 0;
    uint64_t snap_term = 0;
    std::string data;
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (role_ != RaftRole::kLeader) return;
        term = current_term_;
        leader_id = opts_.node_id;
        snap_index = last_snapshot_index_;
        snap_term = last_snapshot_term_;
        {
            std::lock_guard<std::mutex> snap_lock(snapshot_mutex_);
            data = snapshot_data_;
        }
        // Nothing to ship yet: the follower will have to be walked forward
        // through the log instead.
        if (data.empty()) return;
    }

    const uint64_t total = data.size();

    // Small snapshots still go out as a single InstallSnapshot: one round trip,
    // and it keeps the well-exercised single-message path in use.
    if (total <= opts_.snapshot_chunk_size) {
        InstallSnapshotRequest req;
        req.term = term;
        req.leader_id = leader_id;
        req.last_included_index = snap_index;
        req.last_included_term = snap_term;
        req.data = data;

        auto resp = rpc_->SendInstallSnapshot(follower_id, req);

        std::lock_guard<std::mutex> lock(mu_);
        if (role_ != RaftRole::kLeader) return;
        if (resp.term > current_term_) {
            BecomeFollower(resp.term);
            return;
        }
        if (!resp.success) return;
        snapshot_sends_.erase(follower_id);
        for (size_t i = 0; i < opts_.peers.size(); ++i) {
            if (opts_.peers[i].id != follower_id) continue;
            next_index_[i] = snap_index + 1;
            match_index_[i] = snap_index;
            break;
        }
        RAFT_LOG("Installed snapshot on follower %lu up to index %lu",
                 follower_id, snap_index);
        return;
    }

    // Large snapshot: push bounded chunks per call so the replication thread is
    // not monopolised for the duration of the transfer. Progress is kept per
    // peer and resumed where it stopped.
    size_t budget = opts_.snapshot_send_budget;
    if (budget == 0) budget = opts_.snapshot_chunk_size;

    for (;;) {
        uint64_t sent = 0;
        {
            std::lock_guard<std::mutex> lock(mu_);
            if (role_ != RaftRole::kLeader) return;
            if (current_term_ != term) return;
            auto& st = snapshot_sends_[follower_id];
            // A newer snapshot (or a leadership change) invalidates progress.
            if (st.last_included_index != snap_index ||
                st.last_included_term != snap_term || st.total_size != total) {
                st = SnapshotSendState();
                st.last_included_index = snap_index;
                st.last_included_term = snap_term;
                st.total_size = total;
                st.sent = 0;
            }
            sent = st.sent;
        }

        if (sent >= total) {
            // Everything acknowledged; wait for the follower's completion
            // signal before advancing next_index_ (below).
            std::lock_guard<std::mutex> lock(mu_);
            snapshot_sends_.erase(follower_id);
            for (size_t i = 0; i < opts_.peers.size(); ++i) {
                if (opts_.peers[i].id != follower_id) continue;
                next_index_[i] = snap_index + 1;
                match_index_[i] = snap_index;
                break;
            }
            RAFT_LOG("Installed snapshot on follower %lu up to index %lu",
                     follower_id, snap_index);
            return;
        }

        const size_t take = std::min(opts_.snapshot_chunk_size,
                                     static_cast<size_t>(total - sent));
        InstallSnapshotChunkRequest req;
        req.term = term;
        req.leader_id = leader_id;
        req.last_included_index = snap_index;
        req.last_included_term = snap_term;
        req.chunk_offset = sent;
        req.total_size = total;
        req.is_last = (sent + take >= total);
        req.data = data.substr(static_cast<size_t>(sent), take);

        auto resp = rpc_->SendInstallSnapshotChunk(follower_id, req);

        {
            std::lock_guard<std::mutex> lock(mu_);
            if (role_ != RaftRole::kLeader) return;
            if (resp.term > current_term_) {
                BecomeFollower(resp.term);
                return;
            }
            if (resp.rejected) {
                // Forget progress so the next call restarts from zero rather
                // than resuming into a gap the follower will refuse again.
                snapshot_sends_.erase(follower_id);
                return;
            }
            if (resp.accepted_len <= sent) {
                // No forward progress: the follower is stuck or lying about
                // what it holds. Stop and retry from scratch next tick.
                if (resp.accepted_len < sent) snapshot_sends_.erase(follower_id);
                return;
            }
            snapshot_sends_[follower_id].sent = resp.accepted_len;
        }

        if (budget <= take) break;  // budget for this tick is spent
        budget -= take;
    }
}

std::string Raft::GetSnapshotData() const {
    std::lock_guard<std::mutex> snap_lock(snapshot_mutex_);
    return snapshot_data_;
}

// ═══════════════════════════════════════════════════
// 统计
// ═══════════════════════════════════════════════════

Raft::Stats Raft::GetStats() const {
    std::lock_guard<std::mutex> lock(mu_);
    Stats stats;
    stats.current_term = current_term_;
    stats.commit_index = commit_index_;
    stats.last_applied = last_applied_;
    stats.log_count = static_cast<uint64_t>(LiveLogSize());
    stats.role = role_;
    stats.leader_id = leader_id_;
    return stats;
}

// ═══════════════════════════════════════════════════
// 成员变更 (Phase D)
// ═══════════════════════════════════════════════════

RaftConfiguration Raft::GetConfiguration() const {
    std::lock_guard<std::mutex> lock(mu_);
    return configuration_;
}

bool Raft::IsMajority(int count) const {
    if (configuration_.IsJointConsensus()) {
        return configuration_.IsMajority(static_cast<uint32_t>(count));
    }
    int total = 0;
    for (const auto& peer : opts_.peers) {
        if (peer.is_voter) total++;
    }
    return count > total / 2;
}

bool Raft::AddPeer(uint64_t peer_node_id) {
    std::lock_guard<std::mutex> lock(mu_);
    if (role_ != RaftRole::kLeader) {
        RAFT_WARN("AddPeer called on non-leader node %lu", opts_.node_id);
        return false;
    }
    // 创建配置变更日志条目
    RaftLogEntry entry;
    entry.index = GetLastLogIndex() + 1;
    entry.term = current_term_;
    entry.type = RaftEntryType::kConfChange;
    entry.data = "ADD|" + std::to_string(peer_node_id);
    log_.push_back(entry);
    SavePersistentState();
    RAFT_LOG("AddPeer node %lu added to configuration at index %lu",
             peer_node_id, entry.index);
    return true;
}

bool Raft::RemovePeer(uint64_t peer_node_id) {
    std::lock_guard<std::mutex> lock(mu_);
    if (role_ != RaftRole::kLeader) {
        RAFT_WARN("RemovePeer called on non-leader node %lu", opts_.node_id);
        return false;
    }
    RaftLogEntry entry;
    entry.index = GetLastLogIndex() + 1;
    entry.term = current_term_;
    entry.type = RaftEntryType::kConfChange;
    entry.data = "REM|" + std::to_string(peer_node_id);
    log_.push_back(entry);
    SavePersistentState();
    RAFT_LOG("RemovePeer node %lu removed from configuration at index %lu",
             peer_node_id, entry.index);
    return true;
}

void Raft::ApplyConfigChange(uint64_t index, const std::string& command) {
    // 配置变更命令格式: "ADD|node_id" 或 "REMOVE|node_id"
    if (command.size() < 5) return;
    std::string op = command.substr(0, 3);
    if (command.size() < 5) return;
    uint64_t node_id = std::stoull(command.substr(4));

    if (op == "ADD") {
        configuration_.voters.insert(node_id);
        RAFT_LOG("Applied config change ADD node %lu at index %lu", node_id, index);
    } else if (op == "REM") {
        configuration_.voters.erase(node_id);
        RAFT_LOG("Applied config change REMOVE node %lu at index %lu", node_id, index);
    }
}

} // namespace lightkv
