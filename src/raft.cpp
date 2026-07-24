#include "lightkv/raft.h"
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

    // 尝试从持久化状态恢复
    db_path_ = db_path;
    LoadPersistentState(db_path);

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
    SavePersistentState();
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

bool Raft::ProposeSync(const std::string& data, int timeout_ms) {
    // 获取当前日志索引，用于等待提交确认
    uint64_t propose_idx;
    {
        std::lock_guard<std::mutex> lock(mu_);
        propose_idx = GetLastLogIndex() + 1;
    }

    // 先异步提交
    int64_t idx = Propose(data);
    if (idx < 0) return false;

    // 等待日志条目被提交 (commit_index >= propose_idx)
    std::unique_lock<std::mutex> lock(commit_mu_);
    if (timeout_ms <= 0) timeout_ms = 5000;
    return commit_cv_.wait_for(lock, std::chrono::milliseconds(timeout_ms),
        [this, propose_idx]() { return commit_index_ >= propose_idx; });
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

    // 3. 检查 prev_log_index 处的日志是否匹配
    if (req.prev_log_index > 0) {
        if (req.prev_log_index >= log_.size()) {
            // prev_log_index 超出范围
            resp.conflict_index = static_cast<uint64_t>(log_.size());
            resp.conflict_term = 0;
            return resp;
        }

        const auto& existing = log_[req.prev_log_index];
        if (existing.term != req.prev_log_term) {
            // 任期冲突：返回冲突 term 的第一个 index
            resp.conflict_term = existing.term;
            resp.conflict_index = req.prev_log_index;
            // 向前扫描到该 term 的第一个 index
            while (resp.conflict_index > 0 &&
                   log_[resp.conflict_index - 1].term == existing.term) {
                resp.conflict_index--;
            }
            return resp;
        }
    }

    // 4. 处理新日志条目
    if (!req.entries.empty()) {
        uint64_t log_index = req.prev_log_index + 1;
        for (const auto& entry : req.entries) {
            if (entry.index < static_cast<uint64_t>(log_.size())) {
                // 检查是否冲突
                if (log_[entry.index].term != entry.term) {
                    // 截断冲突部分
                    log_.resize(entry.index);
                }
            }
            if (entry.index >= static_cast<uint64_t>(log_.size())) {
                log_.push_back(entry);
            }
            log_index = entry.index + 1;
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

    // 应用快照
    if (state_machine_) {
        state_machine_->ApplySnapshot(req.data);
    }

    // 截断日志到 last_included_index
    // 如果日志中已有该 index，保留它
    uint64_t cut_index = req.last_included_index;
    if (cut_index >= log_.size()) {
        // 快照比日志新，创建一个新的 dummy 条目
        RaftLogEntry dummy;
        dummy.index = cut_index;
        dummy.term = req.last_included_term;
        dummy.type = RaftEntryType::kNoOp;
        log_.clear();
        log_.push_back(dummy);
    } else {
        // 截断到 cut_index
        log_.resize(cut_index + 1);
    }

    commit_index_ = cut_index;
    last_applied_ = cut_index;
    SavePersistentState();

    resp.success = true;
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

        // 构造 AppendEntries 请求
        AppendEntriesRequest req;
        req.term = current_term;
        req.leader_id = leader_id;
        req.prev_log_index = prev_log_index;
        req.prev_log_term = prev_log_term;
        req.leader_commit = leader_commit;

        if (!is_heartbeat && next_idx < static_cast<uint64_t>(log_.size())) {
            // 发送从 next_idx 开始的日志条目（最多 max_log_entries_per_append 条）
            uint64_t end = std::min(
                static_cast<uint64_t>(log_.size()),
                next_idx + opts_.max_log_entries_per_append);
            for (uint64_t j = next_idx; j < end; ++j) {
                req.entries.push_back(log_[j]);
            }
        }

        // 如果日志落后太多，使用安装快照（简化：暂时先不发快照）
        if (!is_heartbeat && next_idx < static_cast<uint64_t>(log_.size()) &&
            log_[prev_log_index].term != prev_log_term) {
            // 这里简化为：让 follower 通过 AppendEntries 的冲突处理来回退
            // 生产级实现应在此处触发 InstallSnapshot
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
                for (uint64_t idx = 1; idx < log_.size(); ++idx) {
                    if (log_[idx].term == resp.conflict_term) {
                        conflict_last_idx = idx;
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
        if (log_[n].term != current_term_) continue;

        uint64_t replicated_count = 1;  // leader 自己
        for (size_t i = 0; i < opts_.peers.size(); ++i) {
            if (opts_.peers[i].id == opts_.node_id) continue;
            if (!opts_.peers[i].is_voter) continue;
            if (match_index_[i] >= n) {
                replicated_count++;
            }
        }

        uint64_t voters = 1;
        for (const auto& peer : opts_.peers) {
            if (peer.is_voter) voters++;
        }
        uint64_t majority = voters / 2 + 1;

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
        last_applied_++;
        const auto& entry = log_[last_applied_];

        if (entry.type == RaftEntryType::kCommand && state_machine_) {
            mu_.unlock();
            state_machine_->Apply(entry.data);
            mu_.lock();
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
    return static_cast<uint64_t>(log_.size()) - 1;
}

uint64_t Raft::GetLastLogTerm() const {
    return log_.back().term;
}

RaftLogEntry Raft::GetLogEntry(uint64_t index) const {
    if (index < log_.size()) {
        return log_[index];
    }
    return RaftLogEntry();
}

uint64_t Raft::GetTermForIndex(uint64_t index) const {
    if (index < log_.size()) {
        return log_[index].term;
    }
    return 0;
}

// ═══════════════════════════════════════════════════
// 持久化
// ═══════════════════════════════════════════════════

std::string Raft::RaftStatePath() const {
    return db_path_.empty() ? "./raft_state.dat" : db_path_ + "/raft_state.dat";
}

Status Raft::SavePersistentState() {
    if (!opts_.enable_raft) return Status::OK();

    // 持久化格式：
    // current_term(8) | voted_for(8) | last_snapshot_index(8) | last_snapshot_term(8)
    // | log_count(4) | [entry_size(4) + entry_data] * log_count
    std::string buf;
    raft_encoding::PutFixed64(&buf, current_term_);
    raft_encoding::PutFixed64(&buf, voted_for_);
    raft_encoding::PutFixed64(&buf, 0);  // last_snapshot_index (预留)
    raft_encoding::PutFixed64(&buf, 0);  // last_snapshot_term (预留)

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
    if (std::rename(tmp_path.c_str(), path.c_str()) != 0) {
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
    // 确保 index 0 是 dummy 条目
    RaftLogEntry dummy;
    dummy.index = 0;
    dummy.term = 0;
    dummy.type = RaftEntryType::kNoOp;
    log_.push_back(dummy);

    for (uint32_t i = 0; i < log_count; ++i) {
        uint32_t entry_size = raft_encoding::GetFixed32(slice, &off);
        Slice entry_slice(slice.data() + off, entry_size);
        off += entry_size;
        log_.push_back(RaftLogEntry::Deserialize(entry_slice));
    }

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
        snap_data = "snapshot_at_" + std::to_string(snap_index);
    }

    last_snapshot_index_ = snap_index;
    last_snapshot_term_ = snap_term;

    {
        std::lock_guard<std::mutex> snap_lock(snapshot_mutex_);
        snapshot_data_ = snap_data;
    }

    // 压缩日志
    CompactLog(snap_index);

    RAFT_LOG("Snapshot generated: index=%lu, term=%lu, log_size=%zu",
             snap_index, snap_term, log_.size());

    SavePersistentState();
    return true;
}

void Raft::CompactLog(uint64_t snap_index) {
    if (snap_index == 0 || snap_index >= log_.size()) return;

    // 保留 index 0 的 dummy 条目和快照之后的日志
    std::vector<RaftLogEntry> new_log;
    RaftLogEntry dummy;
    dummy.index = 0;
    dummy.term = 0;
    dummy.type = RaftEntryType::kNoOp;
    new_log.push_back(dummy);

    for (uint64_t i = snap_index; i < log_.size(); ++i) {
        new_log.push_back(log_[i]);
    }
    log_.swap(new_log);

    RAFT_DEBUG("Log compacted: snap_index=%lu, new_log_size=%zu",
               snap_index, log_.size());
}

void Raft::CheckAndCompactLog() {
    std::unique_lock<std::mutex> lock(mu_);
    if (log_.size() > snapshot_threshold_) {
        RAFT_LOG("Log size %zu exceeds threshold %lu, triggering snapshot",
                 log_.size(), snapshot_threshold_);
        lock.unlock();
        TriggerSnapshot();
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
    stats.log_count = static_cast<uint64_t>(log_.size());
    stats.role = role_;
    stats.leader_id = leader_id_;
    return stats;
}

} // namespace lightkv
