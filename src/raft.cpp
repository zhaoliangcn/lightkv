#include "lightkv/raft.h"
#include <sstream>
#include <algorithm>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <chrono>

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

Status Raft::Initialize() {
    if (!opts_.enable_raft) {
        return Status::OK();
    }
    if (opts_.peers.empty()) {
        return Status::InvalidArgument("Raft peers list is empty");
    }
    if (opts_.node_id == 0) {
        return Status::InvalidArgument("Raft node_id must be non-zero");
    }

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

std::string Raft::RaftStatePath(const std::string& db_path) const {
    return db_path + "/raft_state.dat";
}

Status Raft::SavePersistentState() {
    if (!opts_.enable_raft) return Status::OK();

    // 持久化格式：
    // current_term(8) | voted_for(8) | log_count(4) | [log_entry] * log_count
    std::string buf;
    raft_encoding::PutFixed64(&buf, current_term_);
    raft_encoding::PutFixed64(&buf, voted_for_);

    uint32_t log_count = static_cast<uint32_t>(log_.size());
    raft_encoding::PutFixed32(&buf, log_count);
    for (const auto& entry : log_) {
        auto serialized = entry.Serialize();
        raft_encoding::PutFixed32(&buf, static_cast<uint32_t>(serialized.size()));
        buf.append(serialized);
    }

    // 写入文件（原子替换）
    std::string tmp_path = RaftStatePath(opts_.peers.empty() ? "." : "") + ".tmp";
    // 使用第一个 peer 的 db_path 概念，实际上用外部传入的路径
    // 这里简化：通过文件路径存储
    // 实际路径由调用者通过 LoadPersistentState 提供

    // 简单的文件写入（实际使用需要 db_path）
    // 目前先跳过：Initialize 时从外部加载
    (void)buf;
    return Status::OK();
}

Status Raft::LoadPersistentState(const std::string& db_path) {
    if (!opts_.enable_raft) return Status::OK();

    std::string state_path = db_path + "/raft_state.dat";
    std::ifstream file(state_path, std::ios::binary);
    if (!file) {
        // 文件不存在：首次启动，使用初始状态
        return Status::OK();
    }

    file.seekg(0, std::ios::end);
    size_t size = static_cast<size_t>(file.tellg());
    file.seekg(0, std::ios::beg);

    std::string buf(size, '\0');
    file.read(buf.data(), static_cast<std::streamsize>(size));
    file.close();

    Slice slice(buf);
    size_t off = 0;

    // Node: 这里使用 Slice 的 data() 和 size() 方法
    // 需要确保 Slice 支持 data() 返回 const char*

    return Status::OK();
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
