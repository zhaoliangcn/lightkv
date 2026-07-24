#pragma once

#include "slice.h"
#include "status.h"
#include "options.h"
#include <cstdint>
#include <string>
#include <vector>
#include <memory>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <chrono>
#include <functional>
#include <random>
#include <set>
#include <map>

namespace lightkv {

// ─── Raft 角色 ───
enum class RaftRole : uint8_t {
    kFollower = 0,
    kCandidate = 1,
    kLeader = 2,
};

// ─── Raft 日志条目类型 ───
enum class RaftEntryType : uint8_t {
    kCommand = 0,       // 普通状态机命令 (Put/Delete/Batch)
    kConfChange = 1,    // 集群配置变更
    kNoOp = 2,          // 空日志（新 leader 提交用）
};

// ─── Raft 日志条目 ───
struct RaftLogEntry {
    uint64_t index;      // 日志索引（从 1 开始）
    uint64_t term;       // 该条目所在任期
    RaftEntryType type;
    std::string data;    // 序列化的命令或配置

    // 序列化/反序列化
    std::string Serialize() const;
    static RaftLogEntry Deserialize(const Slice& data);
};

// ─── 集群节点 ───
struct RaftPeer {
    uint64_t id;
    std::string host;
    uint16_t port;
    bool is_voter;       // true = voting member, false = non-voter/learner
};

// ─── Raft 持久状态（需落盘） ───
struct RaftPersistentState {
    uint64_t current_term = 0;
    uint64_t voted_for = 0;   // 0 = none
    std::vector<RaftLogEntry> log_entries;  // index 0 是 dummy entry (term=0)
};

// ─── AppendEntries RPC 请求 ───
struct AppendEntriesRequest {
    uint64_t term;
    uint64_t leader_id;
    uint64_t prev_log_index;
    uint64_t prev_log_term;
    std::vector<RaftLogEntry> entries;
    uint64_t leader_commit;
};

// ─── AppendEntries RPC 响应 ───
struct AppendEntriesResponse {
    uint64_t term;
    bool success;
    // 快速回退优化：follower 返回冲突 term 的第一个 index
    uint64_t conflict_term = 0;
    uint64_t conflict_index = 0;
};

// ─── RequestVote RPC 请求 ───
struct RequestVoteRequest {
    uint64_t term;
    uint64_t candidate_id;
    uint64_t last_log_index;
    uint64_t last_log_term;
};

// ─── RequestVote RPC 响应 ───
struct RequestVoteResponse {
    uint64_t term;
    bool vote_granted;
};

// ─── 安装快照 RPC（用于落后太多的 follower） ───
struct InstallSnapshotRequest {
    uint64_t term;
    uint64_t leader_id;
    uint64_t last_included_index;
    uint64_t last_included_term;
    std::string data;       // 快照数据（DB snapshot）
    bool done;
};

struct InstallSnapshotResponse {
    uint64_t term;
    bool success;
};

// ─── 集群配置（Joint Consensus 成员变更用） ───
struct RaftConfiguration {
    std::set<uint64_t> voters;       // 当前配置投票节点
    std::set<uint64_t> new_voters;   // 新配置投票节点（过渡期）

    bool IsJointConsensus() const { return !new_voters.empty(); }

    bool IsVoter(uint64_t node_id) const {
        return voters.count(node_id) > 0 ||
               (!new_voters.empty() && new_voters.count(node_id) > 0);
    }

    size_t GetVoterCount() const {
        if (new_voters.empty()) return voters.size();
        std::set<uint64_t> all = voters;
        all.insert(new_voters.begin(), new_voters.end());
        return all.size();
    }

    bool IsMajority(uint32_t votes) const {
        if (new_voters.empty()) return votes > voters.size() / 2;
        return votes > GetVoterCount() / 2;
    }
};

// ─── 回调接口：状态机应用和 RPC 发送 ───
class RaftStateMachine {
public:
    virtual ~RaftStateMachine() = default;
    // 应用一条已提交的日志到状态机（DB）
    virtual void Apply(const std::string& command_data) = 0;
    // 生成当前状态快照
    virtual std::string TakeSnapshot() = 0;
    // 从快照恢复
    virtual void ApplySnapshot(const std::string& data) = 0;
};

class RaftRPC {
public:
    virtual ~RaftRPC() = default;
    virtual AppendEntriesResponse SendAppendEntries(uint64_t peer_id, const AppendEntriesRequest& req) = 0;
    virtual RequestVoteResponse SendRequestVote(uint64_t peer_id, const RequestVoteRequest& req) = 0;
    virtual InstallSnapshotResponse SendInstallSnapshot(uint64_t peer_id, const InstallSnapshotRequest& req) = 0;
};

// ─── 配置 ───
struct RaftOptions {
    uint64_t node_id = 0;            // 本节点 ID
    std::vector<RaftPeer> peers;     // 集群所有节点
    bool enable_raft = false;        // 非集群模式下关闭 Raft

    // Raft 超时（milliseconds）
    int election_timeout_min_ms = 150;
    int election_timeout_max_ms = 300;
    int heartbeat_interval_ms = 50;

    // 日志
    size_t max_log_entries_per_append = 100;  // 每次 AppendEntries 最多发送条目数
    size_t max_log_size = 64 * 1024 * 1024;   // 日志总大小限制（64MB）
};

// ─── Raft 共识引擎 ───
//
// 线程安全：所有公开方法可多线程调用
// 非阻塞：Apply/Propose 不会长时间阻塞
//
// 用法：
//   1. 创建 Raft 实例
//   2. 设置 state_machine 和 rpc 回调
//   3. 调用 Start() 开始运行
//   4. 调用 Propose() 提交命令
//   5. 调用 Stop() 停止
class Raft {
public:
    Raft(const RaftOptions& opts, RaftStateMachine* state_machine, RaftRPC* rpc);
    ~Raft();

    Raft(const Raft&) = delete;
    Raft& operator=(const Raft&) = delete;

    // ─── 生命周期 ───
    // 初始化 Raft 引擎，db_path 用于持久化存储路径
    Status Initialize(const std::string& db_path = "");
    void Start();   // 启动后台选举/心跳线程
    void Stop();

    // ─── 客户端接口 ───
    // 提交一条命令给 Raft 复制（仅 leader 接受）
    // 返回日志索引，-1 表示非 leader
    int64_t Propose(const std::string& data);

    // 同步提交：等待日志条目被提交后返回
    // timeout_ms: 超时毫秒（0 表示默认 5000ms）
    bool ProposeSync(const std::string& data, int timeout_ms = 5000);

    // 设置 RPC 回调（必须在 Start() 之前调用）
    void SetRPC(RaftRPC* rpc) {
        std::lock_guard<std::mutex> lock(mu_);
        rpc_ = rpc;
    }

    // 当前角色
    RaftRole GetRole() const;
    uint64_t GetLeaderId() const;
    uint64_t GetCurrentTerm() const;
    // 获取 Leader 地址（格式 "host:raft_port"）
    std::string GetLeaderAddr() const;

    // ─── RPC 处理（由 RaftServer 调用） ───
    AppendEntriesResponse HandleAppendEntries(const AppendEntriesRequest& req);
    RequestVoteResponse HandleRequestVote(const RequestVoteRequest& req);
    InstallSnapshotResponse HandleInstallSnapshot(const InstallSnapshotRequest& req);

    // ─── 持久化 ───
    Status SavePersistentState();
    Status LoadPersistentState(const std::string& db_path);

    // ─── 统计 ───
    struct Stats {
        uint64_t current_term = 0;
        uint64_t commit_index = 0;
        uint64_t last_applied = 0;
        uint64_t log_count = 0;
        RaftRole role = RaftRole::kFollower;
        uint64_t leader_id = 0;
    };
    Stats GetStats() const;

private:
    // ─── 内部逻辑 ───
    void BecomeFollower(uint64_t term);
    void BecomeCandidate();
    void BecomeLeader();

    // 选举
    void StartElection();
    void HandleElectionTimeout();

    // 心跳 / 日志复制
    void SendHeartbeat();
    void ReplicateLog(bool is_heartbeat);

    // 提交
    void AdvanceCommitIndex();

    // 应用已提交日志到状态机
    void ApplyCommitted();

    // 日志管理
    uint64_t GetLastLogIndex() const;
    uint64_t GetLastLogTerm() const;
    RaftLogEntry GetLogEntry(uint64_t index) const;
    uint64_t GetTermForIndex(uint64_t index) const;

    // ─── 成员变更 (Phase D) ───
    bool AddPeer(uint64_t peer_node_id);
    bool RemovePeer(uint64_t peer_node_id);
    RaftConfiguration GetConfiguration() const;
    void ApplyConfigChange(uint64_t index, const std::string& command);
    bool IsMajority(int count) const;

    // ─── 快照管理 (Phase C) ───
    // 手动触发快照，压缩日志
    bool TriggerSnapshot();
    // 压缩日志：删除快照之前的条目
    void CompactLog(uint64_t snap_index);
    // 检查并自动触发快照（由复制线程周期性调用）
    void CheckAndCompactLog();
    // 向落后节点发送快照（由 Leader 调用）
    void SendSnapshotToFollower(uint64_t follower_id);
    // 获取最后快照索引
    uint64_t GetLastSnapshotIndex() const { return last_snapshot_index_; }
    // 获取快照数据（用于 InstallSnapshot 传输）
    std::string GetSnapshotData() const;

    // ─── 持久化路径
    std::string RaftStatePath() const;

    // ─── 配置 ───
    RaftOptions opts_;
    std::string db_path_;  // 持久化路径前缀

    // ─── 持久化状态（持 mu_ 访问） ───
    uint64_t current_term_{0};
    uint64_t voted_for_{0};
    std::vector<RaftLogEntry> log_;

    // ─── 易失状态 ───
    uint64_t commit_index_{0};
    uint64_t last_applied_{0};

    // Leader 易失状态（仅 leader 使用）
    std::vector<uint64_t> next_index_;
    std::vector<uint64_t> match_index_;

    // ─── 运行时状态 ───
    RaftRole role_{RaftRole::kFollower};
    uint64_t leader_id_{0};
    std::atomic<bool> running_{false};

    // 选举超时
    std::chrono::steady_clock::time_point last_heartbeat_;
    int election_timeout_ms_;

    // 回调
    RaftStateMachine* state_machine_;
    RaftRPC* rpc_;

    // 后台线程
    std::thread election_thread_;
    std::thread replication_thread_;
    mutable std::mutex mu_;

    // 同步提交等待
    mutable std::mutex commit_mu_;
    std::condition_variable commit_cv_;

    // 随机数生成器
    std::mt19937 rng_;

    // ─── 快照状态 (Phase C) ───
    uint64_t last_snapshot_index_{0};
    uint64_t last_snapshot_term_{0};
    mutable std::mutex snapshot_mutex_;
    std::string snapshot_data_;
    uint64_t snapshot_threshold_ = 10000;  // 日志条目数超过此值触发自动快照

    // ─── 成员变更 (Phase D) ───
    RaftConfiguration configuration_;  // 当前集群配置（Joint Consensus）
};

// ─── 序列化辅助 ───
namespace raft_encoding {
    void PutFixed32(std::string* dst, uint32_t v);
    void PutFixed64(std::string* dst, uint64_t v);
    uint32_t GetFixed32(const Slice& src, size_t* offset);
    uint64_t GetFixed64(const Slice& src, size_t* offset);
}

} // namespace lightkv
