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
#include <thread>
#include <chrono>
#include <functional>
#include <random>
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
    Status Initialize();
    void Start();   // 启动后台选举/心跳线程
    void Stop();

    // ─── 客户端接口 ───
    // 提交一条命令给 Raft 复制（仅 leader 接受）
    // 返回日志索引，-1 表示非 leader
    int64_t Propose(const std::string& data);

    // 设置 RPC 回调（必须在 Start() 之前调用）
    void SetRPC(RaftRPC* rpc) {
        std::lock_guard<std::mutex> lock(mu_);
        rpc_ = rpc;
    }

    // 当前角色
    RaftRole GetRole() const;
    uint64_t GetLeaderId() const;
    uint64_t GetCurrentTerm() const;

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

    // 持久化路径
    std::string RaftStatePath(const std::string& db_path) const;

    // ─── 配置 ───
    RaftOptions opts_;

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

    // 随机数生成器
    std::mt19937 rng_;
};

// ─── 序列化辅助 ───
namespace raft_encoding {
    void PutFixed32(std::string* dst, uint32_t v);
    void PutFixed64(std::string* dst, uint64_t v);
    uint32_t GetFixed32(const Slice& src, size_t* offset);
    uint64_t GetFixed64(const Slice& src, size_t* offset);
}

} // namespace lightkv
