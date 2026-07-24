# LightKV Raft 共识引擎升级完善方案

> 基于 TinyDB 的 Raft 实现参考，针对 LightKV 现有 Raft 实现的差距分析
> 日期：2026-07-24
> 状态：✅ 已完成（Phase A~E 全部交付）
> 提交：`1cbb998`(A) `89113bb`(B) `8f0945c`(C) `604b7a5`(D) `f581469`(E)

---

## 目录

1. [现状评估](#1-现状评估)
2. [差距分析](#2-差距分析)
3. [升级方案](#3-升级方案)
4. [实施路线图](#4-实施路线图)
5. [附录：TinyDB Raft 关键设计要点](#5-附录tinydb-raft-关键设计要点)

---

## 1. 现状评估

### 1.1 LightKV Raft 当前能力

LightKV 在 Phase 3 中实现了基础的 Raft 共识引擎，具备以下能力：

| 功能 | 状态 | 说明 | Phase |
|------|------|------|-------|
| Leader 选举 | ✅ 基本实现 | 随机超时 + 多数派投票，超时范围 150-300ms | Phase 3 |
| 日志复制 | ✅ 基本实现 | AppendEntries RPC + 快速回退优化 | Phase 3 |
| 安全性保证 | ✅ 基本实现 | 选举限制（日志新旧比较）、日志匹配 | Phase 3 |
| NoOp 提交 | ✅ 已实现 | 新 Leader 上任自动提交 NoOp 日志 | Phase 3 |
| RPC 网络层 | ✅ 基本实现 | TCP 协议 + 自定义序列化 | Phase 3 |
| 持久化 | ✅ 已修复 | `SavePersistentState` 原子文件写入 + `LoadPersistentState` 恢复 | Phase A |
| 同步提交 | ✅ 已实现 | `ProposeSync()` + `condition_variable` 等待提交确认 | Phase A |
| Leader 转发 | ✅ 已实现 | `SendForwardPropose` + `GetLeaderAddr()` 自动转发 | Phase A |
| 自动重连 | ✅ 已实现 | `ConnectToPeer` 检测断连并自动重建 | Phase A |
| 调试日志 | ✅ 已实现 | `RAFT_LOG/WARN/DEBUG` 分级日志，关键函数埋点 | Phase A |
| 数据复制集成 | ✅ 已实现 | 写操作通过 `Raft::ProposeSync` 复制到多数节点 | Phase B |
| 读写分离 | ✅ 已实现 | 读操作本地 `DB::Get`，写操作路由 Raft | Phase B |
| 日志压缩/快照 | ✅ 已实现 | `TriggerSnapshot`/`CompactLog`/`InstallSnapshot` RPC | Phase C |
| 自动快照 | ✅ 已实现 | `CheckAndCompactLog` 阈值 10000 自动触发 | Phase C |
| 成员变更 | ✅ 已实现 | `RaftConfiguration` Joint Consensus + `AddPeer`/`RemovePeer` | Phase D |
| Raft 状态查询 | ✅ 已实现 | `CLUSTER RAFT-STATS` 命令 | Phase E |

### 1.2 三节点集群测试发现问题

| # | 问题 | 表现 | 严重度 |
|---|------|------|--------|
| 1 | RPC 回调为 nullptr 导致 segfault | `Raft` 创建时传入 `nullptr` 作为 RPC 回调 | 🔴 P0 |
| 2 | 数据不通过 Raft 复制 | `SET`/`DELETE` 走直接 DB 路径，不经过 `Raft::Propose()` | 🔴 P0 |
| 3 | `CLUSTER INFO` 显示 0 节点 | `ClusterManager` 未从 Raft 配置初始化 | 🟠 P1 |
| 4 | 无 Raft 状态查询接口 | 无法通过 RESP 命令查询 Leader/term/commitIndex | 🟡 P2 |
| 5 | 持久化未写入文件 | `SavePersistentState` 方法体为空 | 🟡 P2 |

---

## 2. 差距分析

### 2.1 架构设计对比

```
LightKV 当前架构:
┌──────────────────────────────────────────┐
│  Raft (共识引擎)                          │
│   ┌──────────────┐  ┌──────────────────┐ │
│   │ 选举/复制逻辑 │  │ RaftServer(RPC)  │ │
│   │ (in Raft类)   │  │ (accept+处理)    │ │
│   └──────────────┘  └──────────────────┘ │
│   RPC 通信: RaftRPC 接口 (单个文件)        │
└──────────────────────────────────────────┘

TinyDB 推荐架构:
┌──────────────────────────────────────────────┐
│  RaftNode (共识引擎)                           │
│   ├── 选举/复制/提交逻辑                       │
│   ├── 持久化 (Persist/Restore)                │
│   ├── 快照管理 (TriggerSnapshot/CompactLog)    │
│   └── 成员变更 (Joint Consensus)              │
├──────────────────────────────────────────────┤
│  RaftRpcClient (对端 RPC 客户端, 每个 peer 一个) │
│   ├── SendRequestVote / SendAppendEntries      │
│   ├── SendInstallSnapshot / SendForwardPropose │
│   └── 连接池管理 + 自动重连                     │
├──────────────────────────────────────────────┤
│  RaftRpcServer (入站 RPC 服务端)               │
│   ├── 独立监听线程 + accept 循环               │
│   ├── SetHandlers(回调注册)                   │
│   └── 并发处理客户端连接                        │
└──────────────────────────────────────────────┘
```

### 2.2 关键差距详解

| 差距项 | LightKV 现状 | TinyDB 做法 | 升级工作量 |
|--------|-------------|-------------|-----------|
| **同步提交** | 仅 `Propose()`，无法等待提交确认 | `ProposeSync()` + `condition_variable` + `commit_cv_` | 小 |
| **持久化** | `SavePersistentState` 空实现 | `Persist()` 写文件 + `Restore()` 恢复，持久化 term/votedFor/log/snapshot | 中 |
| **快照/日志压缩** | 无 | `TriggerSnapshot()` 定期压缩日志 + `InstallSnapshot` RPC 传输给落后节点 | 大 |
| **成员变更** | 无 | `RaftConfiguration` Joint Consensus + `AddPeer`/`RemovePeer` + `ConfigChangeCallback` | 大 |
| **Leader 转发** | 非 Leader 拒绝请求 | `SendForwardPropose` 自动转发到 Leader，对客户端透明 | 中 |
| **自动重连** | 连接状态不恢复 | `GetPeerClient` 检测断开并重建连接 | 小 |
| **集群管理集成** | 独立 ClusterManager，与 Raft 分离 | `ClusterManager::SetLeader` 统一管理 | 中 |
| **调试日志** | 无 | 完整 `LOG_INFO/WARN/DEBUG` 分级日志 | 小 |
| **RPC 超时** | 硬编码无超时 | `poll` + 可配置 timeout_ms | 小 |
| **序列化** | 手写字节编解码，分散在各函数 | 集中 `Serialize/Deserialize` 函数，`std::vector<uint8_t>` | 小 |

---

## 3. 升级方案

### 3.1 Phase A：核心修复与补全（预估：2-3 天）

#### A.1 修复持久化

```cpp
// 在 Raft 类中添加文件持久化（参考 TinyDB RaftNode::Persist）

bool Raft::SavePersistentState(const std::string& db_path) {
    std::string path = db_path + "/raft_state.dat";
    std::ofstream f(path, std::ios::binary);
    
    // 持久化: current_term | voted_for | last_snapshot_index | last_snapshot_term | log_count | [entries...]
    WriteFixed64(f, current_term_);
    WriteFixed64(f, voted_for_);
    WriteFixed64(f, last_snapshot_index_);
    WriteFixed64(f, last_snapshot_term_);
    WriteFixed64(f, log_.size());
    for (const auto& entry : log_) {
        WriteFixed64(f, entry.index);
        WriteFixed64(f, entry.term);
        WriteString32(f, entry.data);
    }
    return true;
}

bool Raft::LoadPersistentState(const std::string& db_path) {
    std::string path = db_path + "/raft_state.dat";
    std::ifstream f(path, std::ios::binary);
    if (!f.is_open()) return false;
    
    // 反序列化 ...
    ReadFixed64(f, &current_term_);
    ReadFixed64(f, &voted_for_);
    // ...
    return true;
}
```

**涉及文件**：`src/raft.cpp`、`include/lightkv/raft.h`

#### A.2 添加同步提交

```cpp
// 在 Raft 类中添加 ProposeSync（参考 TinyDB RaftNode::ProposeSync）

bool Raft::ProposeSync(const std::string& data, int timeout_ms) {
    uint64_t propose_index;
    {
        std::lock_guard<std::mutex> lock(mu_);
        propose_index = GetLastLogIndex() + 1;
    }
    
    if (!Propose(data)) return false;
    
    // 等待提交确认
    std::unique_lock<std::mutex> lock(commit_mu_);
    if (timeout_ms <= 0) timeout_ms = 5000;
    return commit_cv_.wait_for(lock, std::chrono::milliseconds(timeout_ms),
        [this, propose_index]() { return commit_index_ >= propose_index; });
}
```

**涉及文件**：`src/raft.cpp`、`include/lightkv/raft.h`

#### A.3 添加 Leader 转发

```cpp
// 在 RaftServer/RaftRPC 中添加 ForwardPropose 消息类型（参考 TinyDB SendForwardPropose）

enum class RpcType : uint8_t {
    // ... 现有类型 ...
    kForwardPropose = 5,
    kForwardProposeReply = 6,
};

// 客户端发送
bool RaftRpcClient::SendForwardPropose(const std::string& command) {
    RaftRpcMessage msg(RaftRpcType::FORWARD_PROPOSE, SerializeString(command));
    return SendRaftRpcMessage(fd_, msg);
}

// 服务端处理
void HandleIncomingRPC(int conn_fd) {
    // ...
    case RpcType::kForwardPropose: {
        auto command = DeserializeString(body);
        raft_->Propose(command);
        break;
    }
}
```

**涉及文件**：`include/lightkv/raft_server.h`、`src/raft_server.cpp`

#### A.4 添加自动重连

```cpp
// 参考 TinyDB RaftNode::GetPeerClient 和 ReplicateLog 中的重连逻辑

int RaftServer::ConnectToPeer(uint64_t peer_id) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = connections_.find(peer_id);
    if (it == connections_.end()) return -1;
    auto& conn = it->second;
    
    // 如果连接存在但检测到断开，重建连接
    if (conn.fd >= 0) {
        int error = 0;
        socklen_t len = sizeof(error);
        getsockopt(conn.fd, SOL_SOCKET, SO_ERROR, &error, &len);
        if (error == 0) return conn.fd;  // 连接正常
        // 连接已断开，关闭并重建
        ::close(conn.fd);
        conn.fd = -1;
    }
    
    // 建立新连接
    conn.fd = Connect(conn.host, conn.port);
    return conn.fd;
}
```

**涉及文件**：`src/raft_server.cpp`

#### A.5 添加调试日志

```cpp
// 添加分级日志宏（参考 TinyDB LOG_INFO/LOG_WARN/LOG_DEBUG）

#define RAFT_LOG_INFO(fmt, ...)  fprintf(stderr, "[Raft] " fmt "\n", ##__VA_ARGS__)
#define RAFT_LOG_WARN(fmt, ...)  fprintf(stderr, "[Raft WARN] " fmt "\n", ##__VA_ARGS__)
#ifdef DEBUG
#define RAFT_LOG_DEBUG(fmt, ...) fprintf(stderr, "[Raft DEBUG] " fmt "\n", ##__VA_ARGS__)
#else
#define RAFT_LOG_DEBUG(fmt, ...) ((void)0)
#endif
```

关键日志埋点位置：
- `BecomeCandidate` / `BecomeLeader` / `BecomeFollower`
- `StartElection`：记录候选人 ID、term、请求节点数、得票数
- `HandleAppendEntries`：记录来源 leader、term、日志范围
- `ReplicateLog`：记录目标节点、发送条目数、结果
- `Propose` / `ProposeSync`：记录索引、等待结果

**涉及文件**：`src/raft.cpp`（全部函数添加日志）

---

### 3.2 Phase B：数据复制集成（预估：3-5 天）

#### B.1 写操作路由到 Raft

当前 LightKV 的 `SET`/`DELETE` 走直接 DB 路径。需要改为：

```
当前路径:
Client → Server::handle_set → DB::Put() → MemTable + WAL

升级后路径:
Client → Server::handle_set → DB::Put() (Leader 模式)
                                  ↓
                            Raft::ProposeSync()
                                  ↓
                            Raft 复制到多数节点
                                  ↓
                            RaftStateMachine::Apply()
                                  ↓
                            DB::Put() (仅在 Apply 回调中写入)
```

**关键改动点**：

```cpp
// server.cpp — handle_set 方法
std::string Server::Impl::handle_set(const std::vector<std::string>& args) {
    // ... 参数解析 ...
    
    if (raft_mode_) {
        // Raft 模式：通过 Raft 复制
        std::string command = EncodeWriteOp("SET", key, value);
        if (!raft_->ProposeSync(command)) {
            // 非 Leader：自动转发
            std::string leader_addr = raft_->GetLeaderAddr();
            return resp_error("MOVED " + leader_addr);
        }
        return resp_ok();
    } else {
        // 单机模式：直接写入
        db_->Put(wopts, key, value);
        return resp_ok();
    }
}
```

**涉及文件**：`src/server.cpp`、`include/lightkv/db.h`、`include/lightkv/raft.h`

#### B.2 Raft 状态机 Apply 回调

```cpp
// 参考 TinyDB RaftNode::SetApplyCallback

class RaftDBStateMachine : public RaftStateMachine {
public:
    void Apply(const std::string& command_data) override {
        // 解析命令: "SET|key|value" 或 "DEL|key|" 或 "BATCH|count|..."
        auto [op, key, value] = DecodeWriteOp(command_data);
        
        WriteOptions wopts;
        wopts.sync = false;
        if (op == "SET") {
            db_->Put(wopts, key, value);
        } else if (op == "DEL") {
            db_->Delete(wopts, key);
        }
        // ...
    }
};
```

#### B.3 读写分离

```cpp
std::string Server::Impl::handle_get(const std::vector<std::string>& args) {
    // 读操作在所有节点都可以执行（Leader 或 Follower）
    // 但强一致性读需要从 Leader 读取
    if (opts_.strongly_consistent_read && !is_leader()) {
        // 转发到 Leader
        return resp_error("MOVED " + raft_->GetLeaderAddr());
    }
    
    // 本地读取
    std::string value;
    db_->Get(read_opts, key, &value);
    // ...
}
```

---

### 3.3 Phase C：日志压缩与快照（预估：3-5 天）

#### C.1 TriggerSnapshot

```cpp
// 参考 TinyDB RaftNode::TriggerSnapshot

bool Raft::TriggerSnapshot() {
    std::lock_guard<std::mutex> lock(mu_);
    
    // 获取当前已应用的日志索引
    uint64_t snap_index = last_applied_;
    if (snap_index <= last_snapshot_index_) return true;
    
    // 从状态机获取快照数据
    std::string snap_data;
    if (state_machine_) {
        snap_data = state_machine_->TakeSnapshot();
    }
    
    // 更新快照元数据
    last_snapshot_index_ = snap_index;
    last_snapshot_term_ = GetLogEntry(snap_index).term;
    {
        std::lock_guard<std::mutex> snap_lock(snapshot_mutex_);
        snapshot_data_ = snap_data;
    }
    
    // 压缩日志：删除快照之前的条目
    CompactLog(snap_index);
    
    SavePersistentState();
    return true;
}
```

#### C.2 InstallSnapshot RPC

```cpp
// 参考 TinyDB SendSnapshotToFollower

void Raft::SendSnapshotToFollower(uint64_t follower_id) {
    InstallSnapshotRequest req;
    req.term = current_term_;
    req.leader_id = node_id_;
    req.last_included_index = last_snapshot_index_;
    req.last_included_term = last_snapshot_term_;
    req.data = snapshot_data_;
    req.done = true;
    
    auto resp = rpc_->SendInstallSnapshot(follower_id, req);
    if (resp.success) {
        next_index_[follower_id] = last_snapshot_index_ + 1;
        match_index_[follower_id] = last_snapshot_index_;
    }
}
```

#### C.3 自动日志压缩阈值

```cpp
// 在 Leader 心跳循环中定期检查

void Raft::CheckAndCompactLog() {
    // 当日志条目数超过阈值时触发快照
    if (log_.size() > snapshot_threshold_) {
        TriggerSnapshot();
    }
}
```

---

### 3.4 Phase D：成员变更（预估：3-5 天）

#### D.1 RaftConfiguration

```cpp
// 参考 TinyDB RaftConfiguration（Joint Consensus）

struct RaftConfiguration {
    std::set<uint64_t> voters;        // 当前配置投票节点
    std::set<uint64_t> new_voters;     // 新配置投票节点（过渡期）
    
    bool IsJointConsensus() const { return !new_voters.empty(); }
    
    bool IsVoter(uint64_t node_id) const {
        return voters.count(node_id) > 0 ||
               (!new_voters.empty() && new_voters.count(node_id) > 0);
    }
    
    bool IsMajority(uint32_t votes) const {
        if (new_voters.empty()) return votes > voters.size() / 2;
        return votes > GetVoterCount() / 2;  // 联合配置
    }
};
```

#### D.2 AddPeer / RemovePeer

```cpp
bool Raft::AddPeer(uint64_t peer_node_id) {
    if (role_ != RaftRole::kLeader) return false;
    
    // 创建配置变更日志
    RaftLogEntry entry;
    entry.index = GetLastLogIndex() + 1;
    entry.term = current_term_;
    entry.type = RaftEntryType::kConfChange;
    entry.data = EncodeConfChange(ConfChangeType::kAdd, peer_node_id);
    log_.push_back(entry);
    
    return true;
}
```

---

### 3.5 Phase E：集群管理集成与可观测性（预估：2-3 天）

#### E.1 CLUSTER 命令增强

添加以下 Raft 状态查询命令：

```
CLUSTER RAFT-STATS  → 返回 Leader ID、Term、CommitIndex、LogCount、Role
CLUSTER RAFT-LOG    → 返回日志条目摘要
CLUSTER RAFT-SNAPSHOT → 手动触发快照
CLUSTER RAFT-ADD-PEER id host port  → 添加节点
CLUSTER RAFT-REMOVE-PEER id         → 移除节点
```

#### E.2 ClusterManager 与 Raft 深度集成

```cpp
// ClusterManager 统一管理集群状态
// 复用 TinyDB 的 NodeInfo / NodeState 模型

void Raft::BecomeLeader() {
    // ... 现有逻辑 ...
    if (cluster_) {
        cluster_->SetLeader(node_id_);
    }
}
```

---

## 4. 实施路线图

### 4.1 版本规划

| 阶段 | 内容 | 状态 | 提交 |
|------|------|------|------|
| **Phase A** | 核心修复与补全 | ✅ 已完成 | `1cbb998` |
| A.1 | 修复持久化 | ✅ 已完成 | A |
| A.2 | 添加同步提交 ProposeSync | ✅ 已完成 | A |
| A.3 | 添加 Leader 转发 ForwardPropose | ✅ 已完成 | A |
| A.4 | 添加自动重连 | ✅ 已完成 | A |
| A.5 | 添加调试日志 | ✅ 已完成 | A |
| **Phase B** | 数据复制集成 | ✅ 已完成 | `89113bb` |
| B.1 | 写操作路由到 Raft | ✅ 已完成 | B |
| B.2 | Raft 状态机 Apply 回调 | ✅ 已完成 | B |
| B.3 | 读写分离 | ✅ 已完成 | B |
| **Phase C** | 日志压缩与快照 | ✅ 已完成 | `8f0945c` |
| C.1 | TriggerSnapshot | ✅ 已完成 | C |
| C.2 | InstallSnapshot RPC | ✅ 已完成 | C |
| C.3 | 自动日志压缩阈值 | ✅ 已完成 | C |
| **Phase D** | 成员变更 | ✅ 已完成 | `604b7a5` |
| D.1 | RaftConfiguration Joint Consensus | ✅ 已完成 | D |
| D.2 | AddPeer / RemovePeer | ✅ 已完成 | D |
| **Phase E** | 集群管理集成与可观测性 | ✅ 已完成 | `f581469` |
| E.1 | CLUSTER RAFT-STATS 等命令 | ✅ 已完成 | E |
| E.2 | ClusterManager 深度集成 | ✅ 已完成 | E |

### 4.2 依赖关系

```
Phase A (修复补全) ──────┐
                          │
Phase B (数据复制) ───────┼─── 依赖 Phase A (需要持久化 + 同步提交)
                          │
Phase C (快照/压缩) ──────┤─── 依赖 Phase B (需要 Apply 回调)
                          │
Phase D (成员变更) ───────┘─── 依赖 Phase C (需要快照为落后节点同步)
                          │
Phase E (集群集成) ───────── 依赖 Phase A~D (所有前置功能)
```

### 4.3 关键里程碑

| 里程碑 | 完成后效果 | 验收标准 |
|--------|-----------|---------|
| **M1: Phase A 完成** | Raft 引擎具备生产级基础能力 | 3 节点集群可正常选举、日志复制持久化 |
| **M2: Phase B 完成** | 数据通过 Raft 复制 | `SET key` 在 Node 1 提交后 Node 2/3 能 `GET` 到 |
| **M3: Phase C 完成** | 日志自动压缩不膨胀 | 100 万条写入后日志大小可控 |
| **M4: Phase D 完成** | 动态扩缩容 | 不停机添加/移除节点 |
| **M5: Phase E 完成** | 集群可观测 + 管理 | `CLUSTER RAFT-STATS` 返回完整状态 |

---

## 5. 附录：TinyDB Raft 关键设计要点

### 5.1 架构分层

TinyDB 的 Raft 实现分为三个独立组件：

```
RaftNode (共识引擎)
├── 选举逻辑 (StartElection, HandleElectionTimeout)
├── 日志复制 (ReplicateLog, LeaderHeartbeatLoop)
├── 提交推进 (AdvanceCommitIndex)
├── 持久化 (Persist/Restore)
├── 快照管理 (TriggerSnapshot/CheckAndCompactLog)
└── 成员变更 (AddPeer/RemovePeer/ApplyConfigChange)

RaftRpcClient (对端通信)
├── 连接管理 (Connect/Disconnect/IsConnected)
├── 发送 RPC (SendRequestVote/SendAppendEntries/SendInstallSnapshot)
└── 转发请求 (SendForwardPropose)

RaftRpcServer (服务端监听)
├── 独立线程监听 (ListenerLoop)
├── 回调注册 (SetHandlers/SetInstallSnapshotHandler)
└── 并发处理 (HandleClient)
```

### 5.2 关键数据结构

```cpp
// 1. RaftLogEntry — 日志条目
struct RaftLogEntry {
    uint64_t index;        // 日志索引（从 1 开始）
    uint64_t term;         // 创建时的任期
    std::string command;   // 命令数据
    uint64_t timestamp_ms; // 时间戳（用于统计）
};

// 2. RaftConfiguration — 集群配置（Joint Consensus）
struct RaftConfiguration {
    std::set<uint32_t> voters;      // 当前投票节点
    std::set<uint32_t> new_voters;  // 新配置投票节点（过渡期）
    
    bool IsJointConsensus() const;
    bool IsVoter(uint32_t node_id) const;
    bool IsMajority(uint32_t votes) const;
};
```

### 5.3 线程模型

```
主线程 (调用 Start/Stop/Propose):
  - 加锁 → 追加日志/更新状态 → 释放锁

选举线程 (独立线程):
  循环:
    1. sleep(随机超时)
    2. 检查最近是否联系 Leader
    3. 超时 → 发起选举 (StartElection)

Leader 心跳线程 (独立线程):
  循环:
    1. sleep(100ms)
    2. 向所有 Follower 发送 AppendEntries
    3. 定期检查日志压缩 (每 50 轮)
```

### 5.4 序列化格式

```cpp
// 基于 std::vector<uint8_t> 的序列化
// 优点: 类型安全、内存连续、可扩展

// 写入
static void WriteUint64(std::vector<uint8_t>& buf, uint64_t v);
static void WriteString(std::vector<uint8_t>& buf, const std::string& s);

// 读取
static uint64_t ReadUint64(const uint8_t* data, size_t& offset);
static std::string ReadString(const uint8_t* data, size_t& offset);
```

### 5.5 TCP 帧格式

```
[RaftRpcMessage 线格式]
┌──────────┬──────────────┬────────────────────────────┐
│ type(1B) │ body_len(4B) │ body(body_len bytes)       │
│ (uint8)  │ (network order) │ (序列化后的 RPC 数据)    │
└──────────┴──────────────┴────────────────────────────┘
```

### 5.6 持久化文件格式

```
[raft_{node_id}.dat 文件格式]
┌────────────┬──────────┬────────────────┬──────────────┬──────────┬────────────────────┐
│ term(8B)   │ voted(4B)│ snap_idx(8B)   │ snap_term(8B)│ log_cnt  │ [entries...]       │
│ (uint64)   │ (uint32) │ (uint64)       │ (uint64)     │ (uint64) │ (repeated)         │
└────────────┴──────────┴────────────────┴──────────────┴──────────┴────────────────────┘

每条日志条目:
┌──────────┬────────┬──────────┬────────────┐
│ idx(8B)  │ term(8)│ cmd_len  │ command    │
│ (uint64) │        │ (uint32) │ (bytes)    │
└──────────┴────────┴──────────┴────────────┘
```

---

## 修订历史

| 版本 | 日期 | 修订内容 |
|------|------|---------|
| 1.0 | 2026-07-24 | 初稿，基于 TinyDB Raft 实现的差距分析与升级方案 |
