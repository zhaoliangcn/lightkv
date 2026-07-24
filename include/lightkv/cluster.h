#pragma once

#include "slice.h"
#include "status.h"
#include "options.h"
#include "raft.h"
#include <cstdint>
#include <string>
#include <vector>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <set>

namespace lightkv {

// ─── Redis 集群规范 ───
// 16384 hash slots, CRC16(key) & 16383
constexpr uint16_t kHashSlotCount = 16384;

// ─── 节点角色（在集群中的角色，独立于 Raft 角色） ───
enum class ClusterNodeRole : uint8_t {
    kMaster = 0,
    kReplica = 1,
};

// ─── 节点状态 ───
enum class ClusterNodeState : uint8_t {
    kOnline = 0,
    kOffline = 1,
    kJoining = 2,
    kFailed = 3,
};

// ─── 集群节点信息 ───
struct ClusterNode {
    uint64_t id;                    // 节点 ID（与 Raft node_id 一致）
    std::string host;
    uint16_t port;                  // RESP 端口
    uint16_t raft_port;             // Raft 内部通信端口
    ClusterNodeRole role;
    ClusterNodeState state;
    std::string master_id;          // replica 所属 master（master 为空）
    // 节点负责的 hash 槽范围（master 才有）
    std::vector<std::pair<uint16_t, uint16_t>> slot_ranges;
};

// ─── 集群状态快照（用于 CLUSTER NODES / CLUSTER SLOTS） ───
struct ClusterState {
    uint64_t current_epoch = 0;     // 集群纪元
    std::vector<ClusterNode> nodes; // 所有节点
    uint64_t node_id;               // 本节点 ID
};

// ─── 集群管理器 ───
//
// 职责：
// 1. hash 槽分配与查询 (CRC16 → slot → node)
// 2. CLUSTER 命令实现（CLUSTER SLOTS / NODES / KEYSLOT / INFO）
// 3. 集成 Raft 共识管理集群成员变更
// 4. MOVED/ASK 重定向响应生成
//
// 线程安全：所有公开方法可多线程调用
class ClusterManager {
public:
    ClusterManager();
    ~ClusterManager();

    ClusterManager(const ClusterManager&) = delete;
    ClusterManager& operator=(const ClusterManager&) = delete;

    // ─── 初始化 ───
    // 从 Raft 配置和 Options 初始化集群状态
    Status Initialize(const RaftOptions& raft_opts, const Options& db_opts);

    // ─── Hash Slot 查询 ───
    // CRC16(key) & 16383 → slot
    static uint16_t KeyToSlot(const Slice& key);

    // slot → 负责该 slot 的 master 节点 ID（0 = 未分配）
    uint64_t SlotToNode(uint16_t slot) const;

    // 本节点是否负责指定的 slot
    bool IsSlotLocal(uint16_t slot) const;

    // ─── 键路由 ───
    // 检查 key 是否应由本节点处理
    // 如果不是，返回应该重定向到的目标节点 (host, port)
    // 返回 true = 本地处理, false = 需要重定向
    bool ShouldHandleKey(const Slice& key, std::string* redirect_host = nullptr,
                         uint16_t* redirect_port = nullptr) const;

    // ─── 集群命令 ───
    // CLUSTER KEYSLOT key → 返回 hash slot 编号
    uint16_t ClusterKeySlot(const Slice& key) const;

    // CLUSTER NODES → 返回节点信息字符串（Redis 兼容格式）
    std::string ClusterNodes() const;

    // CLUSTER SLOTS → 返回 slot 分配数组（Redis 兼容格式）
    std::string ClusterSlots() const;

    // CLUSTER INFO → 返回集群信息字符串
    std::string ClusterInfo() const;

    // CLUSTER COUNTKEYSINSLOT slot → 返回该 slot 的 key 数量（简化：暂不实现精确计数）
    uint64_t ClusterCountKeysInSlot(uint16_t slot) const;

    // CLUSTER GETKEYSINSLOT slot count → 返回该 slot 的 key 列表（简化：暂不实现）
    std::vector<std::string> ClusterGetKeysInSlot(uint16_t slot, uint64_t count) const;

    // ─── 集群管理（简化实现） ───
    // 添加节点到集群
    Status AddNode(const ClusterNode& node);
    // 移除节点
    Status RemoveNode(uint64_t node_id);
    // 分配 slot 范围给节点
    Status AssignSlotRange(uint64_t node_id, uint16_t start_slot, uint16_t end_slot);
    // 标记节点失败
    Status MarkNodeFailed(uint64_t node_id);

    // ─── 统计 ───
    uint64_t NodeCount() const;
    uint64_t SlotCount() const;

    // 本节点 ID
    uint64_t NodeId() const { return node_id_; }

private:
    // CRC16 查表法
    static uint16_t CRC16(const char* buf, size_t len);

    // 构建 slot → node 映射表
    void RebuildSlotMap();

    // 从当前 nodes_ 重建 slot_map_
    void RebuildSlotMapLocked();

    // CRC16 查表（Redis 标准多项式）
    static const uint16_t crc16_table_[256];

    uint64_t node_id_{0};
    std::vector<ClusterNode> nodes_;
    // slot → node_id 快速查询表
    std::vector<uint64_t> slot_map_;  // 大小 kHashSlotCount
    std::vector<uint16_t> local_slots_;  // 本节点负责的 slot 列表

    // 本节点的 RESP 端口（用于 MOVED 重定向）
    std::string local_host_;
    uint16_t local_port_{6379};
    uint16_t local_raft_port_{16379};

    mutable std::mutex mu_;
};

} // namespace lightkv
