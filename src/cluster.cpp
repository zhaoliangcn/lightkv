#include "lightkv/cluster.h"
#include <sstream>
#include <algorithm>

namespace lightkv {

// ─── CRC16 查表（Redis 标准多项式 x^16 + x^15 + x^2 + 1） ───
const uint16_t ClusterManager::crc16_table_[256] = {
    0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7,
    0x8108, 0x9129, 0xa14a, 0xb16b, 0xc18c, 0xd1ad, 0xe1ce, 0xf1ef,
    0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6,
    0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de,
    0x2462, 0x3443, 0x0420, 0x1401, 0x64e6, 0x74c7, 0x44a4, 0x5485,
    0xa56a, 0xb54b, 0x8528, 0x9509, 0xe5ee, 0xf5cf, 0xc5ac, 0xd58d,
    0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695, 0x46b4,
    0xb75b, 0xa77a, 0x9719, 0x8738, 0xf7df, 0xe7fe, 0xd79d, 0xc7bc,
    0x48c4, 0x58e5, 0x6886, 0x78a7, 0x0840, 0x1861, 0x2802, 0x3823,
    0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948, 0x9969, 0xa90a, 0xb92b,
    0x5af5, 0x4ad4, 0x7ab7, 0x6a96, 0x1a71, 0x0a50, 0x3a33, 0x2a12,
    0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a,
    0x6ca6, 0x7c87, 0x4ce4, 0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41,
    0xedae, 0xfd8f, 0xcdec, 0xddcd, 0xad2a, 0xbd0b, 0x8d68, 0x9d49,
    0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0x0e70,
    0xff9f, 0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a, 0x9f59, 0x8f78,
    0x9188, 0x81a9, 0xb1ca, 0xa1eb, 0xd10c, 0xc12d, 0xf14e, 0xe16f,
    0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046, 0x6067,
    0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e,
    0x02b1, 0x1290, 0x22f3, 0x32d2, 0x4235, 0x5214, 0x6277, 0x7256,
    0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e, 0xe54f, 0xd52c, 0xc50d,
    0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405,
    0xa7db, 0xb7fa, 0x8799, 0x97b8, 0xe75f, 0xf77e, 0xc71d, 0xd73c,
    0x26d3, 0x36f2, 0x0691, 0x16b0, 0x6657, 0x7676, 0x4615, 0x5634,
    0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9, 0xb98a, 0xa9ab,
    0x5844, 0x4865, 0x7806, 0x6827, 0x18c0, 0x08e1, 0x3882, 0x28a3,
    0xcb7d, 0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a,
    0x4a75, 0x5a54, 0x6a37, 0x7a16, 0x0af1, 0x1ad0, 0x2ab3, 0x3a92,
    0xfd2e, 0xed0f, 0xdd6c, 0xcd4d, 0xbdaa, 0xad8b, 0x9de8, 0x8dc9,
    0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1,
    0xef1f, 0xff3e, 0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8,
    0x6e17, 0x7e36, 0x4e55, 0x5e74, 0x2e93, 0x3eb2, 0x0ed1, 0x1ef0
};

uint16_t ClusterManager::CRC16(const char* buf, size_t len) {
    uint16_t crc = 0;
    for (size_t i = 0; i < len; ++i) {
        crc = static_cast<uint16_t>((crc << 8) ^ crc16_table_[((crc >> 8) ^ static_cast<uint8_t>(buf[i])) & 0xff]);
    }
    return crc;
}

// ═══════════════════════════════════════════════════════════════
// 构造函数 & 初始化
// ═══════════════════════════════════════════════════════════════

ClusterManager::ClusterManager() {
    slot_map_.resize(kHashSlotCount, 0);
}

ClusterManager::~ClusterManager() = default;

Status ClusterManager::Initialize(const RaftOptions& raft_opts, const Options& db_opts) {
    std::lock_guard<std::mutex> lock(mu_);

    node_id_ = raft_opts.node_id;
    local_host_ = db_opts.raft_host;
    local_port_ = db_opts.raft_port;
    // 使用 raft 端口偏移作为 RESP 端口（实际应从配置获取）
    // 这里简化为：RESP 端口 = raft_port - 10000（默认 6379）
    if (db_opts.raft_port > 10000) {
        local_port_ = static_cast<uint16_t>(db_opts.raft_port - 10000);
    } else {
        local_port_ = 6379;
    }
    local_raft_port_ = db_opts.raft_port;

    // 从 Raft peers 构建集群节点列表
    for (const auto& peer : raft_opts.peers) {
        ClusterNode node;
        node.id = peer.id;
        // 从 peer 信息中解析
        node.host = peer.host;
        node.port = (peer.port > 10000) ? static_cast<uint16_t>(peer.port - 10000) : 6379;
        node.raft_port = peer.port;
        node.role = peer.is_voter ? ClusterNodeRole::kMaster : ClusterNodeRole::kReplica;
        node.state = ClusterNodeState::kOnline;
        nodes_.push_back(node);
    }

    // 如果只有一个节点，分配全部 slot
    if (raft_opts.peers.size() == 1 || raft_opts.peers.size() <= 1) {
        // 单节点或首次启动，分配全部 slot 给本节点
        for (size_t i = 0; i < nodes_.size(); ++i) {
            if (nodes_[i].id == node_id_) {
                nodes_[i].slot_ranges.push_back({0, kHashSlotCount - 1});
                break;
            }
        }
    } else {
        // 多节点：均分 slot
        uint16_t slots_per_node = kHashSlotCount / static_cast<uint16_t>(raft_opts.peers.size());
        uint16_t current_slot = 0;
        for (size_t i = 0; i < nodes_.size(); ++i) {
            uint16_t end_slot = (i == nodes_.size() - 1) ?
                static_cast<uint16_t>(kHashSlotCount - 1) :
                static_cast<uint16_t>(current_slot + slots_per_node - 1);
            nodes_[i].slot_ranges.push_back({current_slot, end_slot});
            current_slot = end_slot + 1;
        }
    }

    RebuildSlotMapLocked();
    return Status::OK();
}

// ═══════════════════════════════════════════════════════════════
// Hash Slot 查询
// ═══════════════════════════════════════════════════════════════

uint16_t ClusterManager::KeyToSlot(const Slice& key) {
    // Redis 兼容：CRC16(key) & 16383
    // 如果 key 包含 hash tag {tag}，只对 tag 部分计算
    std::string_view sv(key.data(), key.size());
    size_t start = 0;
    size_t end = key.size();

    // 检查 hash tag
    auto brace_start = sv.find('{');
    if (brace_start != std::string_view::npos) {
        auto brace_end = sv.find('}', brace_start + 1);
        if (brace_end != std::string_view::npos && brace_end > brace_start + 1) {
            start = brace_start + 1;
            end = brace_end;
        }
    }

    uint16_t crc = CRC16(key.data() + start, end - start);
    return crc & 0x3FFF;  // 16383
}

uint64_t ClusterManager::SlotToNode(uint16_t slot) const {
    std::lock_guard<std::mutex> lock(mu_);
    if (slot < slot_map_.size()) {
        return slot_map_[slot];
    }
    return 0;
}

bool ClusterManager::IsSlotLocal(uint16_t slot) const {
    std::lock_guard<std::mutex> lock(mu_);
    if (slot < slot_map_.size()) {
        return slot_map_[slot] == node_id_;
    }
    return false;
}

bool ClusterManager::ShouldHandleKey(const Slice& key, std::string* redirect_host,
                                      uint16_t* redirect_port) const {
    uint16_t slot = KeyToSlot(key);
    std::lock_guard<std::mutex> lock(mu_);

    if (slot >= slot_map_.size()) return true;
    uint64_t owner = slot_map_[slot];
    if (owner == node_id_ || owner == 0) return true;

    // 需要重定向，查找目标节点
    if (redirect_host && redirect_port) {
        for (const auto& node : nodes_) {
            if (node.id == owner) {
                *redirect_host = node.host;
                *redirect_port = node.port;
                break;
            }
        }
    }
    return false;
}

// ═══════════════════════════════════════════════════════════════
// 集群命令
// ═══════════════════════════════════════════════════════════════

uint16_t ClusterManager::ClusterKeySlot(const Slice& key) const {
    return KeyToSlot(key);
}

std::string ClusterManager::ClusterNodes() const {
    std::lock_guard<std::mutex> lock(mu_);

    // Redis CLUSTER NODES 兼容格式：
    // <id> <host:port@raft_port> <flags> <master> <ping_sent> <pong_recv> <epoch> <link> <slot_range>
    std::string result;
    for (const auto& node : nodes_) {
        // id
        result += std::to_string(node.id) + " ";
        // host:port@raft_port
        result += node.host + ":" + std::to_string(node.port);
        result += "@" + std::to_string(node.raft_port) + " ";

        // flags
        if (node.id == node_id_) result += "myself,";
        if (node.role == ClusterNodeRole::kMaster) result += "master";
        else result += "slave";
        result += " ";

        // master
        result += (node.role == ClusterNodeRole::kReplica && !node.master_id.empty())
                  ? node.master_id : "-";
        result += " ";

        // ping/pong/epoch (simplified)
        result += "0 0 " + std::to_string(0) + " ";
        // link state
        result += "connected ";
        // slot ranges
        for (const auto& range : node.slot_ranges) {
            result += std::to_string(range.first) + "-" + std::to_string(range.second) + " ";
        }
        result += "\n";
    }
    return result;
}

std::string ClusterManager::ClusterSlots() const {
    std::lock_guard<std::mutex> lock(mu_);

    // Redis CLUSTER SLOTS 兼容格式：
    // 1) 1) (integer) start_slot
    //    2) (integer) end_slot
    //    3) 1) "host"
    //       2) (integer) port
    //       3) "node_id"
    //
    // 我们简化为 RESP array of arrays

    // 按节点聚合 slot 范围输出
    // 注意：这里简化为 text 格式，实际 server 端需转为 RESP
    std::string result;
    int slot_count = 0;
    for (const auto& node : nodes_) {
        for (const auto& range : node.slot_ranges) {
            if (!result.empty()) result += " ";
            result += std::to_string(range.first) + " " +
                      std::to_string(range.second) + " " +
                      node.host + " " + std::to_string(node.port);
            slot_count++;
        }
    }
    return result;
}

std::string ClusterManager::ClusterInfo() const {
    std::lock_guard<std::mutex> lock(mu_);

    // Redis CLUSTER INFO 兼容格式
    std::string info;
    info += "cluster_state:ok\n";
    info += "cluster_slots_assigned:" + std::to_string(kHashSlotCount) + "\n";
    info += "cluster_slots_ok:" + std::to_string(kHashSlotCount) + "\n";

    uint64_t master_count = 0;
    uint64_t replica_count = 0;
    for (const auto& node : nodes_) {
        if (node.role == ClusterNodeRole::kMaster) master_count++;
        else replica_count++;
    }
    info += "cluster_known_nodes:" + std::to_string(nodes_.size()) + "\n";
    info += "cluster_size:" + std::to_string(master_count) + "\n";
    info += "cluster_current_epoch:" + std::to_string(0) + "\n";
    info += "cluster_my_epoch:" + std::to_string(0) + "\n";

    // 本节点状态
    info += "cluster_node_id:" + std::to_string(node_id_) + "\n";
    info += "cluster_slots_local:" + std::to_string(local_slots_.size()) + "\n";

    return info;
}

uint64_t ClusterManager::ClusterCountKeysInSlot(uint16_t /*slot*/) const {
    // 简化：暂不实现精确计数
    return 0;
}

std::vector<std::string> ClusterManager::ClusterGetKeysInSlot(uint16_t /*slot*/, uint64_t /*count*/) const {
    // 简化：暂不实现
    return {};
}

// ═══════════════════════════════════════════════════════════════
// 集群管理
// ═══════════════════════════════════════════════════════════════

Status ClusterManager::AddNode(const ClusterNode& node) {
    std::lock_guard<std::mutex> lock(mu_);

    // 检查是否已存在
    for (const auto& n : nodes_) {
        if (n.id == node.id) {
            return Status::InvalidArgument("Node already exists");
        }
    }

    nodes_.push_back(node);
    return Status::OK();
}

Status ClusterManager::RemoveNode(uint64_t node_id) {
    std::lock_guard<std::mutex> lock(mu_);

    auto it = std::remove_if(nodes_.begin(), nodes_.end(),
                             [node_id](const ClusterNode& n) { return n.id == node_id; });
    if (it == nodes_.end()) {
        return Status::InvalidArgument("Node not found");
    }
    nodes_.erase(it, nodes_.end());
    RebuildSlotMapLocked();
    return Status::OK();
}

Status ClusterManager::AssignSlotRange(uint64_t node_id, uint16_t start_slot, uint16_t end_slot) {
    std::lock_guard<std::mutex> lock(mu_);

    // 先清除该节点原有的 slot 分配
    for (auto& node : nodes_) {
        if (node.id == node_id) {
            node.slot_ranges.clear();
            node.slot_ranges.push_back({start_slot, end_slot});
            RebuildSlotMapLocked();
            return Status::OK();
        }
    }
    return Status::InvalidArgument("Node not found");
}

Status ClusterManager::MarkNodeFailed(uint64_t node_id) {
    std::lock_guard<std::mutex> lock(mu_);
    for (auto& node : nodes_) {
        if (node.id == node_id) {
            node.state = ClusterNodeState::kFailed;
            return Status::OK();
        }
    }
    return Status::InvalidArgument("Node not found");
}

// ═══════════════════════════════════════════════════════════════
// 内部方法
// ═══════════════════════════════════════════════════════════════

void ClusterManager::RebuildSlotMap() {
    std::lock_guard<std::mutex> lock(mu_);
    RebuildSlotMapLocked();
}

void ClusterManager::RebuildSlotMapLocked() {
    slot_map_.assign(kHashSlotCount, 0);
    local_slots_.clear();

    for (const auto& node : nodes_) {
        for (const auto& range : node.slot_ranges) {
            for (uint16_t slot = range.first; slot <= range.second && slot < kHashSlotCount; ++slot) {
                slot_map_[slot] = node.id;
                if (node.id == node_id_) {
                    local_slots_.push_back(slot);
                }
            }
        }
    }
}

// ═══════════════════════════════════════════════════════════════
// 统计
// ═══════════════════════════════════════════════════════════════

uint64_t ClusterManager::NodeCount() const {
    std::lock_guard<std::mutex> lock(mu_);
    return nodes_.size();
}

uint64_t ClusterManager::SlotCount() const {
    return kHashSlotCount;
}

} // namespace lightkv
