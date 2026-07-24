#pragma once

#include "raft.h"
#include <thread>
#include <atomic>
#include <mutex>
#include <unordered_map>
#include <functional>

namespace lightkv {

// ─── RaftServer ───
//
// 职责：
// 1. 维护节点间 TCP 连接，发送/接收 Raft RPC
// 2. 解析 RPC 请求并调用 Raft 引擎对应的 Handle* 方法
// 3. 处理响应发送
//
// 每个 Raft 节点启动时创建一个 RaftServer 实例。
// RaftServer 监听一个独立端口（raft_port，与 RESP 端口不同），
// 仅用于节点间 Raft 协议通信。
//
// 非集群模式: enable_raft=false 时不启动 RaftServer
class RaftServer : public RaftRPC {
public:
    // host/port: 本节点监听地址（用于接收其他节点的 RPC）
    RaftServer(const std::string& host, uint16_t port, Raft* raft);
    ~RaftServer();

    RaftServer(const RaftServer&) = delete;
    RaftServer& operator=(const RaftServer&) = delete;

    // ─── 生命周期 ───
    Status Initialize(const std::vector<RaftPeer>& peers);
    void Start();
    void Stop();

    // ─── RaftRPC 接口（发送 RPC 到指定 peer） ───
    AppendEntriesResponse SendAppendEntries(uint64_t peer_id, const AppendEntriesRequest& req) override;
    RequestVoteResponse SendRequestVote(uint64_t peer_id, const RequestVoteRequest& req) override;
    InstallSnapshotResponse SendInstallSnapshot(uint64_t peer_id, const InstallSnapshotRequest& req) override;

    // ForwardPropose：Follower 向 Leader 转发客户端写请求
    bool SendForwardPropose(uint64_t peer_id, const std::string& command);

    // 检查是否在运行
    bool IsRunning() const { return running_; }

    // ─── RPC 类型枚举（公开，供辅助函数使用） ───
    enum class RpcType : uint8_t {
        kAppendEntries = 0,
        kRequestVote = 1,
        kInstallSnapshot = 2,
        kAppendEntriesResp = 3,
        kRequestVoteResp = 4,
        kInstallSnapshotResp = 5,
        kForwardPropose = 6,
        kForwardProposeReply = 7,
    };

    // ─── 网络 IO 辅助（公开，供内联辅助函数使用） ───
    static int Connect(const std::string& host, uint16_t port);
    static bool SendAll(int fd, const std::string& data);
    static bool RecvExact(int fd, char* buf, size_t len);
    static bool RecvResponse(int fd, std::string* out, int timeout_ms = 2000);

    // ─── RPC 消息序列化/反序列化（公开） ───
    static std::string SerializeAppendEntries(const AppendEntriesRequest& req);
    static AppendEntriesRequest DeserializeAppendEntries(const Slice& data);
    static std::string SerializeAppendEntriesResp(const AppendEntriesResponse& resp);
    static AppendEntriesResponse DeserializeAppendEntriesResp(const Slice& data);

    static std::string SerializeRequestVote(const RequestVoteRequest& req);
    static RequestVoteRequest DeserializeRequestVote(const Slice& data);
    static std::string SerializeRequestVoteResp(const RequestVoteResponse& resp);
    static RequestVoteResponse DeserializeRequestVoteResp(const Slice& data);

    // ─── RPC 消息编帧辅助（公开） ───
    static bool SendRpcMessage(int fd, RpcType type, const std::string& body);
    static bool RecvRpcMessage(int fd, RpcType& out_type, std::string& out_body, int timeout_ms = 2000);

private:
    // 连接结构（每个 peer 一个 TCP 连接）
    struct PeerConnection {
        int fd = -1;
        std::string host;
        uint16_t port;
        // 由 RaftServer::mu_ 保护，无需 per-connection mutex
    };

    // 连接管理
    int ConnectToPeer(uint64_t peer_id);
    void DisconnectAll();

    // 接收线程：处理入站 RPC
    void AcceptLoop();

    // 处理收到的 RPC 请求
    void HandleIncomingRPC(int conn_fd);

    std::string host_;
    uint16_t port_;
    Raft* raft_;
    std::vector<RaftPeer> peers_;
    std::unordered_map<uint64_t, PeerConnection> connections_;

    int listen_fd_{-1};
    std::atomic<bool> running_{false};
    std::thread accept_thread_;
    mutable std::mutex mu_;
};

} // namespace lightkv
