#include "lightkv/raft_server.h"
#include "lightkv/raft.h"
#include "lightkv/encoding.h"

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <cstring>
#include <sstream>

namespace lightkv {

// ══════════════════════════════════════════════════════════
// 网络 IO 辅助函数（非类成员，仅内部使用）
// ══════════════════════════════════════════════════════════

int RaftServer::Connect(const std::string& host, uint16_t port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, host.c_str(), &addr.sin_addr);

    // 设置连接超时
    struct timeval tv;
    tv.tv_sec = 2;
    tv.tv_usec = 0;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    if (::connect(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        ::close(fd);
        return -1;
    }

    return fd;
}

bool RaftServer::SendAll(int fd, const std::string& data) {
    size_t remaining = data.size();
    const char* ptr = data.data();
    while (remaining > 0) {
        ssize_t n = ::send(fd, ptr, remaining, MSG_NOSIGNAL);
        if (n <= 0) return false;
        ptr += n;
        remaining -= static_cast<size_t>(n);
    }
    return true;
}

bool RaftServer::RecvExact(int fd, char* buf, size_t len) {
    while (len > 0) {
        ssize_t n = ::recv(fd, buf, len, 0);
        if (n <= 0) return false;
        buf += n;
        len -= static_cast<size_t>(n);
    }
    return true;
}

bool RaftServer::RecvResponse(int fd, std::string* out, int /*timeout_ms*/) {
    // 先读取 4 字节的消息长度（网络序）
    char len_buf[4];
    if (!RecvExact(fd, len_buf, 4)) return false;

    uint32_t msg_len = 0;
    for (int i = 0; i < 4; ++i) {
        msg_len = (msg_len << 8) | static_cast<uint8_t>(len_buf[i]);
    }

    if (msg_len == 0 || msg_len > 1024 * 1024) return false;  // sanity: max 1MB

    out->resize(msg_len);
    if (!RecvExact(fd, &(*out)[0], msg_len)) return false;

    return true;
}

// ══════════════════════════════════════════════════════════
// 序列化/反序列化
// ══════════════════════════════════════════════════════════

std::string RaftServer::SerializeAppendEntries(const AppendEntriesRequest& req) {
    std::string buf;
    // Format: term(8) | leader_id(8) | prev_log_idx(8) | prev_log_term(8)
    //         | leader_commit(8) | entry_count(4) | [entry_data] * entry_count
    auto put64 = [&](uint64_t v) {
        for (int i = 7; i >= 0; --i) buf.push_back(static_cast<char>((v >> (i * 8)) & 0xff));
    };
    auto put32 = [&](uint32_t v) {
        for (int i = 3; i >= 0; --i) buf.push_back(static_cast<char>((v >> (i * 8)) & 0xff));
    };

    put64(req.term);
    put64(req.leader_id);
    put64(req.prev_log_index);
    put64(req.prev_log_term);
    put64(req.leader_commit);
    put32(static_cast<uint32_t>(req.entries.size()));

    for (const auto& entry : req.entries) {
        std::string entry_data = entry.Serialize();
        put32(static_cast<uint32_t>(entry_data.size()));
        buf.append(entry_data);
    }

    return buf;
}

AppendEntriesRequest RaftServer::DeserializeAppendEntries(const Slice& data) {
    AppendEntriesRequest req;
    size_t off = 0;

    auto get64 = [&]() -> uint64_t {
        uint64_t v = 0;
        for (int i = 0; i < 8; ++i) {
            v = (v << 8) | static_cast<uint8_t>(data.data()[off++]);
        }
        return v;
    };
    auto get32 = [&]() -> uint32_t {
        uint32_t v = 0;
        for (int i = 0; i < 4; ++i) {
            v = (v << 8) | static_cast<uint8_t>(data.data()[off++]);
        }
        return v;
    };

    req.term = get64();
    req.leader_id = get64();
    req.prev_log_index = get64();
    req.prev_log_term = get64();
    req.leader_commit = get64();

    uint32_t entry_count = get32();
    for (uint32_t i = 0; i < entry_count; ++i) {
        uint32_t entry_len = get32();
        Slice entry_slice(data.data() + off, entry_len);
        req.entries.push_back(RaftLogEntry::Deserialize(entry_slice));
        off += entry_len;
    }

    return req;
}

std::string RaftServer::SerializeAppendEntriesResp(const AppendEntriesResponse& resp) {
    std::string buf;
    auto put64 = [&](uint64_t v) {
        for (int i = 7; i >= 0; --i) buf.push_back(static_cast<char>((v >> (i * 8)) & 0xff));
    };
    put64(resp.term);
    buf.push_back(resp.success ? 1 : 0);
    put64(resp.conflict_term);
    put64(resp.conflict_index);
    return buf;
}

AppendEntriesResponse RaftServer::DeserializeAppendEntriesResp(const Slice& data) {
    AppendEntriesResponse resp;
    size_t off = 0;
    auto get64 = [&]() -> uint64_t {
        uint64_t v = 0;
        for (int i = 0; i < 8; ++i) v = (v << 8) | static_cast<uint8_t>(data.data()[off++]);
        return v;
    };
    resp.term = get64();
    resp.success = (data.data()[off++] != 0);
    resp.conflict_term = get64();
    resp.conflict_index = get64();
    return resp;
}

std::string RaftServer::SerializeRequestVote(const RequestVoteRequest& req) {
    std::string buf;
    auto put64 = [&](uint64_t v) {
        for (int i = 7; i >= 0; --i) buf.push_back(static_cast<char>((v >> (i * 8)) & 0xff));
    };
    put64(req.term);
    put64(req.candidate_id);
    put64(req.last_log_index);
    put64(req.last_log_term);
    return buf;
}

RequestVoteRequest RaftServer::DeserializeRequestVote(const Slice& data) {
    RequestVoteRequest req;
    size_t off = 0;
    auto get64 = [&]() -> uint64_t {
        uint64_t v = 0;
        for (int i = 0; i < 8; ++i) v = (v << 8) | static_cast<uint8_t>(data.data()[off++]);
        return v;
    };
    req.term = get64();
    req.candidate_id = get64();
    req.last_log_index = get64();
    req.last_log_term = get64();
    return req;
}

std::string RaftServer::SerializeRequestVoteResp(const RequestVoteResponse& resp) {
    std::string buf;
    auto put64 = [&](uint64_t v) {
        for (int i = 7; i >= 0; --i) buf.push_back(static_cast<char>((v >> (i * 8)) & 0xff));
    };
    put64(resp.term);
    buf.push_back(resp.vote_granted ? 1 : 0);
    return buf;
}

RequestVoteResponse RaftServer::DeserializeRequestVoteResp(const Slice& data) {
    RequestVoteResponse resp;
    size_t off = 0;
    auto get64 = [&]() -> uint64_t {
        uint64_t v = 0;
        for (int i = 0; i < 8; ++i) v = (v << 8) | static_cast<uint8_t>(data.data()[off++]);
        return v;
    };
    resp.term = get64();
    resp.vote_granted = (data.data()[off] != 0);
    return resp;
}

// ══════════════════════════════════════════════════════════
// RPC 消息编帧
// ══════════════════════════════════════════════════════════

bool RaftServer::SendRpcMessage(int fd, RpcType type, const std::string& body) {
    // Header: type(1) | body_len(4) | body(body_len)
    std::string header;
    header.push_back(static_cast<uint8_t>(type));
    uint32_t net_len = htonl(static_cast<uint32_t>(body.size()));
    header.append(reinterpret_cast<const char*>(&net_len), 4);

    if (!SendAll(fd, header)) return false;
    if (!body.empty() && !SendAll(fd, body)) return false;
    return true;
}

bool RaftServer::RecvRpcMessage(int fd, RpcType& out_type, std::string& out_body, int /*timeout_ms*/) {
    // 先读取 header
    char header_buf[5];
    if (!RecvExact(fd, header_buf, 5)) return false;

    out_type = static_cast<RpcType>(header_buf[0]);

    uint32_t net_len;
    memcpy(&net_len, header_buf + 1, 4);
    uint32_t body_len = ntohl(net_len);

    if (body_len > 0) {
        out_body.resize(body_len);
        if (!RecvExact(fd, &out_body[0], body_len)) return false;
    }

    return true;
}

// ══════════════════════════════════════════════════════════
// 构造函数 & 生命周期
// ══════════════════════════════════════════════════════════

RaftServer::RaftServer(const std::string& host, uint16_t port, Raft* raft)
    : host_(host), port_(port), raft_(raft) {
}

RaftServer::~RaftServer() {
    Stop();
}

Status RaftServer::Initialize(const std::vector<RaftPeer>& peers) {
    peers_ = peers;

    // 初始化 peer 连接表（不实际连接，按需建立）
    for (const auto& peer : peers_) {
        if (peer.id == 0) continue;
        // 使用 try_emplace 避免 mutex 复制问题
        PeerConnection conn;
        conn.host = peer.host;
        conn.port = peer.port;
        conn.fd = -1;
        connections_.try_emplace(peer.id, std::move(conn));
    }

    return Status::OK();
}

void RaftServer::Start() {
    if (running_.exchange(true)) return;

    // 创建监听 socket
    listen_fd_ = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (listen_fd_ < 0) {
        running_ = false;
        return;
    }

    int opt = 1;
    setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port_);
    inet_pton(AF_INET, host_.c_str(), &addr.sin_addr);

    if (::bind(listen_fd_, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        ::close(listen_fd_);
        listen_fd_ = -1;
        running_ = false;
        return;
    }

    if (::listen(listen_fd_, 10) < 0) {
        ::close(listen_fd_);
        listen_fd_ = -1;
        running_ = false;
        return;
    }

    // 接受线程
    accept_thread_ = std::thread([this]() {
        while (running_) {
            struct sockaddr_in client_addr;
            socklen_t addr_len = sizeof(client_addr);
            int conn_fd = ::accept4(listen_fd_, (struct sockaddr*)&client_addr,
                                    &addr_len, SOCK_NONBLOCK);
            if (conn_fd < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
                    continue;
                }
                break;
            }
            // 在每个连接上处理 RPC（简化：同步处理，一个连接一个请求）
            HandleIncomingRPC(conn_fd);
            ::close(conn_fd);
        }
    });
}

void RaftServer::Stop() {
    running_ = false;
    if (accept_thread_.joinable()) {
        accept_thread_.join();
    }
    if (listen_fd_ >= 0) {
        ::close(listen_fd_);
        listen_fd_ = -1;
    }
    DisconnectAll();
}

// ══════════════════════════════════════════════════════════
// 连接管理
// ══════════════════════════════════════════════════════════

int RaftServer::ConnectToPeer(uint64_t peer_id) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = connections_.find(peer_id);
    if (it == connections_.end()) return -1;

    auto& conn = it->second;
    // 检测已有连接是否断开
    if (conn.fd >= 0) {
        // 使用 getsockopt + SO_ERROR 非破坏性检测连接状态
        int error = 0;
        socklen_t len = sizeof(error);
        if (getsockopt(conn.fd, SOL_SOCKET, SO_ERROR, &error, &len) == 0 && error == 0) {
            // 通过 MSG_PEEK 零字节探测确认连接活跃
            char peek_buf;
            ssize_t ret = ::recv(conn.fd, &peek_buf, 1, MSG_PEEK | MSG_DONTWAIT);
            if (ret == 0) {
                // 连接已关闭（EOF）
                ::close(conn.fd);
                conn.fd = -1;
            } else if (ret < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                // 连接正常，只是无数据可读
                return conn.fd;
            } else if (ret < 0) {
                // 连接异常
                ::close(conn.fd);
                conn.fd = -1;
            } else {
                return conn.fd;  // 有数据可读，连接正常
            }
        } else {
            // 连接有错误
            ::close(conn.fd);
            conn.fd = -1;
        }
    }

    // 建立新连接（自动重连）
    conn.fd = Connect(conn.host, conn.port);
    return conn.fd;
}

void RaftServer::DisconnectAll() {
    std::lock_guard<std::mutex> lock(mu_);
    for (auto& [id, conn] : connections_) {
        (void)id;
        if (conn.fd >= 0) {
            ::close(conn.fd);
            conn.fd = -1;
        }
    }
}

// ══════════════════════════════════════════════════════════
// 发送 RPC
// ══════════════════════════════════════════════════════════

AppendEntriesResponse RaftServer::SendAppendEntries(uint64_t peer_id, const AppendEntriesRequest& req) {
    AppendEntriesResponse resp;
    resp.term = 0;
    resp.success = false;

    int fd = ConnectToPeer(peer_id);
    if (fd < 0) return resp;

    std::string body = SerializeAppendEntries(req);
    if (!SendRpcMessage(fd, RpcType::kAppendEntries, body)) {
        // 连接断开，下次重试
        {
            std::lock_guard<std::mutex> lock(mu_);
            if (connections_.count(peer_id)) {
                ::close(connections_[peer_id].fd);
                connections_[peer_id].fd = -1;
            }
        }
        return resp;
    }

    // 读取响应
    RpcType resp_type;
    std::string resp_body;
    if (!RecvRpcMessage(fd, resp_type, resp_body)) {
        return resp;
    }

    if (resp_type == RpcType::kAppendEntriesResp) {
        Slice slice(resp_body);
        resp = DeserializeAppendEntriesResp(slice);
    }

    return resp;
}

RequestVoteResponse RaftServer::SendRequestVote(uint64_t peer_id, const RequestVoteRequest& req) {
    RequestVoteResponse resp;
    resp.term = 0;
    resp.vote_granted = false;

    int fd = ConnectToPeer(peer_id);
    if (fd < 0) return resp;

    std::string body = SerializeRequestVote(req);
    if (!SendRpcMessage(fd, RpcType::kRequestVote, body)) {
        {
            std::lock_guard<std::mutex> lock(mu_);
            if (connections_.count(peer_id)) {
                ::close(connections_[peer_id].fd);
                connections_[peer_id].fd = -1;
            }
        }
        return resp;
    }

    RpcType resp_type;
    std::string resp_body;
    if (!RecvRpcMessage(fd, resp_type, resp_body)) {
        return resp;
    }

    if (resp_type == RpcType::kRequestVoteResp) {
        Slice slice(resp_body);
        resp = DeserializeRequestVoteResp(slice);
    }

    return resp;
}

InstallSnapshotResponse RaftServer::SendInstallSnapshot(uint64_t /*peer_id*/, const InstallSnapshotRequest& /*req*/) {
    InstallSnapshotResponse resp;
    resp.term = 0;
    resp.success = false;
    // 简化：暂不实现快照传输
    return resp;
}

// ══════════════════════════════════════════════════════════
// ForwardPropose 实现
// ══════════════════════════════════════════════════════════

bool RaftServer::SendForwardPropose(uint64_t peer_id, const std::string& command) {
    int fd = ConnectToPeer(peer_id);
    if (fd < 0) return false;

    // 序列化：command_len(4) + command_data
    std::string body;
    uint32_t cmd_len = htonl(static_cast<uint32_t>(command.size()));
    body.append(reinterpret_cast<const char*>(&cmd_len), 4);
    body.append(command);

    if (!SendRpcMessage(fd, RpcType::kForwardPropose, body)) {
        std::lock_guard<std::mutex> lock(mu_);
        if (connections_.count(peer_id)) {
            ::close(connections_[peer_id].fd);
            connections_[peer_id].fd = -1;
        }
        return false;
    }

    // 读取响应（简单确认）
    RpcType resp_type;
    std::string resp_body;
    if (!RecvRpcMessage(fd, resp_type, resp_body)) {
        return false;
    }
    return resp_type == RpcType::kForwardProposeReply;
}

// ══════════════════════════════════════════════════════════
// 处理入站 RPC 请求
// ══════════════════════════════════════════════════════════

void RaftServer::HandleIncomingRPC(int conn_fd) {
    if (!raft_) return;

    RpcType type;
    std::string body;
    if (!RecvRpcMessage(conn_fd, type, body)) return;

    switch (type) {
    case RpcType::kAppendEntries: {
        Slice slice(body);
        auto req = DeserializeAppendEntries(slice);
        auto resp = raft_->HandleAppendEntries(req);
        std::string resp_body = SerializeAppendEntriesResp(resp);
        SendRpcMessage(conn_fd, RpcType::kAppendEntriesResp, resp_body);
        break;
    }
    case RpcType::kRequestVote: {
        Slice slice(body);
        auto req = DeserializeRequestVote(slice);
        auto resp = raft_->HandleRequestVote(req);
        std::string resp_body = SerializeRequestVoteResp(resp);
        SendRpcMessage(conn_fd, RpcType::kRequestVoteResp, resp_body);
        break;
    }
    case RpcType::kInstallSnapshot: {
        // 简化：暂不实现
        InstallSnapshotResponse resp;
        resp.term = raft_->GetCurrentTerm();
        resp.success = false;
        break;
    }
    case RpcType::kForwardPropose: {
        // Follower 收到转发来的写请求，调用 Propose
        if (body.size() < 4) break;
        uint32_t cmd_len;
        memcpy(&cmd_len, body.data(), 4);
        cmd_len = ntohl(cmd_len);
        std::string command = body.substr(4, cmd_len);
        raft_->Propose(command);
        // 回复确认
        SendRpcMessage(conn_fd, RpcType::kForwardProposeReply, "");
        break;
    }
    default:
        break;
    }
}

} // namespace lightkv
