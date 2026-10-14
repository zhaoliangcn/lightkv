// Internal declarations shared by the server's split translation units.
//
// Nothing here is part of the public API -- it exists only so that server.cpp
// can be broken up by responsibility (protocol, networking, commands,
// replication) instead of living as one 5000-line file.
//
// Helpers that used to be file-static in server.cpp live in lightkv::internal
// rather than being re-declared `static` per translation unit: a static copy in
// each .cpp would silently diverge.
#ifndef LIGHTKV_SERVER_INTERNAL_H
#define LIGHTKV_SERVER_INTERNAL_H

#include <cstdint>
#include <mutex>
#include <queue>
#include <string>
#include <vector>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#endif

namespace lightkv {
namespace internal {

enum class ConnType { kTCP, kHTTP };

#ifdef _WIN32
enum class IOCPOp { kRead, kWrite, kAccept };

// Per-connection OVERLAPPED state for IOCP
struct IocpOverlapped {
    OVERLAPPED overlapped;
    IOCPOp op;
    WSABUF buf;
    char buffer[32768];  // 32KB per-operation buffer
    DWORD bytes_transferred;
};

// Per-listening-socket state for AcceptEx
struct ListenContext {
    SOCKET listen_sock;
    IocpOverlapped accept_ol;
    char accept_buf[2 * (sizeof(SOCKADDR_IN) + 16)];
    int type;  // 0 = TCP, 1 = HTTP
};
#endif

struct Connection {
    int fd;
    ConnType type;
    std::string recv_buf;
    std::string send_buf;
    bool closed;
    bool authenticated;  // true if no auth required or AUTH succeeded
    int resp_version = 2;  // 2 = RESP2 (default), 3 = RESP3 (negotiated via HELLO)

    // Multi-threaded support
    std::mutex resp_mutex;
    std::queue<std::string> pending_responses;  // responses from worker threads
    bool has_pending_work = false;  // true if a command is being processed by worker

    // Replication: RDB transfer state (for Slave connections)
    std::string rdb_data;          // RDB snapshot data to send
    size_t rdb_sent = 0;           // how many bytes of RDB have been sent
    bool in_rdb_transfer = false;  // currently sending RDB snapshot

#ifdef _WIN32
    // IOCP per-connection state
    SOCKET raw_sock_ = INVALID_SOCKET;  // Original SOCKET for IOCP HANDLE conversion
    IocpOverlapped read_ol{};      // OVERLAPPED for WSARecv
    IocpOverlapped write_ol{};     // OVERLAPPED for WSASend
    bool reading = false;          // true if WSARecv is pending
    bool writing = false;          // true if WSASend is pending
#endif
};

// ─── RESP protocol helpers ───
std::string resp_ok();
std::string resp_nil();
std::string resp_integer(int64_t n);
std::string resp_error(const std::string& msg);
std::string resp_bulk_string(const std::string& s);
std::string resp_empty_array();
std::string resp_array(const std::vector<std::string>& arr);
std::string resp_map(const std::vector<std::string>& keys,
                     const std::vector<std::string>& vals);

// ─── TTL metadata key prefix ───
std::string ttl_key(const std::string& key);
int64_t now_ms();

// ─── Type prefix helpers ───
// The raw prefixes are exported because the geo handlers deliberately share the
// zset encoding; duplicating the literal would let the two drift apart.
extern const char kTtlMagic;
extern const char HASH_PREFIX[];
extern const char LIST_PREFIX[];
extern const char SET_PREFIX[];
extern const char ZSET_PREFIX[];

std::string hash_field_key(const std::string& name, const std::string& field);
std::string hash_meta_key(const std::string& name);
std::string list_idx_key(const std::string& name, int64_t idx);
std::string list_meta_key(const std::string& name);
std::string set_blob_key(const std::string& name);
std::string zset_member_key(const std::string& name, const std::string& member);
std::string zset_score_key(const std::string& name, double score,
                           const std::string& member);
std::string zset_meta_key(const std::string& name);
std::string zset_score_prefix(const std::string& name);
std::vector<std::string> parse_set_blob(const std::string& blob);
std::string encode_set_blob(const std::vector<std::string>& elems);
bool is_internal_key(const std::string& key);

// ─── Shared numeric formatting ───
std::string format_double_shortest(double v);

// ─── Raft log payload codec ───
// Wire format: [n_args:4][len0:4][arg0][len1:4][arg1]...
std::string encode_raft_command(const std::vector<std::string>& args);
bool decode_raft_command(const std::string& data,
                         std::vector<std::string>* args);

}  // namespace internal
}  // namespace lightkv

#endif  // LIGHTKV_SERVER_INTERNAL_H