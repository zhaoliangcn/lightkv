#pragma once

// Slave-side replication client.
//
// The master half already existed (ReplMaster backlog, PSYNC, RDB dump) but was
// never reachable: AddSlave had no caller, so no slave was ever registered. This
// is the missing other half -- the link a replica opens to its master.
//
// Design notes:
//   - The link owns one blocking socket on its own thread rather than joining the
//     server's event loop. There is exactly one such link per process, so
//     integrating it into the loop would buy nothing and would put replication
//     traffic (potentially large) on the path that also serves client commands.
//   - Everything the master sends is RESP, so the link needs a proper streaming
//     frame reader: the PSYNC reply, then a bulk-string RDB dump, then an endless
//     sequence of command arrays, with no request/response pairing.
//   - Reconnects use exponential backoff. A master that is down must not turn
//     into a tight reconnect loop.

#include "status.h"
#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace lightkv {

struct ReplSlaveOptions {
    std::string host;
    uint16_t port = 0;
    // Sent as AUTH before anything else when non-empty.
    std::string auth;
    // Advertised via REPLCONF listening-port so a cascading setup could find us.
    uint16_t local_port = 0;
    // Backoff bounds for reconnecting after a failure.
    int reconnect_min_ms = 500;
    int reconnect_max_ms = 30000;
    // How often to send REPLCONF ACK with our offset.
    int ack_interval_ms = 5000;
    // Per-read/write socket deadline. Must be finite: the master not answering
    // must not wedge this thread.
    int io_timeout_ms = 10000;
    // Refuse to buffer an RDB larger than this.
    uint64_t max_rdb_size = 512ull * 1024 * 1024;
};

// Where replicated data goes. Implemented by Server::Impl.
class ReplSlaveSink {
public:
    virtual ~ReplSlaveSink() = default;

    // Apply one replicated command. `args` includes the command name.
    virtual void ApplyReplicatedCommand(const std::vector<std::string>& args) = 0;

    // Replace the entire key space with `rows`. Used for a FULLRESYNC.
    virtual Status LoadFullImage(
        const std::vector<std::pair<std::string, std::string>>& rows) = 0;

    // Refuse every write until told otherwise. A replica that has not finished
    // its handshake must not accept client writes.
    virtual void SetReplicaReadOnly(bool readonly) = 0;
};

enum class ReplSlaveState {
    kDisconnected,
    kConnecting,
    kHandshaking,
    kLoadingSnapshot,
    kOnline,
};

const char* ReplSlaveStateName(ReplSlaveState s);

class ReplSlaveLink {
public:
    ReplSlaveLink(const ReplSlaveOptions& opts, ReplSlaveSink* sink);
    ~ReplSlaveLink();

    ReplSlaveLink(const ReplSlaveLink&) = delete;
    ReplSlaveLink& operator=(const ReplSlaveLink&) = delete;

    // Connect and complete the handshake synchronously. Returns OK only once the
    // link is genuinely usable; a failure reports why. REPLICAOF surfaces this
    // instead of returning +OK and hoping (see docs, F-31).
    Status ConnectAndHandshake();

    // Hand the link over to its own thread for the streaming phase. Safe to call
    // after a successful ConnectAndHandshake().
    void StartStreaming();

    void Stop();

    ReplSlaveState state() const { return state_.load(std::memory_order_acquire); }
    bool connected() const { return state() == ReplSlaveState::kOnline; }
    const std::string& master_repl_id() const { return master_repl_id_; }
    uint64_t offset() const { return offset_.load(std::memory_order_acquire); }
    uint64_t reconnects() const { return reconnects_.load(std::memory_order_acquire); }

    std::string LastError() const;
    void SetLastError(const std::string& err);

private:
    bool OpenSocket();
    void CloseSocket();

    // Full request/response exchange for the handshake phase.
    Status SendCommand(const std::vector<std::string>& args,
                       std::string* reply);
    Status DoHandshake();

    // Streaming phase.
    void StreamLoop();
    Status ReadFrame(std::string* out);
    Status ReadExact(char* buf, size_t len);
    bool ReadLine(std::string* line);
    void CompactBuffer();
    Status ReadSnapshot(std::vector<std::pair<std::string, std::string>>* rows);
    void SendAck();

    ReplSlaveOptions opts_;
    ReplSlaveSink* sink_;

    int fd_ = -1;
    std::atomic<ReplSlaveState> state_{ReplSlaveState::kDisconnected};
    std::atomic<uint64_t> offset_{0};
    std::atomic<uint64_t> reconnects_{0};
    std::atomic<bool> stop_requested_{false};

    std::string master_repl_id_;
    // Set by the handshake when the master answered FULLRESYNC (a dump follows)
    // and cleared once it has been consumed. A CONTINUE means no dump is coming.
    bool pending_full_sync_ = false;
    std::string last_error_;
    mutable std::mutex error_mu_;

    std::string rx_buf_;
    size_t rx_pos_ = 0;

    std::thread thread_;
};

}  // namespace lightkv