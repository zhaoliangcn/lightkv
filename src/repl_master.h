// Master-side replication bookkeeping: the replication ring buffer and the
// per-slave offset ledger.
//
// Split out of server.cpp so replication can be worked on without touching the
// 5000-line dispatch file.
#ifndef LIGHTKV_REPL_MASTER_H
#define LIGHTKV_REPL_MASTER_H

#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

namespace lightkv {
namespace internal {

// Slave connection state on Master side
struct SlaveConn {
    int fd;
    uint64_t ack_offset;       // offset acknowledged by slave
    uint64_t pending_offset;   // offset of data pending to be sent (for CONTINUE)
    bool sent_full_resync;     // whether FULLRESYNC has been sent
    bool in_rdb_transfer;      // currently sending RDB snapshot
};

// Replication Master: manages slave connections and replication stream
class ReplMaster {
public:
    explicit ReplMaster(int backlog_size);

    // Feed a write command into the replication stream
    // Returns the offset at which this command was stored
    uint64_t Feed(const std::string& resp_cmd);

    // Register a new slave connection. Idempotent: a reconnecting slave sends
    // PSYNC again on the same fd, and a duplicate entry would make it receive
    // every backlog slice twice.
    void AddSlave(int fd);
    void RemoveSlave(int fd);

    // Handle PSYNC command from slave
    // Returns: "FULLRESYNC <repl_id> <offset>" or "CONTINUE <repl_id>"
    std::string HandlePSYNC(const std::string& slave_repl_id,
                            int64_t slave_offset, int slave_fd);

    // Get replication data from a given offset (for partial resync)
    // Returns empty string if offset is not in backlog
    std::string GetBacklogFrom(uint64_t offset);

    // True once the requested offset has fallen out of the ring buffer. There
    // is no incremental slice left for it; only a fresh full resync can catch
    // it up.
    bool CanServeFrom(uint64_t offset);

    // Get all slave fds that need to receive replication data
    std::vector<int> GetSlaveFds();

    // Update slave ack offset
    void UpdateSlaveAck(int fd, uint64_t offset);

    // Mark slave as having completed RDB sync
    void MarkSlaveSynced(int fd);

    const std::string& GetReplId() const { return repl_id_; }
    uint64_t GetOffset() const { return repl_offset_; }
    int SlaveCount() const;

    // Get backlog info for debugging
    uint64_t GetBacklogFirstOffset() const { return backlog_first_offset_; }
    uint64_t GetBacklogUsed() const;

    // Get slave's pending offset for replication flush
    uint64_t GetSlavePendingOffset(int fd);

    // Update slave's pending offset after sending data
    void UpdateSlavePendingOffset(int fd, uint64_t offset);

private:
    std::string repl_id_;
    uint64_t repl_offset_;
    std::vector<SlaveConn> slaves_;
    std::vector<char> backlog_;       // Ring buffer
    int backlog_size_;
    uint64_t backlog_first_offset_;   // The offset of the first byte in backlog
    mutable std::mutex mutex_;
};

}  // namespace internal
}  // namespace lightkv

#endif  // LIGHTKV_REPL_MASTER_H