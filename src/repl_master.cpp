#include "repl_master.h"

#include "server_internal.h"

#include <algorithm>
#include <iomanip>
#include <random>
#include <sstream>

namespace lightkv {
namespace internal {
namespace {

// Generate a simple UUID (UUID v4 format)
std::string generate_uuid() {
    static thread_local std::mt19937_64 rng(std::random_device{}());
    std::uniform_int_distribution<uint64_t> dist(0);
    uint64_t a = dist(rng), b = dist(rng);
    std::ostringstream oss;
    oss << std::hex << std::setfill('0')
        << std::setw(8) << (a >> 32) << '-'
        << std::setw(4) << (a >> 16 & 0xFFFF) << '-'
        << std::setw(4) << (a & 0xFFFF) << '-'
        << std::setw(4) << (b >> 48 & 0xFFFF) << '-'
        << std::setw(12) << (b & 0xFFFFFFFFFFFFULL);
    return oss.str();
}

}  // namespace

ReplMaster::ReplMaster(int backlog_size)
    : repl_id_(generate_uuid()), repl_offset_(0),
      backlog_size_(backlog_size), backlog_first_offset_(0) {
    backlog_.resize(backlog_size);
}

uint64_t ReplMaster::Feed(const std::string& resp_cmd) {
    std::lock_guard<std::mutex> lock(mutex_);
    uint64_t offset = repl_offset_;
    size_t cmd_size = resp_cmd.size();

    // Append to ring buffer
    for (size_t i = 0; i < cmd_size; i++) {
        size_t pos = static_cast<size_t>(repl_offset_ % backlog_size_);
        backlog_[pos] = resp_cmd[i];
        repl_offset_++;
    }

    // Update first_offset if we overwrote old data
    if (repl_offset_ - backlog_first_offset_ > static_cast<uint64_t>(backlog_size_)) {
        backlog_first_offset_ = repl_offset_ - backlog_size_;
    }

    return offset;
}

void ReplMaster::AddSlave(int fd) {
    if (fd < 0) return;
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& s : slaves_) {
        if (s.fd == fd) return;
    }
    slaves_.push_back({fd, 0, 0, false, false});
}

void ReplMaster::RemoveSlave(int fd) {
    std::lock_guard<std::mutex> lock(mutex_);
    slaves_.erase(
        std::remove_if(slaves_.begin(), slaves_.end(),
            [fd](const SlaveConn& s) { return s.fd == fd; }),
        slaves_.end());
}

std::string ReplMaster::HandlePSYNC(const std::string& slave_repl_id,
                                    int64_t slave_offset, int slave_fd) {
    std::lock_guard<std::mutex> lock(mutex_);

    // First connection or repl_id mismatch → full resync
    if (slave_repl_id == "?" || slave_repl_id != repl_id_) {
        uint64_t current_offset = repl_offset_;
        // Mark this slave for full resync. pending_offset MUST move to the
        // offset the snapshot represents: leaving it at 0 (where AddSlave
        // put it) made flush_replication_backlog() replay the entire backlog
        // from byte zero on top of the snapshot the slave just loaded. SET is
        // idempotent so this stayed invisible; INCR double-counted.
        for (auto& s : slaves_) {
            if (s.fd == slave_fd) {
                s.sent_full_resync = true;
                s.ack_offset = current_offset;
                s.pending_offset = current_offset;
                break;
            }
        }
        return "FULLRESYNC " + repl_id_ + " " + std::to_string(current_offset);
    }

    // Check if backlog contains the requested offset
    if (static_cast<uint64_t>(slave_offset) >= backlog_first_offset_ &&
        static_cast<uint64_t>(slave_offset) <= repl_offset_) {
        // Partial resync: resume from exactly where the slave stopped.
        // Without this the new connection's pending_offset stayed 0 and the
        // slave received every byte ever written, applied twice over.
        const uint64_t resume = static_cast<uint64_t>(slave_offset);
        for (auto& s : slaves_) {
            if (s.fd == slave_fd) {
                s.sent_full_resync = false;
                s.ack_offset = resume;
                s.pending_offset = resume;
                break;
            }
        }
        return "CONTINUE " + repl_id_;
    }

    // Backlog doesn't have the offset → full resync
    uint64_t current_offset = repl_offset_;
    for (auto& s : slaves_) {
        if (s.fd == slave_fd) {
            s.sent_full_resync = true;
            s.ack_offset = current_offset;
            s.pending_offset = current_offset;
            break;
        }
    }
    return "FULLRESYNC " + repl_id_ + " " + std::to_string(current_offset);
}

std::string ReplMaster::GetBacklogFrom(uint64_t offset) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (offset < backlog_first_offset_ || offset > repl_offset_) {
        return "";
    }
    size_t len = static_cast<size_t>(repl_offset_ - offset);
    if (len == 0) return "";

    std::string result;
    result.reserve(len);
    for (size_t i = 0; i < len; i++) {
        size_t pos = static_cast<size_t>((offset + i) % backlog_size_);
        result += backlog_[pos];
    }
    return result;
}

bool ReplMaster::CanServeFrom(uint64_t offset) {
    std::lock_guard<std::mutex> lock(mutex_);
    return offset >= backlog_first_offset_ && offset <= repl_offset_;
}

std::vector<int> ReplMaster::GetSlaveFds() {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<int> fds;
    for (auto& s : slaves_) {
        fds.push_back(s.fd);
    }
    return fds;
}

void ReplMaster::UpdateSlaveAck(int fd, uint64_t offset) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& s : slaves_) {
        if (s.fd == fd) {
            s.ack_offset = offset;
            break;
        }
    }
}

void ReplMaster::MarkSlaveSynced(int fd) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& s : slaves_) {
        if (s.fd == fd) {
            s.in_rdb_transfer = false;
            break;
        }
    }
}

int ReplMaster::SlaveCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return static_cast<int>(slaves_.size());
}

uint64_t ReplMaster::GetBacklogUsed() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return repl_offset_ - backlog_first_offset_;
}

uint64_t ReplMaster::GetSlavePendingOffset(int fd) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& s : slaves_) {
        if (s.fd == fd) {
            return s.pending_offset;
        }
    }
    return 0;
}

void ReplMaster::UpdateSlavePendingOffset(int fd, uint64_t offset) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& s : slaves_) {
        if (s.fd == fd) {
            s.pending_offset = offset;
            break;
        }
    }
}

}  // namespace internal
}  // namespace lightkv