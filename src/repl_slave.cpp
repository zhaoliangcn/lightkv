#include "lightkv/repl_slave.h"

#include "lightkv/platform.h"
#include "lightkv/status.h"
#include "lightkv/encoding.h"
#include <algorithm>
#include <chrono>
#include <cstring>
#include <cstdio>
#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#endif

namespace lightkv {
namespace {

constexpr char kRdbMagic[] = "LKVDB01";
// sizeof() includes the NUL, so the magic is one byte shorter on the wire.
// Deriving lengths from sizeof() instead of this constant made a legitimately
// empty snapshot (magic + CRC, 10 bytes) look one byte too short to parse.
constexpr size_t kRdbMagicLen = sizeof(kRdbMagic) - 1;
constexpr size_t kRdbCrcLen = 4;

// Total bytes we are willing to buffer for one frame before giving up. A
// corrupt length prefix must not be able to make us allocate unbounded memory.
constexpr size_t kMaxFrame = 64u * 1024 * 1024;

#ifdef _WIN32
int PollReadable(int fd, int timeout_ms) {
    WSAPOLLFD pfd{};
    pfd.fd = static_cast<SOCKET>(fd);
    pfd.events = POLLIN;
    return ::WSAPoll(&pfd, 1, timeout_ms);
}
int PollWritable(int fd, int timeout_ms) {
    WSAPOLLFD pfd{};
    pfd.fd = static_cast<SOCKET>(fd);
    pfd.events = POLLOUT;
    return ::WSAPoll(&pfd, 1, timeout_ms);
}
bool WouldBlock(int err) { return err == WSAEWOULDBLOCK || err == WSAEINPROGRESS; }
int LastSocketError() { return ::WSAGetLastError(); }
#else
int PollReadable(int fd, int timeout_ms) {
    struct pollfd p {};
    p.fd = fd;
    p.events = POLLIN;
    return ::poll(&p, 1, timeout_ms);
}
int PollWritable(int fd, int timeout_ms) {
    struct pollfd p {};
    p.fd = fd;
    p.events = POLLOUT;
    return ::poll(&p, 1, timeout_ms);
}
bool WouldBlock(int err) { return err == EINPROGRESS || err == EAGAIN || err == EWOULDBLOCK; }
int LastSocketError() { return errno; }
#endif

std::string BuildResp(const std::vector<std::string>& args) {
    std::string out = "*" + std::to_string(args.size()) + "\r\n";
    for (const auto& a : args) {
        out += "$" + std::to_string(a.size()) + "\r\n" + a + "\r\n";
    }
    return out;
}

}  // namespace

const char* ReplSlaveStateName(ReplSlaveState s) {
    switch (s) {
        case ReplSlaveState::kDisconnected: return "disconnected";
        case ReplSlaveState::kConnecting: return "connecting";
        case ReplSlaveState::kHandshaking: return "handshaking";
        case ReplSlaveState::kLoadingSnapshot: return "loading_snapshot";
        case ReplSlaveState::kOnline: return "online";
    }
    return "unknown";
}

ReplSlaveLink::ReplSlaveLink(const ReplSlaveOptions& opts, ReplSlaveSink* sink)
    : opts_(opts), sink_(sink) {}

ReplSlaveLink::~ReplSlaveLink() {
    Stop();
}

std::string ReplSlaveLink::LastError() const {
    std::lock_guard<std::mutex> lock(error_mu_);
    return last_error_;
}

void ReplSlaveLink::SetLastError(const std::string& err) {
    std::lock_guard<std::mutex> lock(error_mu_);
    last_error_ = err;
}

// ─── Connection ────────────────────────────────────────────────────────

bool ReplSlaveLink::OpenSocket() {
    CloseSocket();

    fd_ = static_cast<int>(::socket(AF_INET, SOCK_STREAM, 0));
    if (fd_ < 0) {
        SetLastError("socket() failed");
        return false;
    }

    int one = 1;
    ::setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY,
                 reinterpret_cast<const char*>(&one), sizeof(one));

    struct sockaddr_in addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(opts_.port);
    if (::inet_pton(AF_INET, opts_.host.c_str(), &addr.sin_addr) != 1) {
        // Replication targets are configured by an operator; refusing a name
        // here is better than silently connecting somewhere unexpected.
        SetLastError("invalid master address: " + opts_.host);
        CloseSocket();
        return false;
    }

    // Non-blocking connect so a dead master cannot hang the caller.
    socket_set_nonblocking(fd_);
    int rc = ::connect(fd_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr));
    if (rc != 0) {
        if (!WouldBlock(LastSocketError())) {
            SetLastError("connect() failed to " + opts_.host + ":" +
                         std::to_string(opts_.port));
            CloseSocket();
            return false;
        }
        if (PollWritable(fd_, opts_.io_timeout_ms) <= 0) {
            SetLastError("connect() to " + opts_.host + ":" +
                         std::to_string(opts_.port) + " timed out");
            CloseSocket();
            return false;
        }
        // poll() says writable; confirm the connection actually succeeded.
        int err = 0;
        socklen_t len = sizeof(err);
        if (::getsockopt(fd_, SOL_SOCKET, SO_ERROR,
                         reinterpret_cast<char*>(&err), &len) != 0 || err != 0) {
            SetLastError("connect() to " + opts_.host + ":" +
                         std::to_string(opts_.port) + " refused");
            CloseSocket();
            return false;
        }
    }

    // Back to blocking: every subsequent read waits on poll() explicitly, so a
    // finite deadline is honoured either way.
    socket_set_blocking(fd_);
    return true;
}

void ReplSlaveLink::CloseSocket() {
    if (fd_ >= 0) {
        socket_close(fd_);
        fd_ = -1;
    }
    rx_buf_.clear();
    rx_pos_ = 0;
}

// ─── Low-level reads ────────────────────────────────────────────────────

Status ReplSlaveLink::ReadExact(char* buf, size_t len) {
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(opts_.io_timeout_ms);
    while (len > 0) {
        const auto now = std::chrono::steady_clock::now();
        if (now >= deadline) {
            return Status::IOError("timed out reading from master");
        }
        const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
                                   deadline - now)
                                   .count();

        const int ready = PollReadable(fd_, static_cast<int>(remaining > 0 ? remaining : 1));
        if (ready == 0) return Status::IOError("timed out reading from master");
        if (ready < 0) return Status::IOError("poll() failed while reading from master");

        const ssize_t n = ::recv(fd_, buf, len, 0);
        if (n == 0) return Status::IOError("master closed the connection");
        if (n < 0) {
#ifdef _WIN32
            const int err = ::WSAGetLastError();
            if (err == WSAEWOULDBLOCK) continue;
            return Status::IOError("recv() from master failed");
#else
            if (errno == EINTR) continue;
            return Status::IOError("recv() from master failed");
#endif
        }
        buf += n;
        len -= static_cast<size_t>(n);
    }
    return Status::OK();
}

bool ReplSlaveLink::ReadLine(std::string* line) {
    line->clear();
    for (;;) {
        const size_t nl = rx_buf_.find("\r\n", rx_pos_);
        if (nl != std::string::npos) {
            *line = rx_buf_.substr(rx_pos_, nl - rx_pos_);
            rx_pos_ = nl + 2;
            CompactBuffer();
            return true;
        }
        if (rx_buf_.size() - rx_pos_ > 4096) return false;  // absurdly long line

        // ONE byte at a time. ReadExact() loops until it has been handed
        // exactly `len` bytes, so asking it for a 4KB block would sit on the
        // socket until 4KB arrived -- even for a five-byte "+OK\r\n".
        char c = '\0';
        Status s = ReadExact(&c, 1);
        if (!s.ok()) return false;
        rx_buf_.push_back(c);
    }
}

// ─── Streaming RESP frame reader ────────────────────────────────────────

void ReplSlaveLink::CompactBuffer() {
    if (rx_pos_ == 0) return;
    rx_buf_.erase(0, rx_pos_);
    rx_pos_ = 0;
}

// Reads exactly one complete RESP frame from the master into `out` (the raw
// bytes, frame included). Returns false on EOF/timeout/corruption.
//
// The replication stream is a flat sequence of frames with no request/response
// pairing, so the length prefix is the only way to know where a frame ends.
Status ReplSlaveLink::ReadFrame(std::string* out) {
    out->clear();
    CompactBuffer();

    // Header line: a type byte then the length. Keep-alive newlines and the
    // blank line that follows a stray payload terminator are skipped rather than
    // treated as a malformed frame.
    std::string line;
    for (int i = 0; i < 64; ++i) {
        if (!ReadLine(&line)) return Status::Corruption("truncated frame header");
        if (!line.empty()) break;
    }
    if (line.empty()) return Status::Corruption("no frame header before the deadline");

    const char type = line.empty() ? '?' : line[0];
    const std::string rest = line.substr(1);

    if (type == '+' || type == '-' || type == ':') {
        *out = line + "\r\n";
        return Status::OK();
    }

    long long n = 0;
    try {
        n = std::stoll(rest);
    } catch (...) {
        return Status::Corruption("bad length in frame header: " + line);
    }
    if (n < 0) {  // null bulk / null array
        *out = line + "\r\n";
        return Status::OK();
    }
    if (static_cast<size_t>(n) > kMaxFrame) {
        return Status::Corruption("frame too large: " + std::to_string(n));
    }

    // Bulk string: n bytes then CRLF.
    if (type == '$') {
        // Ask only for what is still missing -- ReadExact blocks until it is
        // handed exactly that many bytes, so over-requesting would stall until
        // the deadline on a short final frame.
        while (rx_buf_.size() - rx_pos_ < static_cast<size_t>(n) + 2) {
            const size_t need =
                static_cast<size_t>(n) + 2 - (rx_buf_.size() - rx_pos_);
            char tmp[8192];
            const size_t take = std::min(need, sizeof(tmp));
            Status s = ReadExact(tmp, take);
            if (!s.ok()) return s;
            rx_buf_.append(tmp, take);
        }
        *out = line + "\r\n" +
               rx_buf_.substr(rx_pos_, static_cast<size_t>(n) + 2);
        rx_pos_ += static_cast<size_t>(n) + 2;
        CompactBuffer();
        return Status::OK();
    }

    // Array: n elements, each a complete frame of its own.
    if (type == '*') {
        *out = line + "\r\n";
        for (long long i = 0; i < n; ++i) {
            std::string element;
            Status s = ReadFrame(&element);
            if (!s.ok()) return s;
            *out += element;
            if (out->size() > kMaxFrame) {
                return Status::Corruption("replicated command array too large");
            }
        }
        return Status::OK();
    }

    return Status::Corruption("unknown RESP type in replication stream: " + line);
}

// ─── Handshake ──────────────────────────────────────────────────────────

Status ReplSlaveLink::SendCommand(const std::vector<std::string>& args,
                                  std::string* reply) {
    const std::string cmd = BuildResp(args);
    const char* p = cmd.data();
    size_t left = cmd.size();
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(opts_.io_timeout_ms);
    while (left > 0) {
        const auto now = std::chrono::steady_clock::now();
        if (now >= deadline) return Status::IOError("timed out sending to master");
        const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
                                   deadline - now)
                                   .count();
        if (PollWritable(fd_, static_cast<int>(remaining > 0 ? remaining : 1)) <= 0) {
            return Status::IOError("timed out writing to master");
        }
        const ssize_t n = ::send(fd_, p, left, 0);
        if (n <= 0) {
#ifdef _WIN32
            if (::WSAGetLastError() == WSAEWOULDBLOCK) continue;
#else
            if (errno == EINTR) continue;
#endif
            return Status::IOError("send() to master failed");
        }
        p += n;
        left -= static_cast<size_t>(n);
    }

    CompactBuffer();
    rx_buf_.clear();
    return ReadFrame(reply);
}

Status ReplSlaveLink::ConnectAndHandshake() {
    // Read-only from the moment we start: a replica must not accept client writes
    // while its state is still whatever it was before. If the handshake fails
    // the flag is restored below, otherwise a failed REPLICAOF would leave the
    // node permanently read-only.
    if (sink_) sink_->SetReplicaReadOnly(true);
    state_.store(ReplSlaveState::kConnecting, std::memory_order_release);

    auto fail = [this](const Status& s) {
        SetLastError(s.ToString());
        CloseSocket();
        state_.store(ReplSlaveState::kDisconnected, std::memory_order_release);
        if (sink_) sink_->SetReplicaReadOnly(false);
        return s;
    };

    if (!OpenSocket()) {
        return fail(Status::IOError(LastError()));
    }

    state_.store(ReplSlaveState::kHandshaking, std::memory_order_release);
    Status s = DoHandshake();
    if (!s.ok()) return fail(s);

    state_.store(ReplSlaveState::kOnline, std::memory_order_release);
    if (sink_) sink_->SetReplicaReadOnly(true);
    SetLastError("");
    return Status::OK();
}

Status ReplSlaveLink::DoHandshake() {
    std::string reply;

    if (!opts_.auth.empty()) {
        Status s = SendCommand({"AUTH", opts_.auth}, &reply);
        if (!s.ok()) return s;
        if (!reply.empty() && reply[0] == '-') {
            return Status::InvalidArgument("master rejected AUTH: " + reply);
        }
    }

    Status s = SendCommand({"REPLCONF", "listening-port",
                            std::to_string(opts_.local_port)},
                           &reply);
    if (!s.ok()) return s;
    if (!reply.empty() && reply[0] == '-') {
        return Status::InvalidArgument("master rejected REPLCONF: " + reply);
    }

    s = SendCommand({"REPLCONF", "capa", "psync2"}, &reply);
    if (!s.ok()) return s;

    // PSYNC <our repl_id> <our offset>. The master answers with a bulk string
    // holding either "<id>" (continue) or "<id> <offset>" (full resync).
    s = SendCommand({"PSYNC", master_repl_id_.empty() ? "?" : master_repl_id_,
                     std::to_string(offset_.load(std::memory_order_relaxed))},
                    &reply);
    if (!s.ok()) return s;
    if (!reply.empty() && reply[0] == '-') {
        return Status::InvalidArgument("master rejected PSYNC: " + reply);
    }

    // Unwrap the bulk string.
    std::string payload;
    {
        const size_t nl = reply.find("\r\n");
        if (reply.empty() || reply[0] != '$' || nl == std::string::npos) {
            return Status::Corruption("unexpected PSYNC reply: " + reply);
        }
        long len = 0;
        try {
            len = std::stol(reply.substr(1, nl - 1));
        } catch (...) {
            return Status::Corruption("unexpected PSYNC reply length");
        }
        payload = reply.substr(nl + 2, static_cast<size_t>(std::max<long>(len, 0)));
    }

    // The server layer strips the FULLRESYNC/CONTINUE keyword, so what reaches
    // us is the remainder: "<id> <offset>" for a full resync and a bare "<id>"
    // for a partial one. Token count is therefore the discriminator, and a
    // repl_id never contains a space.
    const size_t sp = payload.find(' ');
    if (sp == std::string::npos) {
        // Partial resync: the master already holds our offset, no dump follows
        // and the local offset stays where it is.
        pending_full_sync_ = false;
        master_repl_id_ = payload;
        return Status::OK();
    }

    // FULLRESYNC: the master is discarding whatever we had, so a partial resync
    // is impossible and the local offset must be reset to match the snapshot.
    pending_full_sync_ = true;
    master_repl_id_ = payload.substr(0, sp);
    long long new_offset = 0;
    try {
        new_offset = std::stoll(payload.substr(sp + 1));
    } catch (...) {
        return Status::Corruption("bad offset in PSYNC reply: " + payload);
    }
    offset_.store(static_cast<uint64_t>(std::max<long long>(new_offset, 0)),
                  std::memory_order_release);
    return Status::OK();
}

// ─── Snapshot ───────────────────────────────────────────────────────────

Status ReplSlaveLink::ReadSnapshot(
    std::vector<std::pair<std::string, std::string>>* rows) {
    state_.store(ReplSlaveState::kLoadingSnapshot, std::memory_order_release);

    std::string header;
    if (!ReadLine(&header)) return Status::IOError("truncated RDB header");
    if (header.empty() || header[0] != '$') {
        return Status::Corruption("expected an RDB bulk string, got: " + header);
    }
    long long size = 0;
    try {
        size = std::stoll(header.substr(1));
    } catch (...) {
        return Status::Corruption("bad RDB size: " + header);
    }
    // An empty database still produces magic + trailer, so this only rejects a
    // genuinely malformed payload rather than a legitimately empty master.
    if (size < static_cast<long long>(kRdbMagicLen + kRdbCrcLen)) {
        return Status::Corruption("RDB payload is too short");
    }
    if (static_cast<uint64_t>(size) > opts_.max_rdb_size) {
        return Status::Corruption("RDB payload of " + std::to_string(size) +
                                  " bytes exceeds the configured limit");
    }

    std::string rdb;
    rdb.reserve(static_cast<size_t>(size));
    while (rdb.size() < static_cast<size_t>(size)) {
        const size_t want = std::min<size_t>(65536, static_cast<size_t>(size) - rdb.size());
        rdb.resize(rdb.size() + want);
        Status s = ReadExact(&rdb[rdb.size() - want], want);
        if (!s.ok()) return s;
    }

    // A RESP bulk string is followed by CRLF. Leaving it behind made the next
    // ReadFrame() see an empty header line, fail to parse a length from it, and
    // return Corruption -- so the stream loop spun on that stray byte forever
    // and never applied a single incremental write.
    char tail[2] = {0, 0};
    Status ts = ReadExact(tail, 2);
    if (!ts.ok()) return ts;
    if (tail[0] != '\r' || tail[1] != '\n') {
        return Status::Corruption("RDB payload is not CRLF-terminated");
    }

    if (rdb.size() < kRdbMagicLen + kRdbCrcLen) {
        return Status::Corruption("RDB payload is too short");
    }
    if (std::memcmp(rdb.data(), kRdbMagic, kRdbMagicLen) != 0) {
        return Status::Corruption("RDB magic mismatch; not a LightKV snapshot");
    }

    // Verify the trailer before trusting a single byte of the image. The master
    // used to write a literal 0 here, which made the checksum decorative.
    uint32_t want_crc = 0;
    for (int i = 0; i < 4; ++i) {
        want_crc |= static_cast<uint32_t>(
                        static_cast<uint8_t>(rdb[rdb.size() - 4 + i])) << (8 * i);
    }
    const uint32_t got_crc = Crc32c(rdb.data(), rdb.size() - 4);
    if (want_crc != got_crc) {
        char buf[80];
        snprintf(buf, sizeof(buf),
                 "RDB checksum mismatch: header says 0x%08x, payload is 0x%08x",
                 want_crc, got_crc);
        return Status::Corruption(buf);
    }

    size_t off = kRdbMagicLen;
    auto read32 = [&](uint32_t* v) -> bool {
        if (off + 4 > rdb.size()) return false;
        *v = 0;
        for (int i = 0; i < 4; ++i) {
            *v |= static_cast<uint32_t>(static_cast<uint8_t>(rdb[off + i])) << (8 * i);
        }
        off += 4;
        return true;
    };

    // Entries run up to the trailing 4-byte CRC, so the loop must stop while a full
    // length prefix still fits *with room for the trailer*. Using `<=` here made
    // the trailer itself look like a key length and every snapshot failed with
    // "truncated at a key body".
    while (off + 4 < rdb.size()) {
        uint32_t klen = 0;
        if (!read32(&klen)) break;
        if (off + klen + 4 > rdb.size()) {
            return Status::Corruption("RDB truncated at a key body");
        }
        std::string key(rdb.data() + off, klen);
        off += klen;

        uint32_t vlen = 0;
        if (!read32(&vlen)) return Status::Corruption("RDB truncated at a value length");
        if (off + vlen > rdb.size()) {
            return Status::Corruption("RDB truncated at a value body");
        }
        std::string value(rdb.data() + off, vlen);
        off += vlen;

        rows->emplace_back(std::move(key), std::move(value));
    }

    // No entries is not an error. An empty master is a perfectly ordinary state,
    // and treating it as corruption meant a replica could never follow one --
    // it kept retrying instead of adopting the (empty) image.
    return Status::OK();
}

// ─── Streaming ──────────────────────────────────────────────────────────

void ReplSlaveLink::SendAck() {
    std::string reply;
    SendCommand({"REPLCONF", "ACK", std::to_string(offset_.load())}, &reply);
}

void ReplSlaveLink::StartStreaming() {
    if (thread_.joinable()) return;
    thread_ = std::thread([this]() { StreamLoop(); });
}

void ReplSlaveLink::Stop() {
    stop_requested_.store(true);
    if (thread_.joinable()) thread_.join();
    CloseSocket();
    state_.store(ReplSlaveState::kDisconnected, std::memory_order_release);
}

void ReplSlaveLink::StreamLoop() {
    uint32_t backoff_ms = static_cast<uint32_t>(std::max(1, opts_.reconnect_min_ms));
    auto next_ack = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(std::max(1, opts_.ack_interval_ms));

    for (;;) {
        if (stop_requested_.load()) break;

        // A FULLRESYNC was agreed during the handshake and the master sends the
        // dump immediately after its PSYNC reply, so load it first. A CONTINUE
        // means no dump is coming -- waiting for one anyway would hang forever.
        Status s = Status::OK();
        std::vector<std::pair<std::string, std::string>> rows;
        bool got_image = false;
        if (pending_full_sync_) {
            pending_full_sync_ = false;
            s = ReadSnapshot(&rows);
            got_image = s.ok();
        }
        // An empty master is a legitimate full image: skipping it here left the
        // replica holding keys the master no longer has.
        if (s.ok() && got_image) {
            Status ls = sink_ ? sink_->LoadFullImage(rows) : Status::OK();
            if (!ls.ok()) {
                SetLastError("failed to install the master's image: " + ls.ToString());
                s = ls;
            }
        }
        if (!s.ok()) {
            fprintf(stderr, "[ReplSlave] snapshot load failed: %s\n",
                    s.ToString().c_str());
            SetLastError(s.ToString());
            CloseSocket();
            state_.store(ReplSlaveState::kDisconnected, std::memory_order_release);
            reconnects_.fetch_add(1);
            // Exponential backoff: a master that is down must not turn this into
            // a tight reconnect loop.
            std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
            backoff_ms = std::min<uint32_t>(
                backoff_ms * 2,
                static_cast<uint32_t>(std::max(1, opts_.reconnect_max_ms)));

            if (!stop_requested_.load()) {
                if (OpenSocket()) {
                    if (DoHandshake().ok()) {
                        state_.store(ReplSlaveState::kOnline, std::memory_order_release);
                        backoff_ms = static_cast<uint32_t>(
                            std::max(1, opts_.reconnect_min_ms));
                        continue;
                    }
                }
                CloseSocket();
            }
            continue;
        }

        state_.store(ReplSlaveState::kOnline, std::memory_order_release);
        backoff_ms = static_cast<uint32_t>(std::max(1, opts_.reconnect_min_ms));

        // Steady state: an endless sequence of replicated command arrays.
        while (!stop_requested_.load()) {
            if (std::chrono::steady_clock::now() >= next_ack) {
                SendAck();
                next_ack = std::chrono::steady_clock::now() +
                           std::chrono::milliseconds(std::max(1, opts_.ack_interval_ms));
            }

            std::string frame;
            Status rs = ReadFrame(&frame);
            if (!rs.ok()) break;

            if (frame.empty() || frame[0] != '*') continue;

            // Parse ["CMD", arg, ...] out of the raw frame. ReadFrame hands back the
            // whole array including its "*N\r\n" header, so step over that
            // first -- testing for '$' straight away never matched anything and
            // every replicated command was silently dropped.
            std::vector<std::string> args;
            size_t off = 0;
            if (off < frame.size() && frame[off] == '*') {
                const size_t nl = frame.find("\r\n", off);
                off = (nl == std::string::npos) ? frame.size() : nl + 2;
            }
            while (off < frame.size() && frame[off] == '$') {
                size_t nl = frame.find("\r\n", off);
                if (nl == std::string::npos) break;
                size_t len = 0;
                try {
                    len = static_cast<size_t>(std::stol(frame.substr(off + 1, nl - off - 1)));
                } catch (...) {
                    break;
                }
                if (nl + 2 + len > frame.size()) break;
                args.push_back(frame.substr(nl + 2, len));
                off = nl + 2 + len + 2;
            }
            if (args.empty()) continue;

            if (sink_) sink_->ApplyReplicatedCommand(args);
            offset_.fetch_add(frame.size(), std::memory_order_release);
        }

        if (stop_requested_.load()) break;
        CloseSocket();
        state_.store(ReplSlaveState::kDisconnected, std::memory_order_release);
    }
}

}  // namespace lightkv