#include "lightkv/client.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <netdb.h>
#include <cstring>
#include <cstdio>

namespace lightkv {

Client::Client() : fd_(-1) {}

Client::~Client() {
    Disconnect();
}

bool Client::Connect(const std::string& host, uint16_t port) {
    fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd_ < 0) {
        last_error_ = "Failed to create socket";
        return false;
    }

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = inet_addr(host.c_str());

    if (::connect(fd_, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        last_error_ = "Failed to connect to " + host + ":" + std::to_string(port);
        ::close(fd_);
        fd_ = -1;
        return false;
    }

    last_error_.clear();
    return true;
}

void Client::Disconnect() {
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
}

bool Client::IsConnected() const {
    return fd_ >= 0;
}

std::string Client::GetLastError() const {
    return last_error_;
}

// ─── RESP Protocol Builder ───

std::string Client::build_resp(const std::vector<std::string>& args) {
    std::string r = "*" + std::to_string(args.size()) + "\r\n";
    for (auto& arg : args) {
        r += "$" + std::to_string(arg.size()) + "\r\n" + arg + "\r\n";
    }
    return r;
}

std::string Client::send_command(const std::vector<std::string>& args) {
    if (fd_ < 0) {
        last_error_ = "Not connected";
        return "";
    }

    // Send command (handle partial writes)
    std::string cmd = build_resp(args);
    size_t total_sent = 0;
    while (total_sent < cmd.size()) {
        ssize_t sent = ::send(fd_, cmd.data() + total_sent, cmd.size() - total_sent, MSG_NOSIGNAL);
        if (sent < 0) {
            last_error_ = "Failed to send command";
            return "";
        }
        total_sent += static_cast<size_t>(sent);
    }

    // Read response
    // TODO: support large responses that exceed buffer size
    char buf[4096];
    ssize_t n = ::recv(fd_, buf, sizeof(buf) - 1, 0);
    if (n <= 0) {
        last_error_ = "Failed to receive response";
        return "";
    }
    buf[n] = '\0';
    return std::string(buf, n);
}

// ─── RESP Protocol Parser ───

std::optional<std::string> Client::parse_resp(const std::string& resp) {
    if (resp.empty()) return std::nullopt;

    char type = resp[0];
    if (type == '+') {
        size_t cr = resp.find("\r\n");
        if (cr == std::string::npos) return std::nullopt;
        return resp.substr(1, cr - 1);
    }
    if (type == '-') {
        size_t cr = resp.find("\r\n");
        if (cr == std::string::npos) return std::nullopt;
        last_error_ = resp.substr(1, cr - 1);
        return std::nullopt;
    }
    if (type == ':') {
        size_t cr = resp.find("\r\n");
        if (cr == std::string::npos) return std::nullopt;
        return resp.substr(1, cr - 1);
    }
    if (type == '$') {
        size_t cr = resp.find("\r\n");
        if (cr == std::string::npos) return std::nullopt;
        int len = std::stoi(resp.substr(1, cr - 1));
        if (len < 0) return std::nullopt;
        size_t data_start = cr + 2;
        size_t data_end = data_start + len;
        if (data_end > resp.size()) return std::nullopt;
        return resp.substr(data_start, len);
    }
    if (type == '*') {
        return resp;
    }

    last_error_ = "Unknown RESP type: " + std::string(1, type);
    return std::nullopt;
}

std::optional<int64_t> Client::parse_integer(const std::string& resp) {
    auto s = parse_resp(resp);
    if (!s.has_value()) return std::nullopt;
    try {
        return std::stoll(*s);
    } catch (...) {
        return std::nullopt;
    }
}

// ─── Core Operations ───

bool Client::Set(const std::string& key, const std::string& value) {
    auto resp = send_command({"SET", key, value});
    auto result = parse_resp(resp);
    return result.has_value() && *result == "OK";
}

std::optional<std::string> Client::Get(const std::string& key) {
    auto resp = send_command({"GET", key});
    return parse_resp(resp);
}

bool Client::Delete(const std::string& key) {
    auto resp = send_command({"DEL", key});
    auto result = parse_resp(resp);
    return result.has_value() && *result == "1";
}

bool Client::DeleteRange(const std::string& begin, const std::string& end) {
    auto resp = send_command({"DELRANGE", begin, end});
    auto result = parse_resp(resp);
    return result.has_value() && *result == "1";
}

// ─── String Extension Commands ───

std::optional<int64_t> Client::Incr(const std::string& key) {
    auto resp = send_command({"INCR", key});
    return parse_integer(resp);
}

std::optional<int64_t> Client::Decr(const std::string& key) {
    auto resp = send_command({"DECR", key});
    return parse_integer(resp);
}

std::optional<int64_t> Client::IncrBy(const std::string& key, int64_t delta) {
    auto resp = send_command({"INCRBY", key, std::to_string(delta)});
    return parse_integer(resp);
}

std::optional<int64_t> Client::DecrBy(const std::string& key, int64_t delta) {
    auto resp = send_command({"DECRBY", key, std::to_string(delta)});
    return parse_integer(resp);
}

std::optional<std::string> Client::IncrByFloat(const std::string& key, double delta) {
    auto resp = send_command({"INCRBYFLOAT", key, std::to_string(delta)});
    return parse_resp(resp);
}

bool Client::MSet(const std::vector<std::pair<std::string, std::string>>& kvs) {
    std::vector<std::string> args = {"MSET"};
    for (auto& kv : kvs) {
        args.push_back(kv.first);
        args.push_back(kv.second);
    }
    auto resp = send_command(args);
    auto result = parse_resp(resp);
    return result.has_value() && *result == "OK";
}

std::vector<std::optional<std::string>> Client::MGet(const std::vector<std::string>& keys) {
    std::vector<std::string> args = {"MGET"};
    args.insert(args.end(), keys.begin(), keys.end());
    auto resp = send_command(args);

    std::vector<std::optional<std::string>> result;
    if (resp.empty() || resp[0] != '*') return result;

    size_t cr = resp.find("\r\n");
    if (cr == std::string::npos) return result;
    int count = std::stoi(resp.substr(1, cr - 1));
    size_t pos = cr + 2;

    for (int i = 0; i < count; ++i) {
        if (pos >= resp.size()) { result.emplace_back(std::nullopt); continue; }
        if (resp[pos] == '$') {
            size_t elem_cr = resp.find("\r\n", pos);
            if (elem_cr == std::string::npos) { result.emplace_back(std::nullopt); continue; }
            int len = std::stoi(resp.substr(pos + 1, elem_cr - pos - 1));
            if (len < 0) {
                result.emplace_back(std::nullopt);
                pos = elem_cr + 2;
            } else {
                size_t data_start = elem_cr + 2;
                result.emplace_back(resp.substr(data_start, len));
                pos = data_start + len + 2;
            }
        } else {
            result.emplace_back(std::nullopt);
        }
    }
    return result;
}

bool Client::SetEx(const std::string& key, int64_t seconds, const std::string& value) {
    auto resp = send_command({"SETEX", key, std::to_string(seconds), value});
    auto result = parse_resp(resp);
    return result.has_value() && *result == "OK";
}

bool Client::SetNx(const std::string& key, const std::string& value) {
    auto resp = send_command({"SETNX", key, value});
    auto r = parse_integer(resp);
    return r.has_value() && *r == 1;
}

std::optional<std::string> Client::GetSet(const std::string& key, const std::string& value) {
    auto resp = send_command({"GETSET", key, value});
    return parse_resp(resp);
}

int64_t Client::Append(const std::string& key, const std::string& value) {
    auto resp = send_command({"APPEND", key, value});
    auto r = parse_integer(resp);
    return r.value_or(-1);
}

int64_t Client::StrLen(const std::string& key) {
    auto resp = send_command({"STRLEN", key});
    auto r = parse_integer(resp);
    return r.value_or(0);
}

// ─── General Commands ───

int64_t Client::Exists(const std::vector<std::string>& keys) {
    std::vector<std::string> args = {"EXISTS"};
    args.insert(args.end(), keys.begin(), keys.end());
    auto resp = send_command(args);
    auto r = parse_integer(resp);
    return r.value_or(0);
}

bool Client::Expire(const std::string& key, int64_t seconds) {
    auto resp = send_command({"EXPIRE", key, std::to_string(seconds)});
    auto r = parse_integer(resp);
    return r.has_value() && *r == 1;
}

int64_t Client::Ttl(const std::string& key) {
    auto resp = send_command({"TTL", key});
    auto r = parse_integer(resp);
    return r.value_or(-3);
}

int64_t Client::Pttl(const std::string& key) {
    auto resp = send_command({"PTTL", key});
    auto r = parse_integer(resp);
    return r.value_or(-3);
}

bool Client::Persist(const std::string& key) {
    auto resp = send_command({"PERSIST", key});
    auto r = parse_integer(resp);
    return r.has_value() && *r == 1;
}

std::string Client::Type(const std::string& key) {
    auto resp = send_command({"TYPE", key});
    auto r = parse_resp(resp);
    return r.value_or("none");
}

bool Client::Rename(const std::string& key, const std::string& newkey) {
    auto resp = send_command({"RENAME", key, newkey});
    auto r = parse_resp(resp);
    return r.has_value() && *r == "OK";
}

bool Client::RenameNx(const std::string& key, const std::string& newkey) {
    auto resp = send_command({"RENAMENX", key, newkey});
    auto r = parse_integer(resp);
    return r.has_value() && *r == 1;
}

std::vector<std::string> Client::Keys(const std::string& pattern) {
    auto resp = send_command({"KEYS", pattern});
    std::vector<std::string> result;
    if (resp.empty() || resp[0] != '*') return result;

    size_t cr = resp.find("\r\n");
    if (cr == std::string::npos) return result;
    int count = std::stoi(resp.substr(1, cr - 1));
    size_t pos = cr + 2;

    for (int i = 0; i < count; ++i) {
        if (pos >= resp.size() || resp[pos] != '$') break;
        size_t elem_cr = resp.find("\r\n", pos);
        if (elem_cr == std::string::npos) break;
        int len = std::stoi(resp.substr(pos + 1, elem_cr - pos - 1));
        if (len < 0) { pos = elem_cr + 2; continue; }
        size_t data_start = elem_cr + 2;
        result.push_back(resp.substr(data_start, len));
        pos = data_start + len + 2;
    }
    return result;
}

// ─── Hash Commands ───

int64_t Client::HSet(const std::string& key, const std::vector<std::pair<std::string, std::string>>& fields) {
    std::vector<std::string> args = {"HSET", key};
    for (auto& f : fields) {
        args.push_back(f.first);
        args.push_back(f.second);
    }
    auto resp = send_command(args);
    auto r = parse_integer(resp);
    return r.value_or(0);
}

std::optional<std::string> Client::HGet(const std::string& key, const std::string& field) {
    auto resp = send_command({"HGET", key, field});
    return parse_resp(resp);
}

bool Client::HMSet(const std::string& key, const std::vector<std::pair<std::string, std::string>>& fields) {
    std::vector<std::string> args = {"HMSET", key};
    for (auto& f : fields) {
        args.push_back(f.first);
        args.push_back(f.second);
    }
    auto resp = send_command(args);
    auto r = parse_resp(resp);
    return r.has_value() && *r == "OK";
}

std::vector<std::optional<std::string>> Client::HMGet(const std::string& key, const std::vector<std::string>& fields) {
    std::vector<std::string> args = {"HMGET", key};
    args.insert(args.end(), fields.begin(), fields.end());
    auto resp = send_command(args);
    std::vector<std::optional<std::string>> result;
    if (resp.empty() || resp[0] != '*') return result;
    size_t cr = resp.find("\r\n");
    if (cr == std::string::npos) return result;
    int count = std::stoi(resp.substr(1, cr - 1));
    size_t pos = cr + 2;
    for (int i = 0; i < count; ++i) {
        if (pos >= resp.size()) { result.emplace_back(std::nullopt); continue; }
        if (resp[pos] == '$') {
            size_t elem_cr = resp.find("\r\n", pos);
            if (elem_cr == std::string::npos) { result.emplace_back(std::nullopt); continue; }
            int len = std::stoi(resp.substr(pos + 1, elem_cr - pos - 1));
            if (len < 0) { result.emplace_back(std::nullopt); pos = elem_cr + 2; }
            else { size_t ds = elem_cr + 2; result.emplace_back(resp.substr(ds, len)); pos = ds + len + 2; }
        } else { result.emplace_back(std::nullopt); }
    }
    return result;
}

std::vector<std::pair<std::string, std::string>> Client::HGetAll(const std::string& key) {
    auto resp = send_command({"HGETALL", key});
    std::vector<std::pair<std::string, std::string>> result;
    if (resp.empty() || resp[0] != '*') return result;
    size_t cr = resp.find("\r\n");
    if (cr == std::string::npos) return result;
    int count = std::stoi(resp.substr(1, cr - 1));
    size_t pos = cr + 2;
    for (int i = 0; i < count / 2; ++i) {
        if (pos >= resp.size() || resp[pos] != '$') break;
        size_t kcr = resp.find("\r\n", pos);
        if (kcr == std::string::npos) break;
        int klen = std::stoi(resp.substr(pos + 1, kcr - pos - 1));
        size_t ks = kcr + 2;
        std::string k = resp.substr(ks, klen);
        pos = ks + klen + 2;
        if (pos >= resp.size() || resp[pos] != '$') break;
        size_t vcr = resp.find("\r\n", pos);
        if (vcr == std::string::npos) break;
        int vlen = std::stoi(resp.substr(pos + 1, vcr - pos - 1));
        size_t vs = vcr + 2;
        std::string v = resp.substr(vs, vlen);
        pos = vs + vlen + 2;
        result.emplace_back(k, v);
    }
    return result;
}

int64_t Client::HDel(const std::string& key, const std::vector<std::string>& fields) {
    std::vector<std::string> args = {"HDEL", key};
    args.insert(args.end(), fields.begin(), fields.end());
    auto resp = send_command(args);
    auto r = parse_integer(resp);
    return r.value_or(0);
}

bool Client::HExists(const std::string& key, const std::string& field) {
    auto resp = send_command({"HEXISTS", key, field});
    auto r = parse_integer(resp);
    return r.has_value() && *r == 1;
}

int64_t Client::HLen(const std::string& key) {
    auto resp = send_command({"HLEN", key});
    auto r = parse_integer(resp);
    return r.value_or(0);
}

std::vector<std::string> Client::HKeys(const std::string& key) {
    auto resp = send_command({"HKEYS", key});
    std::vector<std::string> result;
    if (resp.empty() || resp[0] != '*') return result;
    size_t cr = resp.find("\r\n");
    if (cr == std::string::npos) return result;
    int count = std::stoi(resp.substr(1, cr - 1));
    size_t pos = cr + 2;
    for (int i = 0; i < count; ++i) {
        if (pos >= resp.size() || resp[pos] != '$') break;
        size_t ecr = resp.find("\r\n", pos);
        if (ecr == std::string::npos) break;
        int len = std::stoi(resp.substr(pos + 1, ecr - pos - 1));
        if (len < 0) { pos = ecr + 2; continue; }
        size_t ds = ecr + 2;
        result.push_back(resp.substr(ds, len));
        pos = ds + len + 2;
    }
    return result;
}

std::vector<std::string> Client::HVals(const std::string& key) {
    auto resp = send_command({"HVALS", key});
    std::vector<std::string> result;
    if (resp.empty() || resp[0] != '*') return result;
    size_t cr = resp.find("\r\n");
    if (cr == std::string::npos) return result;
    int count = std::stoi(resp.substr(1, cr - 1));
    size_t pos = cr + 2;
    for (int i = 0; i < count; ++i) {
        if (pos >= resp.size() || resp[pos] != '$') break;
        size_t ecr = resp.find("\r\n", pos);
        if (ecr == std::string::npos) break;
        int len = std::stoi(resp.substr(pos + 1, ecr - pos - 1));
        if (len < 0) { pos = ecr + 2; continue; }
        size_t ds = ecr + 2;
        result.push_back(resp.substr(ds, len));
        pos = ds + len + 2;
    }
    return result;
}

int64_t Client::HIncrBy(const std::string& key, const std::string& field, int64_t delta) {
    auto resp = send_command({"HINCRBY", key, field, std::to_string(delta)});
    auto r = parse_integer(resp);
    return r.value_or(0);
}

int64_t Client::HStrLen(const std::string& key, const std::string& field) {
    auto resp = send_command({"HSTRLEN", key, field});
    auto r = parse_integer(resp);
    return r.value_or(0);
}

// ─── List Commands ───

int64_t Client::LPush(const std::string& key, const std::vector<std::string>& values) {
    std::vector<std::string> args = {"LPUSH", key};
    args.insert(args.end(), values.begin(), values.end());
    auto resp = send_command(args);
    auto r = parse_integer(resp);
    return r.value_or(0);
}

int64_t Client::RPush(const std::string& key, const std::vector<std::string>& values) {
    std::vector<std::string> args = {"RPUSH", key};
    args.insert(args.end(), values.begin(), values.end());
    auto resp = send_command(args);
    auto r = parse_integer(resp);
    return r.value_or(0);
}

std::optional<std::string> Client::LPop(const std::string& key) {
    auto resp = send_command({"LPOP", key});
    return parse_resp(resp);
}

std::optional<std::string> Client::RPop(const std::string& key) {
    auto resp = send_command({"RPOP", key});
    return parse_resp(resp);
}

std::vector<std::string> Client::LRange(const std::string& key, int64_t start, int64_t stop) {
    auto resp = send_command({"LRANGE", key, std::to_string(start), std::to_string(stop)});
    std::vector<std::string> result;
    if (resp.empty() || resp[0] != '*') return result;
    size_t cr = resp.find("\r\n");
    if (cr == std::string::npos) return result;
    int count = std::stoi(resp.substr(1, cr - 1));
    size_t pos = cr + 2;
    for (int i = 0; i < count; ++i) {
        if (pos >= resp.size() || resp[pos] != '$') break;
        size_t ecr = resp.find("\r\n", pos);
        if (ecr == std::string::npos) break;
        int len = std::stoi(resp.substr(pos + 1, ecr - pos - 1));
        if (len < 0) { pos = ecr + 2; continue; }
        size_t ds = ecr + 2;
        result.push_back(resp.substr(ds, len));
        pos = ds + len + 2;
    }
    return result;
}

int64_t Client::LLen(const std::string& key) {
    auto resp = send_command({"LLEN", key});
    auto r = parse_integer(resp);
    return r.value_or(0);
}

std::optional<std::string> Client::LIndex(const std::string& key, int64_t idx) {
    auto resp = send_command({"LINDEX", key, std::to_string(idx)});
    return parse_resp(resp);
}

bool Client::LSet(const std::string& key, int64_t idx, const std::string& value) {
    auto resp = send_command({"LSET", key, std::to_string(idx), value});
    auto r = parse_resp(resp);
    return r.has_value() && *r == "OK";
}

bool Client::LTrim(const std::string& key, int64_t start, int64_t stop) {
    auto resp = send_command({"LTRIM", key, std::to_string(start), std::to_string(stop)});
    auto r = parse_resp(resp);
    return r.has_value() && *r == "OK";
}

int64_t Client::LRem(const std::string& key, int64_t count, const std::string& value) {
    auto resp = send_command({"LREM", key, std::to_string(count), value});
    auto r = parse_integer(resp);
    return r.value_or(0);
}

// ─── Set Commands ───

int64_t Client::SAdd(const std::string& key, const std::vector<std::string>& members) {
    std::vector<std::string> args = {"SADD", key};
    args.insert(args.end(), members.begin(), members.end());
    auto resp = send_command(args);
    auto r = parse_integer(resp);
    return r.value_or(0);
}

int64_t Client::SRem(const std::string& key, const std::vector<std::string>& members) {
    std::vector<std::string> args = {"SREM", key};
    args.insert(args.end(), members.begin(), members.end());
    auto resp = send_command(args);
    auto r = parse_integer(resp);
    return r.value_or(0);
}

std::vector<std::string> Client::SMembers(const std::string& key) {
    auto resp = send_command({"SMEMBERS", key});
    std::vector<std::string> result;
    if (resp.empty() || resp[0] != '*') return result;
    size_t cr = resp.find("\r\n");
    if (cr == std::string::npos) return result;
    int count = std::stoi(resp.substr(1, cr - 1));
    size_t pos = cr + 2;
    for (int i = 0; i < count; ++i) {
        if (pos >= resp.size() || resp[pos] != '$') break;
        size_t ecr = resp.find("\r\n", pos);
        if (ecr == std::string::npos) break;
        int len = std::stoi(resp.substr(pos + 1, ecr - pos - 1));
        if (len < 0) { pos = ecr + 2; continue; }
        size_t ds = ecr + 2;
        result.push_back(resp.substr(ds, len));
        pos = ds + len + 2;
    }
    return result;
}

bool Client::SIsMember(const std::string& key, const std::string& member) {
    auto resp = send_command({"SISMEMBER", key, member});
    auto r = parse_integer(resp);
    return r.has_value() && *r == 1;
}

int64_t Client::SCard(const std::string& key) {
    auto resp = send_command({"SCARD", key});
    auto r = parse_integer(resp);
    return r.value_or(0);
}

std::optional<std::string> Client::SPop(const std::string& key) {
    auto resp = send_command({"SPOP", key});
    return parse_resp(resp);
}

std::optional<std::string> Client::SRandMember(const std::string& key) {
    auto resp = send_command({"SRANDMEMBER", key});
    return parse_resp(resp);
}

bool Client::SMove(const std::string& src, const std::string& dst, const std::string& member) {
    auto resp = send_command({"SMOVE", src, dst, member});
    auto r = parse_integer(resp);
    return r.has_value() && *r == 1;
}

// ═══════════════════════════════════════════════════════════════
// P2: Bitmap Commands
// ═══════════════════════════════════════════════════════════════

int64_t Client::SetBit(const std::string& key, int64_t offset, int64_t value) {
    auto resp = send_command({"SETBIT", key, std::to_string(offset), std::to_string(value)});
    auto r = parse_integer(resp);
    return r.value_or(0);
}

int64_t Client::GetBit(const std::string& key, int64_t offset) {
    auto resp = send_command({"GETBIT", key, std::to_string(offset)});
    auto r = parse_integer(resp);
    return r.value_or(0);
}

int64_t Client::BitCount(const std::string& key, int64_t start, int64_t end) {
    auto resp = send_command({"BITCOUNT", key, std::to_string(start), std::to_string(end)});
    auto r = parse_integer(resp);
    return r.value_or(0);
}

int64_t Client::BitPos(const std::string& key, int64_t bit, int64_t start, int64_t end) {
    auto resp = send_command({"BITPOS", key, std::to_string(bit), std::to_string(start), std::to_string(end)});
    auto r = parse_integer(resp);
    return r.value_or(-1);
}

// ═══════════════════════════════════════════════════════════════
// P2: HyperLogLog Commands
// ═══════════════════════════════════════════════════════════════

int64_t Client::PfAdd(const std::string& key, const std::vector<std::string>& elements) {
    std::vector<std::string> args = {"PFADD", key};
    args.insert(args.end(), elements.begin(), elements.end());
    auto resp = send_command(args);
    auto r = parse_integer(resp);
    return r.value_or(0);
}

int64_t Client::PfCount(const std::vector<std::string>& keys) {
    std::vector<std::string> args = {"PFCOUNT"};
    args.insert(args.end(), keys.begin(), keys.end());
    auto resp = send_command(args);
    auto r = parse_integer(resp);
    return r.value_or(0);
}

bool Client::PfMerge(const std::string& dest, const std::vector<std::string>& sources) {
    std::vector<std::string> args = {"PFMERGE", dest};
    args.insert(args.end(), sources.begin(), sources.end());
    auto resp = send_command(args);
    auto result = parse_resp(resp);
    return result.has_value() && *result == "OK";
}

// ═══════════════════════════════════════════════════════════════
// P2: ZSet Commands
// ═══════════════════════════════════════════════════════════════

int64_t Client::ZAdd(const std::string& key, const std::vector<std::pair<double, std::string>>& members) {
    std::vector<std::string> args = {"ZADD", key};
    for (auto& m : members) {
        args.push_back(std::to_string(m.first));
        args.push_back(m.second);
    }
    auto resp = send_command(args);
    auto r = parse_integer(resp);
    return r.value_or(0);
}

int64_t Client::ZRem(const std::string& key, const std::vector<std::string>& members) {
    std::vector<std::string> args = {"ZREM", key};
    args.insert(args.end(), members.begin(), members.end());
    auto resp = send_command(args);
    auto r = parse_integer(resp);
    return r.value_or(0);
}

std::optional<std::string> Client::ZScore(const std::string& key, const std::string& member) {
    auto resp = send_command({"ZSCORE", key, member});
    return parse_resp(resp);
}

std::vector<std::string> Client::ZRange(const std::string& key, int64_t start, int64_t stop, bool withscores) {
    std::vector<std::string> args = {"ZRANGE", key, std::to_string(start), std::to_string(stop)};
    if (withscores) args.push_back("WITHSCORES");
    auto resp = send_command(args);
    std::vector<std::string> result;
    if (resp.empty() || resp[0] != '*') return result;
    size_t cr = resp.find("\r\n");
    if (cr == std::string::npos) return result;
    int count = std::stoi(resp.substr(1, cr - 1));
    size_t pos = cr + 2;
    for (int i = 0; i < count; ++i) {
        if (pos >= resp.size() || resp[pos] != '$') break;
        size_t bcr = resp.find("\r\n", pos);
        if (bcr == std::string::npos) break;
        int blen = std::stoi(resp.substr(pos + 1, bcr - pos - 1));
        size_t bs = bcr + 2;
        result.push_back(resp.substr(bs, blen));
        pos = bs + blen + 2;
    }
    return result;
}

std::vector<std::pair<std::string, double>> Client::ZRangeWithScores(const std::string& key, int64_t start, int64_t stop) {
    auto arr = ZRange(key, start, stop, true);
    std::vector<std::pair<std::string, double>> result;
    for (size_t i = 0; i + 1 < arr.size(); i += 2) {
        try {
            double score = std::stod(arr[i + 1]);
            result.emplace_back(arr[i], score);
        } catch (...) {
            result.emplace_back(arr[i], 0.0);
        }
    }
    return result;
}

int64_t Client::ZCard(const std::string& key) {
    auto resp = send_command({"ZCARD", key});
    auto r = parse_integer(resp);
    return r.value_or(0);
}

int64_t Client::ZCount(const std::string& key, const std::string& min, const std::string& max) {
    auto resp = send_command({"ZCOUNT", key, min, max});
    auto r = parse_integer(resp);
    return r.value_or(0);
}

std::vector<std::string> Client::ZRangeByScore(const std::string& key, const std::string& min, const std::string& max,
                                                int64_t offset, int64_t count, bool withscores) {
    std::vector<std::string> args = {"ZRANGEBYSCORE", key, min, max};
    if (offset != 0 || count != -1) {
        args.push_back("LIMIT");
        args.push_back(std::to_string(offset));
        args.push_back(std::to_string(count));
    }
    if (withscores) args.push_back("WITHSCORES");
    auto resp = send_command(args);
    std::vector<std::string> result;
    if (resp.empty() || resp[0] != '*') return result;
    size_t cr = resp.find("\r\n");
    if (cr == std::string::npos) return result;
    int arr_count = std::stoi(resp.substr(1, cr - 1));
    size_t pos = cr + 2;
    for (int i = 0; i < arr_count; ++i) {
        if (pos >= resp.size() || resp[pos] != '$') break;
        size_t bcr = resp.find("\r\n", pos);
        if (bcr == std::string::npos) break;
        int blen = std::stoi(resp.substr(pos + 1, bcr - pos - 1));
        size_t bs = bcr + 2;
        result.push_back(resp.substr(bs, blen));
        pos = bs + blen + 2;
    }
    return result;
}

std::vector<std::string> Client::ZRevRange(const std::string& key, int64_t start, int64_t stop, bool withscores) {
    std::vector<std::string> args = {"ZREVRANGE", key, std::to_string(start), std::to_string(stop)};
    if (withscores) args.push_back("WITHSCORES");
    auto resp = send_command(args);
    std::vector<std::string> result;
    if (resp.empty() || resp[0] != '*') return result;
    size_t cr = resp.find("\r\n");
    if (cr == std::string::npos) return result;
    int count = std::stoi(resp.substr(1, cr - 1));
    size_t pos = cr + 2;
    for (int i = 0; i < count; ++i) {
        if (pos >= resp.size() || resp[pos] != '$') break;
        size_t bcr = resp.find("\r\n", pos);
        if (bcr == std::string::npos) break;
        int blen = std::stoi(resp.substr(pos + 1, bcr - pos - 1));
        size_t bs = bcr + 2;
        result.push_back(resp.substr(bs, blen));
        pos = bs + blen + 2;
    }
    return result;
}

std::optional<int64_t> Client::ZRank(const std::string& key, const std::string& member) {
    auto resp = send_command({"ZRANK", key, member});
    if (resp.empty() || resp[0] == '$') {
        // nil bulk string ($-1\r\n) means member not found
        size_t cr = resp.find("\r\n");
        if (cr != std::string::npos && resp.size() >= 4 && resp.substr(0, 3) == "$-1") {
            return std::nullopt;
        }
        return std::nullopt;
    }
    if (resp[0] == ':') {
        size_t cr = resp.find("\r\n");
        if (cr != std::string::npos) {
            try { return std::stoll(resp.substr(1, cr - 1)); } catch (...) {}
        }
    }
    return std::nullopt;
}

std::optional<int64_t> Client::ZRevRank(const std::string& key, const std::string& member) {
    auto resp = send_command({"ZREVRANK", key, member});
    if (resp.empty()) return std::nullopt;
    if (resp[0] == '$') {
        size_t cr = resp.find("\r\n");
        if (cr != std::string::npos && resp.size() >= 4 && resp.substr(0, 3) == "$-1") {
            return std::nullopt;
        }
        return std::nullopt;
    }
    if (resp[0] == ':') {
        size_t cr = resp.find("\r\n");
        if (cr != std::string::npos) {
            try { return std::stoll(resp.substr(1, cr - 1)); } catch (...) {}
        }
    }
    return std::nullopt;
}

int64_t Client::GeoAdd(const std::string& key, const std::vector<std::tuple<double, double, std::string>>& members) {
    std::vector<std::string> args = {"GEOADD", key};
    for (auto& m : members) {
        args.push_back(std::to_string(std::get<0>(m)));
        args.push_back(std::to_string(std::get<1>(m)));
        args.push_back(std::get<2>(m));
    }
    auto resp = send_command(args);
    auto r = parse_integer(resp);
    return r.value_or(0);
}

std::vector<std::pair<double, double>> Client::GeoPos(const std::string& key, const std::vector<std::string>& members) {
    std::vector<std::string> args = {"GEOPOS", key};
    args.insert(args.end(), members.begin(), members.end());
    auto resp = send_command(args);
    std::vector<std::pair<double, double>> result;
    if (resp.empty() || resp[0] != '*') return result;
    size_t cr = resp.find("\r\n");
    if (cr == std::string::npos) return result;
    int count = std::stoi(resp.substr(1, cr - 1));
    size_t pos = cr + 2;
    // Server returns flat array: [lon1, lat1, lon2, lat2, ...]
    // Each pair of values represents one position
    for (int i = 0; i < count / 2; ++i) {
        if (pos >= resp.size() || resp[pos] != '$') {
            result.push_back({0.0, 0.0});
            continue;
        }
        size_t bcr1 = resp.find("\r\n", pos);
        if (bcr1 == std::string::npos) break;
        int len1 = std::stoi(resp.substr(pos + 1, bcr1 - pos - 1));
        size_t bs1 = bcr1 + 2;
        std::string val1 = (len1 > 0) ? resp.substr(bs1, len1) : "";
        pos = bs1 + len1 + 2;

        if (pos >= resp.size() || resp[pos] != '$') {
            result.push_back({0.0, 0.0});
            continue;
        }
        size_t bcr2 = resp.find("\r\n", pos);
        if (bcr2 == std::string::npos) break;
        int len2 = std::stoi(resp.substr(pos + 1, bcr2 - pos - 1));
        size_t bs2 = bcr2 + 2;
        std::string val2 = (len2 > 0) ? resp.substr(bs2, len2) : "";
        pos = bs2 + len2 + 2;

        double lon = val1.empty() ? 0.0 : std::stod(val1);
        double lat = val2.empty() ? 0.0 : std::stod(val2);
        result.push_back({lon, lat});
    }
    return result;
}

std::optional<double> Client::GeoDist(const std::string& key, const std::string& member1, const std::string& member2,
                                      const std::string& unit) {
    std::vector<std::string> args = {"GEODIST", key, member1, member2};
    if (!unit.empty()) args.push_back(unit);
    auto resp = send_command(args);
    if (resp.empty() || (resp[0] == '$' && resp.substr(1, 2) == "-1")) return std::nullopt;
    // Parse bulk string response
    if (resp[0] == '$') {
        size_t cr = resp.find("\r\n");
        if (cr != std::string::npos) {
            int len = std::stoi(resp.substr(1, cr - 1));
            if (len > 0) {
                size_t bs = cr + 2;
                std::string val = resp.substr(bs, len);
                return std::stod(val);
            }
        }
    }
    return std::nullopt;
}

bool Client::Ping() {
    auto resp = send_command({"PING"});
    auto result = parse_resp(resp);
    return result.has_value() && *result == "PONG";
}

bool Client::Quit() {
    auto resp = send_command({"QUIT"});
    Disconnect();
    return true;
}

bool Client::Auth(const std::string& password) {
    auto resp = send_command({"AUTH", password});
    return resp == "+OK\r\n";
}

std::vector<std::pair<std::string, std::string>> Client::ConfigGet(const std::string& param) {
    auto resp = send_command({"CONFIG", "GET", param});
    std::vector<std::pair<std::string, std::string>> result;
    if (resp.empty()) {
        last_error_ = "Empty response from server";
        return result;
    }
    if (resp[0] == '-') {
        // Error response
        size_t cr = resp.find("\r\n");
        last_error_ = cr != std::string::npos ? resp.substr(1, cr - 1) : resp;
        return result;
    }
    if (resp[0] != '*') {
        last_error_ = "Expected array response, got: " + std::string(1, resp[0]);
        return result;
    }
    // Parse RESP array: *2\r\n$11\r\nrequirepass\r\n$9\r\nmysecret\r\n
    size_t pos = 0;
    // Skip array header
    while (pos < resp.size() && resp[pos] != '\r') pos++;
    pos += 2; // skip \r\n
    while (pos < resp.size()) {
        if (pos + 1 >= resp.size()) break;
        if (resp[pos] == '$') {
            pos++; // skip $
            size_t len_start = pos;
            while (pos < resp.size() && resp[pos] != '\r') pos++;
            size_t len;
            try { len = std::stoull(resp.substr(len_start, pos - len_start)); } catch (...) { break; }
            pos += 2; // skip \r\n
            std::string val = resp.substr(pos, len);
            pos += len + 2; // skip value + \r\n
            if (!result.empty() && result.back().second.empty()) {
                result.back().second = val;
            } else {
                result.push_back({val, ""});
            }
        } else {
            break;
        }
    }
    return result;
}

bool Client::ConfigSet(const std::string& param, const std::string& value) {
    auto resp = send_command({"CONFIG", "SET", param, value});
    return resp == "+OK\r\n";
}

std::vector<std::pair<std::string, std::string>> Client::Stats() {
    auto resp = send_command({"STATS"});
    std::vector<std::pair<std::string, std::string>> result;

    if (resp.empty() || resp[0] != '*') return result;

    // Parse array
    size_t cr = resp.find("\r\n");
    if (cr == std::string::npos) return result;

    int count = std::stoi(resp.substr(1, cr - 1));
    size_t pos = cr + 2;

    for (int i = 0; i < count / 2; ++i) {
        // Parse key
        if (pos >= resp.size() || resp[pos] != '$') break;
        size_t key_cr = resp.find("\r\n", pos);
        if (key_cr == std::string::npos) break;
        int key_len = std::stoi(resp.substr(pos + 1, key_cr - pos - 1));
        size_t key_start = key_cr + 2;
        std::string key = resp.substr(key_start, key_len);
        pos = key_start + key_len + 2;

        // Parse value
        if (pos >= resp.size() || resp[pos] != '$') break;
        size_t val_cr = resp.find("\r\n", pos);
        if (val_cr == std::string::npos) break;
        int val_len = std::stoi(resp.substr(pos + 1, val_cr - pos - 1));
        size_t val_start = val_cr + 2;
        std::string value = resp.substr(val_start, val_len);
        pos = val_start + val_len + 2;

        result.emplace_back(key, value);
    }

    return result;
}

// ─── Pipeline Operations ───

void Client::Pipeline() {
    pipeline_buf_.clear();
}

void Client::Queue(const std::vector<std::string>& args) {
    pipeline_buf_.push_back(build_resp(args));
}

std::vector<std::string> Client::ExecPipeline() {
    if (fd_ < 0 || pipeline_buf_.empty()) return {};

    // Send all commands in one write
    std::string all_cmds;
    for (auto& cmd : pipeline_buf_) {
        all_cmds += cmd;
    }

    ssize_t sent = ::send(fd_, all_cmds.data(), all_cmds.size(), MSG_NOSIGNAL);
    if (sent < 0) {
        last_error_ = "Failed to send pipeline";
        pipeline_buf_.clear();
        return {};
    }

    // Read all responses with buffering
    std::vector<std::string> results;
    results.reserve(pipeline_buf_.size());
    size_t count = pipeline_buf_.size();

    char buf[8192];
    std::string buffer;
    size_t responses_received = 0;

    while (responses_received < count) {
        ssize_t n = ::recv(fd_, buf, sizeof(buf) - 1, 0);
        if (n <= 0) {
            last_error_ = "Failed to receive pipeline response";
            break;
        }
        buf[n] = '\0';
        buffer.append(buf, n);

        // Parse complete RESP responses from buffer
        while (!buffer.empty() && responses_received < count) {
            size_t cr = buffer.find("\r\n");
            if (cr == std::string::npos) break;

            char type = buffer[0];
            size_t resp_end = 0;

            if (type == '+' || type == '-' || type == ':') {
                // Simple types: +OK\r\n or -ERR\r\n or :1\r\n
                resp_end = cr + 2;
            } else if (type == '$') {
                // Bulk string: $5\r\nhello\r\n or $-1\r\n
                int len = std::stoi(buffer.substr(1, cr - 1));
                if (len < 0) {
                    resp_end = cr + 2; // $-1\r\n
                } else {
                    resp_end = cr + 2 + len + 2; // $len\r\ndata\r\n
                }
            } else if (type == '*') {
                // Array - skip for now
                resp_end = cr + 2;
            } else {
                resp_end = cr + 2;
            }

            if (resp_end > buffer.size()) break; // Incomplete response

            results.emplace_back(buffer.substr(0, resp_end));
            buffer.erase(0, resp_end);
            responses_received++;
        }
    }

    pipeline_buf_.clear();
    return results;
}

} // namespace lightkv
