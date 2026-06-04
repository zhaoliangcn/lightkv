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

    std::string cmd = build_resp(args);
    ssize_t sent = ::send(fd_, cmd.data(), cmd.size(), MSG_NOSIGNAL);
    if (sent < 0) {
        last_error_ = "Failed to send command";
        return "";
    }

    // Read response
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
