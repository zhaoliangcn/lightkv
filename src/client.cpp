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
        // Simple string: +OK\r\n
        size_t cr = resp.find("\r\n");
        if (cr == std::string::npos) return std::nullopt;
        return resp.substr(1, cr - 1);
    }
    if (type == '-') {
        // Error: -ERR message\r\n
        size_t cr = resp.find("\r\n");
        if (cr == std::string::npos) return std::nullopt;
        last_error_ = resp.substr(1, cr - 1);
        return std::nullopt;
    }
    if (type == ':') {
        // Integer: :123\r\n
        size_t cr = resp.find("\r\n");
        if (cr == std::string::npos) return std::nullopt;
        return resp.substr(1, cr - 1);
    }
    if (type == '$') {
        // Bulk string: $5\r\nhello\r\n or $-1\r\n
        size_t cr = resp.find("\r\n");
        if (cr == std::string::npos) return std::nullopt;
        int len = std::stoi(resp.substr(1, cr - 1));
        if (len < 0) return std::nullopt; // nil
        size_t data_start = cr + 2;
        size_t data_end = data_start + len;
        if (data_end > resp.size()) return std::nullopt;
        return resp.substr(data_start, len);
    }
    if (type == '*') {
        // Array: *N\r\n...
        return resp; // Return raw for array parsing
    }

    last_error_ = "Unknown RESP type: " + std::string(1, type);
    return std::nullopt;
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

} // namespace lightkv
