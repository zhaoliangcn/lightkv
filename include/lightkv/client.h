#pragma once

#include <string>
#include <vector>
#include <cstdint>
#include <optional>

namespace lightkv {

// Client SDK for connecting to LightKV Server via TCP (Redis RESP protocol)
class Client {
public:
    Client();
    ~Client();

    // Connect to server
    bool Connect(const std::string& host = "127.0.0.1", uint16_t port = 6379);
    void Disconnect();
    bool IsConnected() const;

    // Core KV operations
    bool Set(const std::string& key, const std::string& value);
    std::optional<std::string> Get(const std::string& key);
    bool Delete(const std::string& key);
    bool DeleteRange(const std::string& begin, const std::string& end);

    // Utility commands
    bool Ping();
    bool Quit();

    // Stats: returns key-value pairs
    std::vector<std::pair<std::string, std::string>> Stats();

    // Get last error message
    std::string GetLastError() const;

private:
    std::string send_command(const std::vector<std::string>& args);
    std::string build_resp(const std::vector<std::string>& args);
    std::optional<std::string> parse_resp(const std::string& resp);

    int fd_;
    std::string last_error_;
};

} // namespace lightkv
