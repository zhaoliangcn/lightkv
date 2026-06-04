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

    // String extension commands
    std::optional<int64_t> Incr(const std::string& key);
    std::optional<int64_t> Decr(const std::string& key);
    std::optional<int64_t> IncrBy(const std::string& key, int64_t delta);
    std::optional<int64_t> DecrBy(const std::string& key, int64_t delta);
    std::optional<std::string> IncrByFloat(const std::string& key, double delta);
    bool MSet(const std::vector<std::pair<std::string, std::string>>& kvs);
    std::vector<std::optional<std::string>> MGet(const std::vector<std::string>& keys);
    bool SetEx(const std::string& key, int64_t seconds, const std::string& value);
    bool SetNx(const std::string& key, const std::string& value);
    std::optional<std::string> GetSet(const std::string& key, const std::string& value);
    int64_t Append(const std::string& key, const std::string& value);
    int64_t StrLen(const std::string& key);

    // General commands
    int64_t Exists(const std::vector<std::string>& keys);
    bool Expire(const std::string& key, int64_t seconds);
    int64_t Ttl(const std::string& key);
    int64_t Pttl(const std::string& key);
    bool Persist(const std::string& key);
    std::string Type(const std::string& key);
    bool Rename(const std::string& key, const std::string& newkey);
    bool RenameNx(const std::string& key, const std::string& newkey);
    std::vector<std::string> Keys(const std::string& pattern);

    // Utility commands
    bool Ping();
    bool Quit();

    // Stats: returns key-value pairs
    std::vector<std::pair<std::string, std::string>> Stats();

    // Get last error message
    std::string GetLastError() const;

    // Pipeline operations
    void Pipeline();
    void Queue(const std::vector<std::string>& args);
    std::vector<std::string> ExecPipeline();

private:
    std::string send_command(const std::vector<std::string>& args);
    std::string build_resp(const std::vector<std::string>& args);
    std::optional<std::string> parse_resp(const std::string& resp);

    // Parse integer from RESP integer response like ":123\r\n"
    std::optional<int64_t> parse_integer(const std::string& resp);

    int fd_;
    std::string last_error_;
    std::vector<std::string> pipeline_buf_;
};

} // namespace lightkv
