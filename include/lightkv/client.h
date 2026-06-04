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

    // Hash commands
    int64_t HSet(const std::string& key, const std::vector<std::pair<std::string, std::string>>& fields);
    std::optional<std::string> HGet(const std::string& key, const std::string& field);
    bool HMSet(const std::string& key, const std::vector<std::pair<std::string, std::string>>& fields);
    std::vector<std::optional<std::string>> HMGet(const std::string& key, const std::vector<std::string>& fields);
    std::vector<std::pair<std::string, std::string>> HGetAll(const std::string& key);
    int64_t HDel(const std::string& key, const std::vector<std::string>& fields);
    bool HExists(const std::string& key, const std::string& field);
    int64_t HLen(const std::string& key);
    std::vector<std::string> HKeys(const std::string& key);
    std::vector<std::string> HVals(const std::string& key);
    int64_t HIncrBy(const std::string& key, const std::string& field, int64_t delta);
    int64_t HStrLen(const std::string& key, const std::string& field);

    // List commands
    int64_t LPush(const std::string& key, const std::vector<std::string>& values);
    int64_t RPush(const std::string& key, const std::vector<std::string>& values);
    std::optional<std::string> LPop(const std::string& key);
    std::optional<std::string> RPop(const std::string& key);
    std::vector<std::string> LRange(const std::string& key, int64_t start, int64_t stop);
    int64_t LLen(const std::string& key);
    std::optional<std::string> LIndex(const std::string& key, int64_t idx);
    bool LSet(const std::string& key, int64_t idx, const std::string& value);
    bool LTrim(const std::string& key, int64_t start, int64_t stop);
    int64_t LRem(const std::string& key, int64_t count, const std::string& value);

    // Set commands
    int64_t SAdd(const std::string& key, const std::vector<std::string>& members);
    int64_t SRem(const std::string& key, const std::vector<std::string>& members);
    std::vector<std::string> SMembers(const std::string& key);
    bool SIsMember(const std::string& key, const std::string& member);
    int64_t SCard(const std::string& key);
    std::optional<std::string> SPop(const std::string& key);
    std::optional<std::string> SRandMember(const std::string& key);
    bool SMove(const std::string& src, const std::string& dst, const std::string& member);

    // Bitmap commands
    int64_t SetBit(const std::string& key, int64_t offset, int64_t value);
    int64_t GetBit(const std::string& key, int64_t offset);
    int64_t BitCount(const std::string& key, int64_t start = 0, int64_t end = -1);
    int64_t BitPos(const std::string& key, int64_t bit, int64_t start = 0, int64_t end = -1);

    // HyperLogLog commands
    int64_t PfAdd(const std::string& key, const std::vector<std::string>& elements);
    int64_t PfCount(const std::vector<std::string>& keys);
    bool PfMerge(const std::string& dest, const std::vector<std::string>& sources);

    // ZSet commands
    int64_t ZAdd(const std::string& key, const std::vector<std::pair<double, std::string>>& members);
    int64_t ZRem(const std::string& key, const std::vector<std::string>& members);
    std::optional<std::string> ZScore(const std::string& key, const std::string& member);
    std::vector<std::string> ZRange(const std::string& key, int64_t start, int64_t stop, bool withscores = false);
    std::vector<std::pair<std::string, double>> ZRangeWithScores(const std::string& key, int64_t start, int64_t stop);
    int64_t ZCard(const std::string& key);
    int64_t ZCount(const std::string& key, const std::string& min, const std::string& max);
    std::vector<std::string> ZRangeByScore(const std::string& key, const std::string& min, const std::string& max,
                                           int64_t offset = 0, int64_t count = -1, bool withscores = false);
    std::vector<std::string> ZRevRange(const std::string& key, int64_t start, int64_t stop, bool withscores = false);

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
