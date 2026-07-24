#pragma once

#include "lightkv/client.h"
#include <string>
#include <vector>
#include <cstdint>
#include <set>
#include <functional>

namespace lightkv {

// ─── Redis → LightKV 数据迁移工具 ───
//
// 功能：
// 1. 连接 Redis 服务，使用 SCAN 命令遍历所有 key
// 2. 按数据类型（string/hash/list/set/zset）读取数据
// 3. 写入 LightKV 服务（RESP 协议）
// 4. 支持增量迁移和断点续传
//
// 用法：
//   RedisMigrator migrator("127.0.0.1", 6379, "127.0.0.1", 6380, "");
//   migrator.MigrateAll();  // 全量迁移
//
// 限制：
// - 只迁移数据，不迁移 TTL
// - 不迁移 Lua 脚本、Pub/Sub 订阅等非数据状态
// - 大 key 分批读取避免 OOM
class RedisMigrator {
public:
    // redis_host/redis_port: 源 Redis 服务地址
    // lightkv_host/lightkv_port: 目标 LightKV 服务地址
    // redis_password: Redis AUTH 密码（可选）
    // batch_size: SCAN 每次返回的 key 数量
    RedisMigrator(const std::string& redis_host, uint16_t redis_port,
                  const std::string& lightkv_host, uint16_t lightkv_port,
                  const std::string& redis_password = "",
                  size_t batch_size = 1000);

    ~RedisMigrator();

    RedisMigrator(const RedisMigrator&) = delete;
    RedisMigrator& operator=(const RedisMigrator&) = delete;

    // ─── 连接管理 ───
    bool Connect();
    void Disconnect();

    // ─── 迁移操作 ───
    // 全量迁移所有 key
    // 返回迁移的 key 数量
    int64_t MigrateAll();

    // 按前缀迁移
    // 只迁移匹配 prefix* 的 key（使用 SCAN MATCH）
    int64_t MigrateByPrefix(const std::string& prefix);

    // 迁移指定 key 列表
    int64_t MigrateKeys(const std::vector<std::string>& keys);

    // ─── 统计 ───
    struct Stats {
        int64_t total_keys = 0;
        int64_t migrated_keys = 0;
        int64_t skipped_keys = 0;
        int64_t failed_keys = 0;
        int64_t total_bytes = 0;
        double elapsed_seconds = 0.0;
    };
    Stats GetStats() const { return stats_; }

    // ─── 进度回调 ───
    using ProgressCallback = std::function<void(int64_t current, int64_t total)>;
    void SetProgressCallback(ProgressCallback cb) { progress_cb_ = cb; }

private:
    // 迁移单个 key
    bool MigrateKey(const std::string& key);

    // 获取 key 的类型并迁移
    bool MigrateString(const std::string& key);
    bool MigrateHash(const std::string& key);
    bool MigrateList(const std::string& key);
    bool MigrateSet(const std::string& key);
    bool MigrateZSet(const std::string& key);

    // 发送命令到 LightKV
    bool SendCommand(const std::vector<std::string>& args);

    // 扫描 Redis（使用 SCAN 命令）
    int64_t ScanAndMigrate(const std::string& match_pattern);

    std::string redis_host_;
    uint16_t redis_port_;
    std::string lightkv_host_;
    uint16_t lightkv_port_;
    std::string redis_password_;
    size_t batch_size_;

    // 源 Redis 和目标 LightKV 的客户端连接
    Client* redis_client_;
    Client* lightkv_client_;

    Stats stats_;
    ProgressCallback progress_cb_;

    // 已迁移的 key 集合（用于断点续传和去重）
    std::set<std::string> migrated_keys_;
};

} // namespace lightkv
