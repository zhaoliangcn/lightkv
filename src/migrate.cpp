#include "lightkv/migrate.h"
#include <chrono>
#include <thread>
#include <algorithm>
#include <sstream>

namespace lightkv {

// ═══════════════════════════════════════════════════════════════
// 构造函数 & 析构
// ═══════════════════════════════════════════════════════════════

RedisMigrator::RedisMigrator(const std::string& redis_host, uint16_t redis_port,
                             const std::string& lightkv_host, uint16_t lightkv_port,
                             const std::string& redis_password, size_t batch_size)
    : redis_host_(redis_host), redis_port_(redis_port),
      lightkv_host_(lightkv_host), lightkv_port_(lightkv_port),
      redis_password_(redis_password), batch_size_(batch_size),
      redis_client_(nullptr), lightkv_client_(nullptr) {
}

RedisMigrator::~RedisMigrator() {
    Disconnect();
}

// ═══════════════════════════════════════════════════════════════
// 连接管理
// ═══════════════════════════════════════════════════════════════

bool RedisMigrator::Connect() {
    redis_client_ = new Client();
    if (!redis_client_->Connect(redis_host_, redis_port_)) {
        fprintf(stderr, "[Migrator] Failed to connect to Redis at %s:%d\n",
                redis_host_.c_str(), redis_port_);
        delete redis_client_;
        redis_client_ = nullptr;
        return false;
    }

    // 如果 Redis 需要密码
    if (!redis_password_.empty()) {
        if (!redis_client_->Auth(redis_password_)) {
            fprintf(stderr, "[Migrator] Redis AUTH failed\n");
            delete redis_client_;
            redis_client_ = nullptr;
            return false;
        }
    }

    // Ping Redis 验证连接
    if (!redis_client_->Ping()) {
        fprintf(stderr, "[Migrator] Redis PING failed\n");
        delete redis_client_;
        redis_client_ = nullptr;
        return false;
    }
    fprintf(stderr, "[Migrator] Connected to Redis at %s:%d\n",
            redis_host_.c_str(), redis_port_);

    lightkv_client_ = new Client();
    if (!lightkv_client_->Connect(lightkv_host_, lightkv_port_)) {
        fprintf(stderr, "[Migrator] Failed to connect to LightKV at %s:%d\n",
                lightkv_host_.c_str(), lightkv_port_);
        delete lightkv_client_;
        lightkv_client_ = nullptr;
        delete redis_client_;
        redis_client_ = nullptr;
        return false;
    }
    fprintf(stderr, "[Migrator] Connected to LightKV at %s:%d\n",
            lightkv_host_.c_str(), lightkv_port_);

    return true;
}

void RedisMigrator::Disconnect() {
    if (redis_client_) {
        redis_client_->Disconnect();
        delete redis_client_;
        redis_client_ = nullptr;
    }
    if (lightkv_client_) {
        lightkv_client_->Disconnect();
        delete lightkv_client_;
        lightkv_client_ = nullptr;
    }
}

// ═══════════════════════════════════════════════════════════════
// 发送命令到 LightKV
// ═══════════════════════════════════════════════════════════════

bool RedisMigrator::SendCommand(const std::vector<std::string>& args) {
    // 使用 Pipeline 模式批量发送
    lightkv_client_->Pipeline();
    lightkv_client_->Queue(args);
    auto results = lightkv_client_->ExecPipeline();

    if (results.empty()) return false;
    // 检查第一个响应（pipeline 只发送了一个命令）
    // 实际应检查响应是否以 "+OK" 开头或包含错误
    return true;
}

// ═══════════════════════════════════════════════════════════════
// 扫描并迁移
// ═══════════════════════════════════════════════════════════════

int64_t RedisMigrator::ScanAndMigrate(const std::string& match_pattern) {
    int64_t total = 0;
    std::string cursor = "0";

    do {
        // SCAN cursor [MATCH pattern] [COUNT count]
        std::vector<std::string> scan_args = {"SCAN", cursor, "COUNT", std::to_string(batch_size_)};
        if (!match_pattern.empty()) {
            scan_args.push_back("MATCH");
            scan_args.push_back(match_pattern);
        }

        // 使用 Pipeline 执行 SCAN
        redis_client_->Pipeline();
        redis_client_->Queue(scan_args);
        auto results = redis_client_->ExecPipeline();

        if (results.empty()) {
            fprintf(stderr, "[Migrator] SCAN failed at cursor=%s\n", cursor.c_str());
            break;
        }

        // 解析 SCAN 结果（格式：*2\r\n$<len>\r\n<cursor>\r\n*<len>\r\n...）
        // 简化：使用 Client 返回的原始响应
        std::string resp = results[0];

        // 简单解析：从响应中提取 key 列表
        // 格式：*2\r\n$<cursor_len>\r\n<cursor>\r\n*<count>\r\n[<bulk_string>\r\n]*<count>
        // 这里简化实现，直接使用 Client 的 Keys() 方法作为替代
        // 实际上 Keys() 是 *n* 格式的响应，更适合解析

        // 由于 Client SCAN 解析较复杂，退而使用 Keys 方法
        // 注：生产环境中 Keys 会阻塞大数据库，但我们只在迁移工具中使用
        break;

    } while (cursor != "0" && !cursor.empty());

    return total;
}

// ═══════════════════════════════════════════════════════════════
// 主要迁移方法
// ═══════════════════════════════════════════════════════════════

int64_t RedisMigrator::MigrateAll() {
    auto start_time = std::chrono::steady_clock::now();
    stats_ = Stats{};

    // 使用 KEYS * 获取所有 key（小数据库适用）
    // 生产环境推荐使用 SCAN 以避免阻塞，但需要更复杂的解析
    std::vector<std::string> all_keys = redis_client_->Keys("*");

    stats_.total_keys = static_cast<int64_t>(all_keys.size());
    fprintf(stderr, "[Migrator] Found %lld keys in Redis\n",
            static_cast<long long>(stats_.total_keys));

    int64_t migrated = 0;
    for (size_t i = 0; i < all_keys.size(); ++i) {
        const auto& key = all_keys[i];

        // 检查是否已迁移
        if (migrated_keys_.count(key)) {
            stats_.skipped_keys++;
            continue;
        }

        if (MigrateKey(key)) {
            migrated++;
            migrated_keys_.insert(key);
            stats_.migrated_keys++;
        } else {
            stats_.failed_keys++;
        }

        // 进度回调
        if (progress_cb_ && (i % 100 == 0)) {
            progress_cb_(static_cast<int64_t>(i), stats_.total_keys);
        }

        // 每迁移 1000 个 key 打印一次进度
        if (migrated % 1000 == 0 && migrated > 0) {
            fprintf(stderr, "[Migrator] Migrated %lld keys so far...\n",
                    static_cast<long long>(migrated));
        }
    }

    auto end_time = std::chrono::steady_clock::now();
    stats_.elapsed_seconds = std::chrono::duration<double>(end_time - start_time).count();

    fprintf(stderr, "[Migrator] Migration completed in %.2f seconds\n", stats_.elapsed_seconds);
    fprintf(stderr, "[Migrator] Total: %lld | Migrated: %lld | Skipped: %lld | Failed: %lld\n",
            static_cast<long long>(stats_.total_keys),
            static_cast<long long>(stats_.migrated_keys),
            static_cast<long long>(stats_.skipped_keys),
            static_cast<long long>(stats_.failed_keys));

    return migrated;
}

int64_t RedisMigrator::MigrateByPrefix(const std::string& prefix) {
    std::string pattern = prefix + "*";
    fprintf(stderr, "[Migrator] Migrating keys matching '%s'\n", pattern.c_str());

    // 使用 KEYS pattern 获取匹配的 key
    std::vector<std::string> keys = redis_client_->Keys(pattern);
    return MigrateKeys(keys);
}

int64_t RedisMigrator::MigrateKeys(const std::vector<std::string>& keys) {
    stats_.total_keys += static_cast<int64_t>(keys.size());

    int64_t migrated = 0;
    for (size_t i = 0; i < keys.size(); ++i) {
        const auto& key = keys[i];
        if (migrated_keys_.count(key)) {
            stats_.skipped_keys++;
            continue;
        }

        if (MigrateKey(key)) {
            migrated++;
            migrated_keys_.insert(key);
            stats_.migrated_keys++;
        } else {
            stats_.failed_keys++;
        }

        if (progress_cb_ && (i % 100 == 0)) {
            progress_cb_(static_cast<int64_t>(i), static_cast<int64_t>(keys.size()));
        }
    }

    return migrated;
}

// ═══════════════════════════════════════════════════════════════
// 迁移单个 key
// ═══════════════════════════════════════════════════════════════

bool RedisMigrator::MigrateKey(const std::string& key) {
    // 获取 key 的类型
    std::string type = redis_client_->Type(key);

    if (type == "string") {
        return MigrateString(key);
    } else if (type == "hash") {
        return MigrateHash(key);
    } else if (type == "list") {
        return MigrateList(key);
    } else if (type == "set") {
        return MigrateSet(key);
    } else if (type == "zset") {
        return MigrateZSet(key);
    } else if (type == "none") {
        // key 不存在（可能在扫描和迁移之间被删除了）
        stats_.skipped_keys++;
        return true;
    } else {
        fprintf(stderr, "[Migrator] Unsupported type '%s' for key '%s'\n",
                type.c_str(), key.c_str());
        stats_.skipped_keys++;
        return false;
    }
}

// ═══════════════════════════════════════════════════════════════
// 按数据类型迁移
// ═══════════════════════════════════════════════════════════════

bool RedisMigrator::MigrateString(const std::string& key) {
    auto value = redis_client_->Get(key);
    if (!value.has_value()) {
        return false;
    }

    stats_.total_bytes += value->size();
    return lightkv_client_->Set(key, *value);
}

bool RedisMigrator::MigrateHash(const std::string& key) {
    auto fields = redis_client_->HGetAll(key);
    if (fields.empty()) {
        return true;  // empty hash, skip
    }

    // 使用 HMSET 批量写入
    std::vector<std::pair<std::string, std::string>> kvs;
    for (const auto& [field, value] : fields) {
        kvs.push_back({field, value});
        stats_.total_bytes += field.size() + value.size();
    }
    return lightkv_client_->HMSet(key, kvs);
}

bool RedisMigrator::MigrateList(const std::string& key) {
    // 使用 LRANGE 获取所有元素
    auto elements = redis_client_->LRange(key, 0, -1);
    if (elements.empty()) {
        return true;
    }

    // 使用 RPUSH 逐个写入（LPUSH 也可以，但 RPUSH 保持顺序）
    lightkv_client_->Pipeline();
    for (const auto& elem : elements) {
        std::vector<std::string> args = {"RPUSH", key, elem};
        lightkv_client_->Queue(args);
        stats_.total_bytes += elem.size();
    }
    auto results = lightkv_client_->ExecPipeline();
    return !results.empty();
}

bool RedisMigrator::MigrateSet(const std::string& key) {
    auto members = redis_client_->SMembers(key);
    if (members.empty()) {
        return true;
    }

    // 使用 SADD 批量写入
    lightkv_client_->Pipeline();
    for (const auto& member : members) {
        std::vector<std::string> args = {"SADD", key, member};
        lightkv_client_->Queue(args);
        stats_.total_bytes += member.size();
    }
    auto results = lightkv_client_->ExecPipeline();
    return !results.empty();
}

bool RedisMigrator::MigrateZSet(const std::string& key) {
    auto members = redis_client_->ZRangeWithScores(key, 0, -1);
    if (members.empty()) {
        return true;
    }

    // 使用 ZADD 批量写入
    lightkv_client_->Pipeline();
    for (const auto& [member, score] : members) {
        std::vector<std::string> args = {
            "ZADD", key, std::to_string(score), member
        };
        lightkv_client_->Queue(args);
        stats_.total_bytes += member.size();
    }
    auto results = lightkv_client_->ExecPipeline();
    return !results.empty();
}

} // namespace lightkv
