# LightKV 示例代码

## 1. 最简示例：打开、写入、读取、关闭

```cpp
#include "lightkv/db.h"
#include <iostream>
#include <cassert>

int main() {
    // 1. 配置数据库
    lightkv::Options opts;
    opts.db_path = "./example_db";
    opts.create_if_missing = true;

    // 2. 打开数据库
    lightkv::DB* db = nullptr;
    lightkv::Status s = lightkv::DB::Open(opts, &db);
    if (!s.ok()) {
        std::cerr << "Open failed: " << s.ToString() << std::endl;
        return 1;
    }

    // 3. 写入数据
    s = db->Put(lightkv::WriteOptions(), "name", "LightKV");
    assert(s.ok());

    s = db->Put(lightkv::WriteOptions(), "version", "2.0");
    assert(s.ok());

    // 4. 读取数据
    std::string value;
    s = db->Get(lightkv::ReadOptions(), "name", &value);
    if (s.ok()) {
        std::cout << "name = " << value << std::endl;  // "LightKV"
    }

    s = db->Get(lightkv::ReadOptions(), "version", &value);
    if (s.ok()) {
        std::cout << "version = " << value << std::endl;  // "2.0"
    }

    // 5. 删除键
    s = db->Delete(lightkv::WriteOptions(), "version");
    assert(s.ok());

    // 6. 验证删除
    s = db->Get(lightkv::ReadOptions(), "version", &value);
    assert(s.IsNotFound());

    // 7. 关闭数据库
    delete db;
    return 0;
}
```

---

## 2. 二进制数据存储

Slice 支持任意字节序列，适合存储二进制数据（如序列化后的 Protobuf、图片缩略图等）。

```cpp
#include "lightkv/db.h"
#include <iostream>
#include <vector>
#include <cstring>

int main() {
    lightkv::Options opts;
    opts.db_path = "./binary_db";
    lightkv::DB* db = nullptr;
    lightkv::DB::Open(opts, &db);

    // 存储二进制数据（含 \0 的 key 和 value）
    const char binary_key[] = {'u', 's', 'e', 'r', '\0', '1', '2', '3'};
    const char binary_value[] = {'\x00', '\x01', '\x02', '\xFF', '\xFE'};

    lightkv::Slice key(binary_key, sizeof(binary_key));
    lightkv::Slice value(binary_value, sizeof(binary_value));

    db->Put(lightkv::WriteOptions(), key, value);

    // 读取二进制数据
    std::string result;
    lightkv::Status s = db->Get(lightkv::ReadOptions(), key, &result);
    if (s.ok()) {
        std::cout << "Read " << result.size() << " bytes" << std::endl;
        // 可以直接转为所需的二进制格式
        // 例如: YourProtoMsg msg; msg.ParseFromString(result);
    }

    delete db;
    return 0;
}
```

---

## 3. 覆盖写入与 MVCC 快照读

LightKV 使用 MVCC（多版本并发控制），重复写入同一 key 不会产生冲突。

```cpp
#include "lightkv/db.h"
#include <iostream>
#include <cassert>
#include <thread>

int main() {
    lightkv::Options opts;
    opts.db_path = "./mvcc_db";
    lightkv::DB* db = nullptr;
    lightkv::DB::Open(opts, &db);

    // 写入初始值
    db->Put(lightkv::WriteOptions(), "counter", "0");

    for (int i = 1; i <= 100; ++i) {
        // 每次写入新版本，旧版本仍保留在 MemTable/SSTable 中
        std::string v = std::to_string(i);
        db->Put(lightkv::WriteOptions(), "counter", v);
    }

    // 读取始终返回最新版本
    std::string value;
    db->Get(lightkv::ReadOptions(), "counter", &value);
    std::cout << "counter = " << value << std::endl;  // "100"
    assert(value == "100");

    delete db;
    return 0;
}
```

---

## 4. 同步写入（关键数据持久化）

对于不允许丢失的关键数据，使用 `WriteOptions::sync = true`。

```cpp
#include "lightkv/db.h"
#include <iostream>

int main() {
    lightkv::Options opts;
    opts.db_path = "./sync_db";
    lightkv::DB* db = nullptr;
    lightkv::DB::Open(opts, &db);

    lightkv::WriteOptions sync_opt;
    sync_opt.sync = true;  // 强制刷盘

    // 金融交易、账户余额等关键数据
    db->Put(sync_opt, "account:1001:balance", "999900");
    db->Put(sync_opt, "account:1002:balance", "500000");

    // 即使此时进程崩溃，重启后数据不丢失
    std::cout << "Critical data persisted." << std::endl;

    delete db;
    return 0;
}
```

---

## 5. 批量写入

循环写入大量数据是常见的场景。优化后的 LightKV 在批量写入场景下 QPS 可达 89 万+。

```cpp
#include "lightkv/db.h"
#include <iostream>
#include <chrono>

int main() {
    lightkv::Options opts;
    opts.db_path = "./batch_db";
    opts.memtable_size = 128 * 1024 * 1024;  // 128MB 减少 Flush
    lightkv::DB* db = nullptr;
    lightkv::DB::Open(opts, &db);

    const int N = 500000;
    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < N; ++i) {
        char key[32];
        snprintf(key, sizeof(key), "user:%08d", i);
        std::string value = "profile_data_" + std::to_string(i);
        db->Put(lightkv::WriteOptions(), key, value);
    }

    auto end = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double, std::milli>(end - start).count();
    double qps = N / (elapsed / 1000.0);

    std::cout << "Wrote " << N << " entries in " << elapsed << " ms" << std::endl;
    std::cout << "QPS: " << static_cast<long long>(qps) << std::endl;

    delete db;
    return 0;
}
```

---

## 6. 错误处理模式

```cpp
#include "lightkv/db.h"
#include <iostream>
#include <string>

void SafeGet(lightkv::DB* db, const std::string& key) {
    std::string value;
    lightkv::Status s = db->Get(lightkv::ReadOptions(), key, &value);

    if (s.ok()) {
        std::cout << "[" << key << "] = " << value << std::endl;
    } else if (s.IsNotFound()) {
        std::cout << "[" << key << "] not found (may be deleted)" << std::endl;
    } else if (s.IsCorruption()) {
        std::cerr << "[" << key << "] data corrupted: " << s.ToString() << std::endl;
    } else if (s.IsIOError()) {
        std::cerr << "[" << key << "] I/O error: " << s.ToString() << std::endl;
    } else {
        std::cerr << "[" << key << "] unknown error: " << s.ToString() << std::endl;
    }
}

int main() {
    lightkv::Options opts;
    opts.db_path = "./error_db";
    lightkv::DB* db = nullptr;
    lightkv::DB::Open(opts, &db);

    db->Put(lightkv::WriteOptions(), "existing_key", "hello");
    db->Delete(lightkv::WriteOptions(), "deleted_key");

    SafeGet(db, "existing_key");   // 正常命中
    SafeGet(db, "deleted_key");    // NotFound（已删除）
    SafeGet(db, "missing_key");    // NotFound（从未写入）

    delete db;
    return 0;
}
```

---

## 7. 并发读写（多线程）

```cpp
#include "lightkv/db.h"
#include <iostream>
#include <thread>
#include <vector>
#include <atomic>
#include <cassert>

int main() {
    lightkv::Options opts;
    opts.db_path = "./concurrent_db";
    lightkv::DB* db = nullptr;
    lightkv::DB::Open(opts, &db);

    const int num_writers = 4;
    const int num_readers = 2;
    const int ops_per_thread = 25000;

    std::atomic<bool> start_flag{false};

    // 写入线程
    std::vector<std::thread> writers;
    for (int t = 0; t < num_writers; ++t) {
        writers.emplace_back([&, t]() {
            while (!start_flag.load()) {}  // 等待起跑信号
            for (int i = 0; i < ops_per_thread; ++i) {
                char key[32];
                snprintf(key, sizeof(key), "t%d:%06d", t, i);
                db->Put(lightkv::WriteOptions(), key, "value");
            }
        });
    }

    // 读取线程
    std::vector<std::thread> readers;
    for (int t = 0; t < num_readers; ++t) {
        readers.emplace_back([&, t]() {
            while (!start_flag.load()) {}
            for (int i = 0; i < ops_per_thread; ++i) {
                std::string value;
                char key[32];
                snprintf(key, sizeof(key), "t%d:%06d", t % num_writers, i);
                db->Get(lightkv::ReadOptions(), key, &value);
            }
        });
    }

    start_flag.store(true);

    for (auto& w : writers) w.join();
    for (auto& r : readers) r.join();

    std::cout << "All threads completed." << std::endl;

    delete db;
    return 0;
}
```

---

## 8. 崩溃恢复演示

以下代码演示 LightKV 的持久性与崩溃恢复能力。

```cpp
// 第一次运行：写入数据
// > g++ -std=c++20 recover_demo.cpp -I./include -L./build_opt -llightkv -o recover_demo
// > ./recover_demo write
// 模拟崩溃（Ctrl+C 结束），然后再次运行：
// > ./recover_demo read

#include "lightkv/db.h"
#include <iostream>
#include <cstring>

int main(int argc, char* argv[]) {
    lightkv::Options opts;
    opts.db_path = "./recover_db";
    opts.create_if_missing = true;

    lightkv::DB* db = nullptr;
    lightkv::Status s = lightkv::DB::Open(opts, &db);
    if (!s.ok()) {
        std::cerr << "Open failed: " << s.ToString() << std::endl;
        return 1;
    }

    if (argc > 1 && strcmp(argv[1], "write") == 0) {
        // 写入模式：持续写入数据
        std::cout << "Writing data... (Press Ctrl+C to simulate crash)" << std::endl;
        for (int i = 0; ; ++i) {
            char key[32];
            snprintf(key, sizeof(key), "seq:%010d", i);
            db->Put(lightkv::WriteOptions(), key, "persisted_value");
            if (i % 10000 == 0) {
                std::cout << "  Written " << i << " entries" << std::endl;
            }
        }
    } else {
        // 读取模式：验证恢复后的数据
        std::cout << "Reading recovered data..." << std::endl;
        int found = 0;
        for (int i = 0; i < 100000; ++i) {
            char key[32];
            snprintf(key, sizeof(key), "seq:%010d", i);
            std::string value;
            if (db->Get(lightkv::ReadOptions(), key, &value).ok()) {
                ++found;
            }
        }
        std::cout << "Recovered " << found << " entries from WAL." << std::endl;
    }

    delete db;
    return 0;
}
```

---

## 9. 字符串键值对存储（类 Redis 用法）

```cpp
#include "lightkv/db.h"
#include <iostream>
#include <unordered_map>

class SimpleCache {
public:
    bool Open(const std::string& path) {
        lightkv::Options opts;
        opts.db_path = path;
        opts.memtable_size = 256 * 1024 * 1024;  // 256MB
        return lightkv::DB::Open(opts, &db_).ok();
    }

    void Set(const std::string& key, const std::string& value) {
        db_->Put(lightkv::WriteOptions(), key, value);
    }

    std::string Get(const std::string& key, const std::string& default_val = "") {
        std::string value;
        if (db_->Get(lightkv::ReadOptions(), key, &value).ok()) {
            return value;
        }
        return default_val;
    }

    void Del(const std::string& key) {
        db_->Delete(lightkv::WriteOptions(), key);
    }

    bool Exists(const std::string& key) {
        std::string dummy;
        return db_->Get(lightkv::ReadOptions(), key, &dummy).ok();
    }

    ~SimpleCache() { delete db_; }

private:
    lightkv::DB* db_ = nullptr;
};

int main() {
    SimpleCache cache;
    cache.Open("./cache_db");

    // 类 Redis 操作
    cache.Set("session:abc123", "user_id=42|expire=3600");
    cache.Set("config:theme", "dark");
    cache.Set("config:lang", "zh-CN");

    std::cout << "Session: " << cache.Get("session:abc123") << std::endl;
    std::cout << "Theme:   " << cache.Get("config:theme") << std::endl;
    std::cout << "Lang:    " << cache.Get("config:lang") << std::endl;

    if (!cache.Exists("config:font")) {
        std::cout << "config:font not set, using default" << std::endl;
    }

    return 0;
}
```

---

## 10. 结构化数据存取（序列化 + Slice）

结合 Protobuf / FlatBuffers / MessagePack 等序列化库存储复杂对象。

```cpp
#include "lightkv/db.h"
#include <iostream>
#include <cstring>
#include <cstdint>

// 模拟一个简单的二进制结构
#pragma pack(push, 1)
struct UserProfile {
    uint32_t user_id;
    char     name[32];
    uint32_t level;
    uint64_t last_login;
};
#pragma pack(pop)

int main() {
    lightkv::Options opts;
    opts.db_path = "./struct_db";
    lightkv::DB* db = nullptr;
    lightkv::DB::Open(opts, &db);

    // 写入结构化数据
    UserProfile profile = {};
    profile.user_id = 1001;
    strncpy(profile.name, "Alice", sizeof(profile.name) - 1);
    profile.level = 42;
    profile.last_login = 1715872000;

    char key[32];
    snprintf(key, sizeof(key), "user:%u", profile.user_id);
    lightkv::Slice value_slice(reinterpret_cast<const char*>(&profile), sizeof(profile));
    db->Put(lightkv::WriteOptions(), key, value_slice);

    // 读取并解析
    std::string raw;
    lightkv::Status s = db->Get(lightkv::ReadOptions(), key, &raw);
    if (s.ok() && raw.size() == sizeof(UserProfile)) {
        const UserProfile* loaded = reinterpret_cast<const UserProfile*>(raw.data());
        std::cout << "User ID:    " << loaded->user_id << std::endl;
        std::cout << "Name:       " << loaded->name << std::endl;
        std::cout << "Level:      " << loaded->level << std::endl;
        std::cout << "Last Login: " << loaded->last_login << std::endl;
    }

    delete db;
    return 0;
}
```

---

## 11. 计数器 / 序列号生成

利用 LightKV 的原子序列号 + Put 覆盖语义实现分布式计数器。

```cpp
#include "lightkv/db.h"
#include <iostream>
#include <thread>
#include <vector>
#include <atomic>

int main() {
    lightkv::Options opts;
    opts.db_path = "./counter_db";
    lightkv::DB* db = nullptr;
    lightkv::DB::Open(opts, &db);

    // 初始化计数器
    db->Put(lightkv::WriteOptions(), "order_id_seq", "100000");

    auto incr = [&](const std::string& counter_key, int times) {
        for (int i = 0; i < times; ++i) {
            std::string current;
            db->Get(lightkv::ReadOptions(), counter_key, &current);
            long long val = std::stoll(current) + 1;
            db->Put(lightkv::WriteOptions(), counter_key, std::to_string(val));
        }
    };

    // 多线程递增计数器（注意：非原子递增，仅做演示）
    std::vector<std::thread> threads;
    for (int t = 0; t < 4; ++t) {
        threads.emplace_back(incr, "order_id_seq", 1000);
    }
    for (auto& t : threads) t.join();

    std::string final_val;
    db->Get(lightkv::ReadOptions(), "order_id_seq", &final_val);
    std::cout << "Final counter value: " << final_val << std::endl;

    delete db;
    return 0;
}
```

---

## 12. TTL 模拟（基于时间戳过期）

LightKV 暂未内置 TTL 支持，可通过在 value 中嵌入过期时间戳来模拟。

```cpp
#include "lightkv/db.h"
#include <iostream>
#include <chrono>
#include <sstream>

std::string EncodeWithTTL(const std::string& value, int ttl_seconds) {
    auto now = std::chrono::system_clock::now();
    auto expire_time = now + std::chrono::seconds(ttl_seconds);
    auto expire_ts = std::chrono::duration_cast<std::chrono::seconds>(
        expire_time.time_since_epoch()).count();

    std::ostringstream oss;
    oss << expire_ts << "|" << value;  // 格式: "到期时间戳|实际值"
    return oss.str();
}

bool DecodeWithTTL(const std::string& encoded, std::string* value) {
    size_t sep = encoded.find('|');
    if (sep == std::string::npos) return false;

    long long expire_ts = std::stoll(encoded.substr(0, sep));
    auto now_ts = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    if (now_ts > expire_ts) return false;  // 已过期

    *value = encoded.substr(sep + 1);
    return true;
}

int main() {
    lightkv::Options opts;
    opts.db_path = "./ttl_db";
    lightkv::DB* db = nullptr;
    lightkv::DB::Open(opts, &db);

    // 写入带 TTL 的数据
    db->Put(lightkv::WriteOptions(), "session:token1",
            EncodeWithTTL("eyJhbGciOi...", 3600));  // 1 小时后过期

    // 读取时检查过期
    std::string raw;
    if (db->Get(lightkv::ReadOptions(), "session:token1", &raw).ok()) {
        std::string actual;
        if (DecodeWithTTL(raw, &actual)) {
            std::cout << "Token valid: " << actual << std::endl;
        } else {
            std::cout << "Token expired, cleaning up..." << std::endl;
            db->Delete(lightkv::WriteOptions(), "session:token1");
        }
    }

    delete db;
    return 0;
}
```

---

## 13. 配置服务模式

利用 LightKV 作为本地配置存储，支持热更新。

```cpp
#include "lightkv/db.h"
#include <iostream>
#include <string>
#include <unordered_map>
#include <functional>

class ConfigService {
public:
    using ChangeCallback = std::function<void(const std::string&, const std::string&)>;

    bool Init(const std::string& path) {
        lightkv::Options opts;
        opts.db_path = path;
        return lightkv::DB::Open(opts, &db_).ok();
    }

    void Set(const std::string& key, const std::string& value) {
        db_->Put(lightkv::WriteOptions(), key, value);
        NotifyChange(key, value);
    }

    std::string Get(const std::string& key, const std::string& fallback = "") {
        std::string value;
        if (db_->Get(lightkv::ReadOptions(), key, &value).ok()) {
            return value;
        }
        return fallback;
    }

    int GetInt(const std::string& key, int fallback = 0) {
        std::string v = Get(key);
        return v.empty() ? fallback : std::stoi(v);
    }

    bool GetBool(const std::string& key, bool fallback = false) {
        std::string v = Get(key);
        if (v == "true") return true;
        if (v == "false") return false;
        return fallback;
    }

    void Watch(const std::string& key, ChangeCallback cb) {
        watchers_[key] = std::move(cb);
    }

    ~ConfigService() { delete db_; }

private:
    lightkv::DB* db_ = nullptr;
    std::unordered_map<std::string, ChangeCallback> watchers_;

    void NotifyChange(const std::string& key, const std::string& value) {
        auto it = watchers_.find(key);
        if (it != watchers_.end()) {
            it->second(key, value);
        }
    }
};

int main() {
    ConfigService config;
    config.Init("./config_db");

    // 写入默认配置
    config.Set("server.port", "8080");
    config.Set("server.max_conn", "1000");
    config.Set("cache.enabled", "true");
    config.Set("cache.ttl_seconds", "300");

    // 监听配置变更
    config.Watch("server.port", [](const std::string& key, const std::string& val) {
        std::cout << "[Watcher] " << key << " changed to: " << val << std::endl;
    });

    // 更新配置（触发回调）
    config.Set("server.port", "9090");

    // 读取配置
    std::cout << "Port:     " << config.GetInt("server.port") << std::endl;
    std::cout << "Max Conn: " << config.GetInt("server.max_conn") << std::endl;
    std::cout << "Cache:    " << (config.GetBool("cache.enabled") ? "on" : "off") << std::endl;

    return 0;
}
```

---

## 14. 完整项目 CMakeLists.txt 集成

```cmake
cmake_minimum_required(VERSION 3.16)
project(MyKVApp VERSION 1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 引入 LightKV 子项目
add_subdirectory(lightkv)

# 你的应用程序
add_executable(my_app
    main.cpp
)

target_link_libraries(my_app PRIVATE lightkv::lightkv)

# Release 优化（可选）
if(CMAKE_BUILD_TYPE STREQUAL "Release")
    target_compile_options(my_app PRIVATE -O3 -march=native -DNDEBUG)
endif()
```

**目录结构**：

```
mykv_app/
├── CMakeLists.txt
├── main.cpp
└── lightkv/              ← LightKV 源码（git submodule 或拷贝）
    ├── CMakeLists.txt
    ├── include/
    │   └── lightkv/
    │       ├── db.h
    │       └── ...
    └── src/
        └── ...
```

**构建**：

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j$(nproc)
./build/my_app
```

---

## 15. 性能调优速查

```cpp
// 高吞吐写入配置
lightkv::Options high_write_opts;
high_write_opts.memtable_size = 256 * 1024 * 1024;  // 256MB MemTable
high_write_opts.block_cache_size = 256 * 1024 * 1024; // 256MB Cache
high_write_opts.bloom_bits_per_key = 10;

// 高并发读取配置
lightkv::Options high_read_opts;
high_read_opts.memtable_size = 64 * 1024 * 1024;    // 64MB（平衡）
high_read_opts.block_cache_size = 1024 * 1024 * 1024; // 1GB Cache
high_read_opts.bloom_bits_per_key = 14;              // 更低误报率

// 低延迟同步写入
lightkv::WriteOptions sync_opt;
sync_opt.sync = true;  // 写入后立即 fdatasync
```