# LightKV API 接口文档

## 1. 命名空间

所有 API 均位于 `lightkv` 命名空间下。

```cpp
#include "lightkv/db.h"
#include "lightkv/options.h"
#include "lightkv/status.h"
#include "lightkv/slice.h"

using namespace lightkv;
```

---

## 2. 核心类

### 2.1 DB — 数据库抽象接口

[db.h](file:///Users/macmima1234/code/mykvdb/lightkv/include/lightkv/db.h)

纯虚类，定义 KV 数据库的核心操作。

#### 生命周期

```cpp
class DB {
    DB(const DB&) = delete;                 // 不可拷贝
    DB& operator=(const DB&) = delete;      // 不可赋值
    virtual ~DB();                          // 虚析构
};
```

DB 对象由 `DB::Open()` 工厂方法创建，调用方负责通过 `delete` 释放。

---

### 2.2 DB::Open — 打开/创建数据库

```cpp
static Status Open(const Options& options, DB** dbptr);
```

**参数**

| 参数 | 类型 | 说明 |
|------|------|------|
| `options` | `const Options&` | 数据库配置，见 [Options](#3-options--readoptions--writeoptions-配置结构体) |
| `dbptr` | `DB**` | 输出参数，指向创建的 DB 实例指针 |

**返回值**

| 状态 | 含义 |
|------|------|
| `Status::OK()` | 成功打开或创建数据库 |
| `Status::IOError(...)` | 目录创建失败、WAL 打开失败、SSTable 读取失败 |

**行为**：
1. 若 `options.db_path` 目录不存在：
   - `options.create_if_missing == true` → 自动创建目录和数据库
   - `options.create_if_missing == false` → 返回 `IOError`
2. 检测并恢复已有 WAL 日志（崩溃恢复）
3. 扫描已有 SSTable 文件，建立索引
4. 启动后台线程（Flush / Compaction）

**示例**：
```cpp
Options opts;
opts.db_path = "./mydb";
DB* db = nullptr;
Status s = DB::Open(opts, &db);
if (!s.ok()) {
    std::cerr << "Open failed: " << s.ToString() << std::endl;
    return;
}
// ... 使用 db ...
delete db;
```

---

### 2.3 DB::Put — 写入键值对

```cpp
virtual Status Put(const WriteOptions& options, const Slice& key, const Slice& value) = 0;
```

**参数**

| 参数 | 类型 | 说明 |
|------|------|------|
| `options` | `const WriteOptions&` | 写入选项 |
| `key` | `const Slice&` | 键（零拷贝，不要求 `\0` 结尾） |
| `value` | `const Slice&` | 值（零拷贝，可包含二进制数据） |

**返回值**

| 状态 | 含义 |
|------|------|
| `Status::OK()` | 写入成功 |

**语义**：
- 若 key 已存在，**覆盖**旧值
- 数据先写 WAL（持久化），再写 MemTable（内存索引）
- `options.sync == true` 时同步刷盘后才返回
- 写入的键值对立即对后续 Get 可见（MVCC 序列号保证）

**性能**：优化后小值写入 QPS 约 90 万（k8v100），平均延迟 0.9µs。

---

### 2.4 DB::Delete — 删除键

```cpp
virtual Status Delete(const WriteOptions& options, const Slice& key) = 0;
```

**参数**

| 参数 | 类型 | 说明 |
|------|------|------|
| `options` | `const WriteOptions&` | 写入选项 |
| `key` | `const Slice&` | 要删除的键 |

**返回值**

| 状态 | 含义 |
|------|------|
| `Status::OK()` | 删除标记写入成功 |

**注意**：
- 删除操作**不物理删除**数据，而是写入删除标记（逻辑删除）
- 被删除的 key 通过 `Get` 查询返回 `NotFound`
- 物理删除由 Compaction（合并压缩）在后台完成
- 即使 key 不存在，Delete 也返回 OK（幂等性）

---

### 2.5 DB::Get — 读取键值

```cpp
virtual Status Get(const ReadOptions& options, const Slice& key, std::string* value) = 0;
```

**参数**

| 参数 | 类型 | 说明 |
|------|------|------|
| `options` | `const ReadOptions&` | 读取选项 |
| `key` | `const Slice&` | 要查询的键 |
| `value` | `std::string*` | 输出参数，值写入此处。**仅在 OK 时有效** |

**返回值**

| 状态 | 含义 |
|------|------|
| `Status::OK()` | 找到，`*value` 包含结果 |
| `Status::NotFound(...)` | 键不存在（或已被删除） |
| `Status::Corruption(...)` | 数据损坏（paranoid_checks 开启时） |

**查找优先级**：

```
MemTable (活跃内存表)
  → Immutable MemTable (等待刷盘)
    → SSTable L0 (未排序, 可能重叠)
      → SSTable L1 (key 有序)
        → ...
          → SSTable L6
```

同一 key 可能有多个版本（不同 seq），返回最新且 `seq <= snapshot_seq` 的未删除版本。

**快照读**：
- `snapshot_seq == 0` → 读最新数据
- `snapshot_seq != 0` → 只读 `seq <= snapshot_seq` 的数据

---

## 3. Options / ReadOptions / WriteOptions 配置结构体

[options.h](file:///Users/macmima1234/code/mykvdb/lightkv/include/lightkv/options.h)

### 3.1 Options — 数据库全局配置

```cpp
struct Options {
    size_t memtable_size       = 64  * 1024 * 1024;  // 64MB
    size_t wal_file_size       = 256 * 1024 * 1024;  // 256MB
    size_t block_cache_size    = 512 * 1024 * 1024;  // 512MB
    size_t block_size          = 4   * 1024;         // 4KB
    size_t max_level           = 7;                  // LSM-Tree 最大层数
    size_t l0_file_num_trigger = 4;                  // L0 Compaction 触发阈值
    size_t level_multiplier    = 10;                 // 层间大小倍数
    size_t bloom_bits_per_key  = 10;                 // Bloom Filter 每 key 位数
    size_t restart_interval    = 16;                 // 前缀压缩重启点间隔
    bool   enable_compression  = false;              // 暂未实现
    std::string db_path        = "./lightkv_data";   // 数据库目录路径
    bool   create_if_missing   = true;               // 目录不存在时自动创建
    bool   paranoid_checks     = false;              // 开启严格校验
};
```

**调优建议**：

| 场景 | 建议调整 |
|------|---------|
| 写多读少 | 增大 `memtable_size`（128MB），减少 Flush 频率 |
| 读多写少 | 增大 `block_cache_size`（1GB），提升 Cache 命中率 |
| 大数据量 | 增大 `level_multiplier`（如 20），减少 Compaction 频率 |
| 低误报需求 | 增大 `bloom_bits_per_key`（如 14），误报率降至 ~0.1% |
| 碰撞 key 多 | 减小 `restart_interval`（如 8），提升块内查找速度 |

### 3.2 ReadOptions — 读取选项

```cpp
struct ReadOptions {
    bool verify_checksums = false;     // 暂未实现
    bool fill_cache       = true;      // 是否填充 Block Cache
    uint64_t snapshot_seq = 0;         // 快照序列号，0=最新
};
```

### 3.3 WriteOptions — 写入选项

```cpp
struct WriteOptions {
    bool sync = false;    // true=同步刷盘, false=异步（仅写 OS buffer）
};
```

| sync | 持久性 | 写入延迟 | 适用场景 |
|------|--------|---------|---------|
| `false` | 进程崩溃可能丢失最近数据 | ~1µs | 高性能缓存、非关键数据 |
| `true` | 写入后立即 fdatasync | ~100µs+ | 金融、账户等关键数据 |

---

## 4. Status — 操作状态

[status.h](file:///Users/macmima1234/code/mykvdb/lightkv/include/lightkv/status.h)

```cpp
class Status {
public:
    // 状态码
    enum Code {
        kOk             = 0,   // 成功
        kNotFound       = 1,   // 键不存在
        kCorruption     = 2,   // 数据损坏
        kNotSupported   = 3,   // 不支持的操作
        kInvalidArgument = 4,  // 非法参数
        kIOError        = 5,   // I/O 错误
    };

    // 工厂方法
    static Status OK();
    static Status NotFound(std::string_view msg = "");
    static Status Corruption(std::string_view msg = "");
    static Status NotSupported(std::string_view msg = "");
    static Status InvalidArgument(std::string_view msg = "");
    static Status IOError(std::string_view msg = "");

    // 查询方法
    bool ok() const;              // 是否成功
    bool IsNotFound() const;      // 是否 NotFound
    bool IsCorruption() const;    // 是否数据损坏
    bool IsIOError() const;       // 是否 I/O 错误

    Code code() const;            // 获取状态码
    std::string_view message() const; // 错误消息
    std::string ToString() const; // 状态码+消息的字符串表示
};
```

**典型用法**：

```cpp
Status s = db->Put(WriteOptions(), "key1", "value1");
if (!s.ok()) {
    std::cerr << "Put error: " << s.ToString() << std::endl;
}

s = db->Get(ReadOptions(), "key1", &value);
if (s.IsNotFound()) {
    std::cout << "key1 not found" << std::endl;
} else if (!s.ok()) {
    std::cerr << "Get error: " << s.ToString() << std::endl;
}
```

---

## 5. Slice — 零拷贝字节视图

[slice.h](file:///Users/macmima1234/code/mykvdb/lightkv/include/lightkv/slice.h)

```cpp
class Slice {
public:
    // 构造
    Slice();                             // 空切片
    Slice(const char* data, size_t size);// 指针+长度（可含二进制 \0）
    Slice(const char* str);              // C 字符串（自动 strlen）
    Slice(const std::string& str);       // std::string（零拷贝）
    Slice(std::string_view sv);          // string_view（零拷贝）

    // 属性
    const char* data() const;            // 字节指针
    size_t size() const;                 // 字节长度
    bool empty() const;                  // 是否为空
    const char* begin() const;           // 首字节指针
    const char* end() const;             // 尾后指针
    char operator[](size_t i) const;     // 按索引取字节

    // 转换（仅在需要时拷贝）
    std::string ToString() const;        // 拷贝为 std::string
    std::string_view ToStringView() const;// 转为 string_view

    // 比较
    bool operator==(const Slice& other) const;
    bool operator!=(const Slice& other) const;
    bool operator<(const Slice& other) const;
    bool operator>(const Slice& other) const;
    int compare(const Slice& other) const; // 字典序（-1/0/1）

    // 操作
    void remove_prefix(size_t n);        // 移除前 n 字节（O(1)，不拷贝）
    uint32_t Hash() const;               // FNV-1a 哈希

    // 拼接运算符（产生新的 std::string）
    friend std::string operator+(const Slice& a, const Slice& b);
    friend std::string operator+(const std::string& a, const Slice& b);
    friend std::string operator+(const Slice& a, const std::string& b);
};
```

**设计要点**：Slice 不拥有内存，仅持有外部指针。传入 API 后不要释放原内存，直到 API 返回。

---

## 6. 数据模型

### 6.1 Key 规范

- Key 为任意字节序列（**不要求 C 字符串**，可包含 `\0`）
- Key 最大长度受 MemTable（64MB）约束，建议不超过 64KB
- Key 比较为**字典序**（memcmp）

### 6.2 Value 规范

- Value 为任意字节序列（二进制安全）
- Value 长度无硬限制，受 MemTable 大小（64MB）约束
- Value 为空（长度 0）是合法的

### 6.3 并发语义

| 操作 | 线程安全 | 说明 |
|------|---------|------|
| `Put` | 是 | 多线程可并发调用，内部 mutex 保护 |
| `Delete` | 是 | 同上 |
| `Get` | 是 | 读写互斥，但与其他 Read 可并发（简化为 mutex） |
| `Open` | 否 | 应在主线程调用，完成后才传给其他线程 |

---

## 7. 错误码速查

| Code | 含义 | 触发条件 |
|------|------|---------|
| `kOk` (0) | 操作成功 | — |
| `kNotFound` (1) | 键不存在 | Get 未命中，或命中已删除的 key |
| `kCorruption` (2) | 数据损坏 | CRC 校验失败（paranoid_checks 模式）、文件格式错误 |
| `kNotSupported` (3) | 不支持的操作 | 预留 |
| `kInvalidArgument` (4) | 无效参数 | 预留 |
| `kIOError` (5) | I/O 错误 | 文件打开失败、读写失败、mmap 失败 |

---

## 8. 头文件依赖关系

```
                     ┌─────────────┐
                     │   db.h      │  ← 主接口
                     └──┬───┬───┬──┘
                        │   │   │
          ┌─────────────┘   │   └─────────────┐
          ▼                 ▼                 ▼
   ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
   │  options.h  │  │  status.h   │  │  slice.h    │
   └─────────────┘  └─────────────┘  └─────────────┘

应用只需包含 db.h 即可间接获得所有依赖的类型定义。
```

**最小化包含**：

```cpp
#include "lightkv/db.h"      // DB + Options + ReadOptions + WriteOptions + Status + Slice
// 首次使用前链接: liblightkv.a
```

### CMake 集成

```cmake
# 项目 CMakeLists.txt
add_subdirectory(lightkv)
target_link_libraries(your_app lightkv::lightkv)
```

### 编译命令

```bash
# Debug (带 ASan)
cmake -B build_debug -DCMAKE_BUILD_TYPE=Debug
cmake --build build_debug

# Release (优化)
cmake -B build_opt -DCMAKE_BUILD_TYPE=Release
cmake --build build_opt
```