<div align="center">
  <h1>⚡ LightKV</h1>
  <p><em>高性能嵌入式 KV 数据库 · 兼容 Redis 协议 · 三语言 SDK</em></p>

  <p>
    <img src="https://img.shields.io/badge/C%2B%2B-20-blue?logo=c%2B%2B" alt="C++20">
    <img src="https://img.shields.io/badge/license-MIT-green" alt="MIT License">
    <img src="https://img.shields.io/badge/Go-SDK-00ADD8?logo=go" alt="Go SDK">
    <img src="https://img.shields.io/badge/Python-SDK-3776AB?logo=python" alt="Python SDK">
    <img src="https://img.shields.io/badge/Node.js-SDK-339933?logo=nodedotjs" alt="Node.js SDK">
  </p>

  <p>
    <a href="#-快速开始">快速开始</a> •
    <a href="#-特性">特性</a> •
    <a href="#-架构">架构</a> •
    <a href="#-构建">构建</a> •
    <a href="#-命令支持">命令支持</a> •
    <a href="#-多语言-sdk">SDK</a> •
    <a href="#-配置">配置</a> •
    <a href="#-性能">性能</a>
  </p>
</div>

---

## 📖 简介

**LightKV** 是一个**跨平台**、**现代化**、**高性能**的嵌入式 KV 数据库，采用 **LSM-Tree** 作为核心存储引擎，兼容 **Redis 协议**，支持完整的 Redis 数据类型与命令。提供 C++、Go、Python、Node.js 四语言 SDK，适合嵌入式存储、边缘计算、微服务本地缓存等场景。

### 设计目标

- **高性能**：LSM-Tree 引擎 + 无锁跳表 + 多级 Compaction，毫秒级读写
- **低依赖**：核心引擎零外部依赖（可选 LZ4 压缩），纯 C++20 实现
- **兼容性**：兼容 Redis 序列化协议（RESP），可通过 redis-cli 直接连接
- **可嵌入**：既可作为独立服务运行，也可作为 C++ 库嵌入应用

---

## ✨ 特性

### 存储引擎
- **LSM-Tree** 分层存储，支持 7 级 Leveled Compaction
- **无锁 SkipList** MemTable，支持高并发写入
- **WAL 预写日志**，Crash-safe 持久化
- **SSTable** 不可变数据文件，支持 Bloom Filter 加速查找
- **Block Cache** 多级缓存，Block-based 存储格式
- **LZ4 压缩**（可选），减少磁盘 I/O
- **前缀压缩** 重启点（Restart Interval）
- **Range Tombstone** 高效范围删除
- **MANIFEST** 元数据管理与恢复

### 网络服务
- **多线程 TCP 服务器** (epoll/kqueue/select)
- **Redis RESP 协议** 兼容，支持 redis-cli
- **HTTP 监控端点**，实时查看运行状态
- **认证机制** (AUTH 命令)
- **TTL 主动过期扫描**
- **主从复制** (Replication)
- **优雅停机** (信号处理)

### 数据类型与命令
| 类型 | 已支持命令 |
|------|-----------|
| String | SET, GET, GETSET, MGET, MSET, INCR, DECR, INCRBY, DECRBY, APPEND, STRLEN, GETRANGE, SETRANGE, SETEX, SETNX, GETDEL |
| Hash | HSET, HGET, HDEL, HEXISTS, HGETALL, HKEYS, HLEN, HMGET, HMSET, HSETNX, HVALS, HSTRLEN, HINCRBY, HSCAN |
| List | LPUSH, RPUSH, LPOP, RPOP, LLEN, LRANGE, LINDEX, LINSERT, LSET, LREM, LTRIM, LPUSHX, RPUSHX |
| Set | SADD, SREM, SMEMBERS, SISMEMBER, SCARD, SINTER, SUNION, SDIFF, SPOP, SRANDMEMBER, SMOVE |
| ZSet | ZADD, ZREM, ZSCORE, ZRANK, ZREVRANK, ZCARD, ZRANGE, ZREVRANGE, ZRANGEBYSCORE, ZCOUNT, ZINCRBY, ZRANK, ZREVRANK |
| Bitmap | SETBIT, GETBIT, BITCOUNT, BITPOS, BITOP |
| HyperLogLog | PFADD, PFCOUNT, PFMERGE |
| Geo | GEOADD, GEODIST, GEOHASH, GEOPOS, GEORADIUS, GEORADIUSBYMEMBER |
| 通用 | DEL, EXISTS, EXPIRE, TTL, TYPE, RENAME, SCAN, DBSIZE, KEYS, RANDOMKEY, PING, AUTH, SELECT, FLUSHDB, FLUSHALL, INFO |

> 更多命令持续添加中，详见 [TODO](./TODO)。

---

## 🏗 架构

```
┌──────────────────────────────────────────────────────────┐
│                     Client (API / Network)                │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐ │
│  │  C++ SDK  │  │  Go SDK  │  │Python SDK│  │ Node SDK │ │
│  └─────┬─────┘  └─────┬─────┘  └─────┬─────┘  └─────┬────┘ │
└────────┼───────────────┼───────────────┼───────────────┼────┘
         │               │               │               │
┌────────▼───────────────▼───────────────▼───────────────▼────┐
│                    TCP Server (RESP Protocol)                 │
│          multi-threaded │ epoll/kqueue │ AUTH │ TTL           │
└───────────────────────────────┬──────────────────────────────┘
                                │
┌───────────────────────────────▼──────────────────────────────┐
│                         DB Interface                          │
│     Put / Get / Delete / Scan / Increment / Snapshot          │
└───────────────────────────────┬──────────────────────────────┘
                                │
┌───────────────────────────────▼──────────────────────────────┐
│                   MemTable (Concurrent SkipList)               │
│                  ┌────────────────────────┐                   │
│                  │      WAL (Write-Ahead Log)                 │
│                  └────────────────────────┘                   │
└───────────────────────────────┬──────────────────────────────┘
         │ (immutable when full) │
┌────────▼───────────────────────▼──────────────────────────────┐
│                    Flush / Compaction Worker                    │
│              Multi-way Merge │ Leveled Compaction               │
└───────────────────────────────┬──────────────────────────────┘
                                │
┌───────────────────────────────▼──────────────────────────────┐
│               SSTable Manager (Level 0 ~ Level N)             │
│   Block Cache │ Bloom Filter │ Index Block │ LZ4 Compress     │
│   MANIFEST │ Range Tombstone │ Block CRC                      │
└──────────────────────────────────────────────────────────────┘
```

核心组件：
- **MemTable**：无锁跳表实现，O(log N) 读写，写满后转为 Immutable MemTable
- **WAL**：Crash-safe 预写日志，批量写入 + 异步刷盘
- **Compaction**：多路归并，按 key+seq 保留最新版本，分层管理
- **SSTable**：Block-based 存储，Bloom Filter 加速，支持 LZ4 压缩与 CRC 校验
- **Block Cache**：LRU 淘汰策略，缓存热点数据块
- **Server**：多线程事件循环，RESP 协议解析，支持认证与主从复制

---

## 🚀 快速开始

### 安装依赖

```bash
# Ubuntu / Debian
sudo apt install cmake g++ liblz4-dev   # LZ4 可选

# macOS
brew install cmake lz4                  # LZ4 可选
```

### 构建

```bash
git clone https://github.com/your-org/lightkv.git
cd lightkv
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
```

### 运行服务器

```bash
# 默认监听 0.0.0.0:6379，HTTP 监控 0.0.0.0:8080
./tools/server_main

# 指定端口
./tools/server_main --port 6380

# 启用认证
./tools/server_main --requirepass mysecret

# 多线程模式（4 个工作线程）
./tools/server_main --workers 4

# 只读从库模式
./tools/server_main --master-host 192.168.1.100 --master-port 6379
```

### 使用 redis-cli 连接

```bash
redis-cli -p 6379

127.0.0.1:6379> SET key hello
OK
127.0.0.1:6379> GET key
"hello"
127.0.0.1:6379> INCR counter
(integer) 1
127.0.0.1:6379> HSET user:1 name "Alice" age 30
(integer) 2
127.0.0.1:6379> ZADD leaderboard 100 "player1" 200 "player2"
(integer) 2
```

---

## 🔧 构建选项

| 选项 | 默认值 | 说明 |
|------|--------|------|
| `CMAKE_BUILD_TYPE` | `Release` | `Release` / `Debug` |
| `LZ4` | 自动检测 | 启用 LZ4 Block 压缩 |

```bash
# Debug 构建（含 AddressSanitizer）
cmake .. -DCMAKE_BUILD_TYPE=Debug

# Release 构建（O3 + 原生优化）
cmake .. -DCMAKE_BUILD_TYPE=Release
```

---

## 🎯 命令行参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--port` | 6379 | TCP 服务端口 |
| `--host` | 0.0.0.0 | TCP 绑定地址 |
| `--http-port` | 8080 | HTTP 监控端口 |
| `--db-path` | ./lightkv_data | 数据目录 |
| `--workers` | 0 | 工作线程数（0=单线程） |
| `--requirepass` | (空) | 认证密码 |
| `--master-host` | (空) | 主库地址（从库模式） |
| `--master-port` | 6379 | 主库端口 |
| `--readonly` | false | 只读模式 |
| `--max-clients` | 1024 | 最大客户端连接数 |

---

## 📚 多语言 SDK

LightKV 提供四语言客户端 SDK，位于 [`clients/`](./clients/) 目录：

### Go
```go
import "github.com/your-org/lightkv/clients/go"

client := lightkv.NewClient("127.0.0.1:6379")
defer client.Close()

val, err := client.Get("key")
client.Set("key", "value", 0)
```

### Python
```python
from lightkv import Client

client = Client("127.0.0.1", 6379)
client.set("key", "value")
print(client.get("key"))
```

### Node.js
```javascript
const { Client } = require('./clients/nodejs/src/client');
const client = new Client('127.0.0.1', 6379);
await client.set('key', 'value');
console.log(await client.get('key'));
```

> 详细 API 文档见 [`docs/Client-SDK-API-文档.md`](./docs/Client-SDK-API-文档.md)。

---

## ⚙️ 配置

LightKV 支持通过 JSON 配置文件或代码中的 `Options` 结构体进行配置：

```cpp
#include "lightkv/options.h"
#include "lightkv/config.h"

lightkv::Options opts;
opts.memtable_size = 64 * 1024 * 1024;    // 64 MB MemTable
opts.wal_file_size = 256 * 1024 * 1024;   // 256 MB WAL
opts.block_cache_size = 512 * 1024 * 1024; // 512 MB Cache
opts.block_size = 4 * 1024;                // 4 KB Block
opts.max_level = 7;                        // 7 Level Compaction
opts.bloom_bits_per_key = 10;              // Bloom Filter
opts.compression = lightkv::CompressionType::kLZ4Compression;
opts.db_path = "./lightkv_data";
```

```bash
# 或从 JSON 文件加载
./tools/server_main --config /path/to/config.json
```

---

## 📊 性能

> 以下为典型测试数据，实际性能受硬件配置和工作负载影响。

| 操作 | QPS（单线程） | QPS（多线程） | 延迟（P99） |
|------|-------------|-------------|-----------|
| SET (1KB) | 150,000+ | 500,000+ | < 1ms |
| GET (1KB) | 200,000+ | 800,000+ | < 0.5ms |
| 批量写入 | 100,000+ | 400,000+ | < 2ms |
| ZSet 操作 | 80,000+ | 300,000+ | < 2ms |

详见 [`docs/LightKV性能测试报告.md`](./docs/LightKV性能测试报告.md) 和 [`docs/LightKV性能优化报告.md`](./docs/LightKV性能优化报告.md)。

---

## 📂 项目结构

```
lightkv
├── include/lightkv/     # 公共头文件
│   ├── db.h             # DB 接口
│   ├── server.h         # Server 接口
│   ├── options.h        # 配置项
│   ├── config.h         # 配置文件加载
│   ├── slice.h          # 零拷贝切片
│   ├── status.h         # 状态码
│   ├── memtable.h       # 跳表 MemTable
│   ├── sstable.h        # SSTable
│   ├── wal.h            # 预写日志
│   ├── block.h          # Block 存储格式
│   ├── bloom_filter.h   # 布隆过滤器
│   ├── compaction.h     # Compaction
│   ├── cache.h          # Block Cache
│   ├── transaction.h    # 事务
│   ├── encoding.h       # 编码工具
│   ├── skiplist.h       # 无锁跳表
│   ├── db_impl.h        # DB 实现
│   ├── manifest.h       # MANIFEST
│   ├── client.h         # C++ 客户端
│   └── table_builder.h  # SSTable Builder
├── src/                 # 源代码
├── tools/               # 工具
│   └── server_main.cpp  # 服务器入口
├── tests/               # 测试
├── clients/             # 多语言 SDK
│   ├── go/
│   ├── python/
│   └── nodejs/
├── scripts/             # 脚本
├── docs/                # 文档
├── CMakeLists.txt
├── README.md
├── CHANGELOG.md
├── CONTRIBUTING.md
├── LICENSE
└── TODO
```

---

## 🧪 测试

```bash
# 运行所有测试（含回归 + 性能 + 压力）
./scripts/run_full_test.sh

# 仅运行单元测试
cd build && ctest

# 运行特定测试
./build/tests/db_test
./build/tests/server_test
```

测试覆盖：
- **P0 基础命令**：String / Hash / List / Set 核心操作
- **P1/P2 高级命令**：Bitmap / HyperLogLog / Geo / ZSet
- **ZSet 专项测试**：排行榜场景全链路验证
- **Geo 专项测试**：地理空间查询
- **认证测试**：AUTH 命令 + 密码验证
- **压力测试**：高并发随机读写，长时间稳定性
- **性能基准**：单命令、Pipeline、对比 Redis

---

## 🤝 贡献

欢迎贡献代码、提交 Issue 或改进文档！

1. Fork 本仓库
2. 创建特性分支 (`git checkout -b feature/amazing-feature`)
3. 提交更改 (`git commit -m 'Add amazing feature'`)
4. 推送到分支 (`git push origin feature/amazing-feature`)
5. 提交 Pull Request

详见 [CONTRIBUTING.md](./CONTRIBUTING.md)。

---

## 📄 许可

本项目基于 MIT 许可证开源 —— 详见 [LICENSE](./LICENSE)。

---

## 📚 文档目录

| 文档 | 说明 |
|------|------|
| [LightKV 详细设计文档](./docs/LightKV详细设计文档.md) | 引擎核心实现细节 |
| [LightKV API 接口文档](./docs/LightKV%20API接口文档.md) | C++ API 完整参考 |
| [Client SDK API 文档](./docs/Client-SDK-API-文档.md) | 多语言客户端 API |
| [LightKV 示例代码](./docs/LightKV示例代码.md) | 使用示例 |
| [性能测试报告](./docs/LightKV性能测试报告.md) | 性能基准数据 |
| [性能优化报告](./docs/LightKV性能优化报告.md) | 优化方案与效果 |
| [主从复制设计](./docs/主从复制设计.md) | 复制机制设计 |
| [生产环境部署设计](./docs/生产环境部署优化设计.md) | 部署运维方案 |
| [代码地图](./docs/代码地图.md) | 代码结构导航 |
| [支持 Redis 全量命令分析](./docs/LightKV支持Redis全量命令与数据类型的架构演进分析.md) | 演进路线图 |
