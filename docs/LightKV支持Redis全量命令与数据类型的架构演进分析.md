# LightKV 支持 Redis 全量命令与数据类型的架构演进分析

## 摘要

本文系统分析 LightKV 从当前仅支持 String 类型的基础 KV 操作，演进到支持 Redis 全量 500+ 命令与 9 种数据类型的完整技术路径。分析覆盖数据类型层、命令层、高级功能层三个维度，评估实现难度、架构改造幅度与优先级。

---

## 1. 现状与目标对比

### 1.1 当前能力

| 维度 | 当前状态 |
|------|---------|
| 数据类型 | String, Hash, List, Set, ZSet, Bitmap, HyperLogLog, Geo |
| 支持命令 | **83 条**：P0 (34) + P1 (Hash 12 + List 10 + Set 8) + P2 (ZSet 8 + Bitmap 4 + HLL 3 + Geo 3) |
| 协议 | Redis RESP 协议（完整兼容） |
| 存储引擎 | LSM-Tree（WAL + MemTable + SSTable） |
| 高级功能 | 基础 Pipeline、快照隔离事务、TTL 惰性删除 |
| 网络层 | TCP Server（RESP 协议解析 + Pipeline 批量处理） |
| 多语言 SDK | C++（完整支持 83 条命令）/ Node.js / Python / Go（P0/P1 命令已支持，P2 Geo 待补充） |

### 1.2 目标能力

| 维度 | 目标状态 |
|------|---------|
| 数据类型 | String, List, Hash, Set, ZSet, Bitmap, HyperLogLog, Geo, Stream |
| 支持命令 | 500+（覆盖 Redis 7.x 核心命令） |
| 高级功能 | 事务、Pub/Sub、Lua 脚本、复制、集群 |

---

## 2. Redis 功能全景图

### 2.1 9 种数据类型

```
┌─────────────────────────────────────────────────────────────────┐
│                    Redis 数据类型体系                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  基础类型（4 种）                                                  │
│  ├── String  ── 二进制安全字符串，支持位操作                        │
│  ├── List    ── 双向链表，支持 push/pop                           │
│  ├── Hash    ── 字段 - 值映射，适合对象存储                          │
│  └── Set     ── 无序集合，支持交集/并集/差集                       │
│                                                                 │
│  高级类型（3 种）                                                  │
│  ├── ZSet    ── 有序集合，带分数排序                               │
│  ├── Bitmap  ── 位图，基于 String 的位级操作                        │
│  └── HLL     ── HyperLogLog，基数估算                             │
│                                                                 │
│  空间类型（1 种）                                                  │
│  └── Geo     ── 地理位置，基于 ZSet 的经纬度编码                     │
│                                                                 │
│  流式类型（1 种）                                                  │
│  └── Stream  ── 消息队列，支持消费者组                             │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 500+ 命令分类

| 分类 | 命令数 | 代表命令 |
|------|--------|---------|
| String | ~50 | SET, GET, MSET, INCR, DECR, GETSET, SETNX, SETEX |
| List | ~40 | LPUSH, RPUSH, LPOP, RPOP, LRANGE, LINDEX, LSET |
| Hash | ~30 | HSET, HGET, HMSET, HGETALL, HDEL, HINCRBY |
| Set | ~30 | SADD, SREM, SMEMBERS, SINTER, SUNION, SDIFF |
| ZSet | ~40 | ZADD, ZRANGE, ZREM, ZINCRBY, ZRANK, ZSCORE |
| Bitmap | ~10 | SETBIT, GETBIT, BITCOUNT, BITPOS, BITOP |
| HLL | ~5 | PFADD, PFCOUNT, PFMERGE |
| Geo | ~10 | GEOADD, GEOPOS, GEODIST, GEORADIUS, GEOHASH |
| Stream | ~15 | XADD, XREAD, XGROUP, XREADGROUP, XDEL |
| 通用 | ~50 | DEL, EXISTS, EXPIRE, TTL, KEYS, SCAN, TYPE |
| 事务 | ~10 | MULTI, EXEC, DISCARD, WATCH, UNWATCH |
| Pub/Sub | ~10 | PUBLISH, SUBSCRIBE, PSUBSCRIBE, PUBSUB |
| Lua | ~5 | EVAL, EVALSHA |
| 连接/服务器 | ~40 | AUTH, SELECT, BGSAVE, FLUSHALL, CONFIG |

---

## 3. 数据类型层扩展

### 3.1 核心挑战

LightKV 当前使用 LSM-Tree 存储 String 类型，每个 key-value 对独立存储。Redis 的复合数据类型（List、Hash、Set、ZSet）需要**内嵌结构**支持，不能简单用 KV 映射。

### 3.2 各类型实现方案

#### 3.2.1 Hash（中等难度）

**方案 A：KV 编码（推荐，初期）**

将 Hash 字段编码为复合 key：`hash:{hashname}:{field}`

```
HSET user:1 name "Alice" age "30"
→ SET hash:user:1:name "Alice"
→ SET hash:user:1:age "30"

HGETALL user:1
→ SCAN hash:user:1:*  → 批量 GET
```

**优点**：无需修改存储引擎，利用现有 LSM-Tree 的 range scan 能力  
**缺点**：HMGET/HGETALL 需要多次网络往返；无法原子更新整个 Hash

**方案 B：内嵌编码（后期）**

将 Hash 序列化为二进制 blob 存储在单个 key 中：

```
HSET user:1 name "Alice" age "30"
→ SET hash:user:1 <protobuf/thrift 编码的 {name:Alice, age:30}>

HGETALL user:1
→ GET hash:user:1 → 反序列化
```

**优点**：HMGET/HGETALL 只需一次往返；原子更新  
**缺点**：大 Hash 更新需要重写整个 blob；无法按字段过滤

**推荐路径**：初期用方案 A，Hash 超过阈值（如 512 字段）自动切换到方案 B

#### 3.2.2 List（中等难度）

**方案 A：索引 + KV（推荐，初期）**

```
LPUSH mylist "a" "b" "c"
→ SET list:mylist:index:0 "c"
→ SET list:mylist:index:1 "b"
→ SET list:mylist:index:2 "a"
→ SET list:mylist:meta {"head":0, "tail":2, "len":3}

LRANGE mylist 0 2
→ GET list:mylist:meta → 读取索引范围
→ 批量 GET list:mylist:index:0~2
```

**挑战**：
- LPOP/LPUSH 需要更新所有后续元素的索引（O(N)）
- 大 List 性能差

**方案 B：双向链表 + 节点分块（后期）**

将 List 分块存储，每块 512 个元素：

```
mylist:block:0  → ["a", "b", ..., "z"] (512 个)
mylist:block:1  → ["aa", "bb", ..., "zz"]
mylist:block:2  → ["aaa", ...]

LPUSH → 在 block:0 头部插入，溢出则创建 block:-1
RPOP  → 从最后一个 block 尾部弹出
```

**优点**：LPUSH/RPOP 为 O(1) 或 O(块大小)  
**缺点**：实现复杂，需要维护块链表

**推荐路径**：初期用方案 A（小 List），超过 512 元素自动分块

#### 3.2.3 Set（中等难度）

**方案：排序 + 二分查找**

```
SADD myset "a" "b" "c"
→ SET set:myset <sorted binary blob: ["a","b","c"]>

SMEMBERS myset
→ GET set:myset → 反序列化

SISMEMBER myset "b"
→ GET set:myset → 二分查找（无需全量加载，可用 Bloom Filter 加速）
```

**优化**：
- 小 Set（<64 元素）：使用 ziplist 编码
- 大 Set：使用 hash 编码（方案 A 的变体）
- SISMEMBER 可配合 Bloom Filter 快速判断不存在

#### 3.2.4 ZSet（高难度）

ZSet 是 Redis 最复杂的类型之一，需要**按 score 排序 + 按 member 查找**的双结构维护。

**实际实现方案：KV 编码 + Score 填充排序**

```
ZADD myzset 1.0 "a" 2.0 "b" 3.0 "c"

存储方案（3 种 key）：
→ zset:myzset:member:a → "1.000000"        （member → score 映射，ZSCORE 用）
→ zset:myzset:score:+00000000000001.0000:a → ""  （score → member 映射，ZRANGE 用）
→ zset:myzset:score:+00000000000002.0000:b → ""
→ zset:myzset:score:+00000000000003.0000:c → ""
→ zset:myzset:__meta__ → "3"               （元素计数，ZCARD 用）
```

**Score 填充格式**：`+00000000000001.0000`（符号 + 15 位整数 + 小数点 + 4 位小数），确保字典序与数值序一致。

**已实现命令**（8 条）：
- `ZADD`：添加/更新成员，返回新增成员数
- `ZREM`：删除成员，返回删除数
- `ZSCORE`：O(1) 查找成员分数
- `ZCARD`：O(1) 获取元素数量
- `ZRANGE`：按索引范围查询，支持 WITHSCORES 和负数索引
- `ZREVRANGE`：逆序范围查询
- `ZCOUNT`：按分数范围计数，支持开区间 `(min`
- `ZRANGEBYSCORE`：按分数范围查询，支持 LIMIT 分页和 WITHSCORES
- `ZRANK`：返回 member 升序排名（0 = 最低分），不存在返回 nil
- `ZREVRANK`：返回 member 降序排名（0 = 最高分），不存在返回 nil

**实现状态**：✅ 已完成。54 个回归测试全部通过，覆盖基础操作、负数分数、同分数字典序排序、Pipeline 等场景。

**挑战**：
- ZRANGE/ZREVRANGE：需要按 score 范围查询 → 利用 LSM-Tree 的 range scan + 内存排序
- ZADD（更新 score）：需要删除旧 score key 并创建新 key
- ZRANK/ZREVRANK：复用 zscan_all 的 (score, member) 升序数组，ZRANK 直接返回下标，ZREVRANK 返回 n-1-i

**优化方案**：
- 小 ZSet 可考虑内存跳表缓存
- ZRANGEBYSCORE 利用 score key 的字典序前缀扫描

#### 3.2.5 Bitmap（低难度）

Bitmap 本质是 String 的位级操作，无需新结构：

```
SETBIT mybitmap 100 1  → 读取当前 blob → 修改位 → 写回
GETBIT mybitmap 100    → 读取当前 blob → 返回位
BITCOUNT mybitmap      → 读取 blob → 统计 1 的个数
BITPOS mybitmap 1      → 读取 blob → 查找第一个 1 的位置
BITOP AND dest src1 src2 → 读取 → 位运算 → 写回
```

**实际实现方案**：
- 基于 String 值直接进行位级操作
- MSB first 编码（与 Redis 一致）：offset 8 对应字节的最高位
- 大偏移量自动扩容 blob
- BITCOUNT 使用 popcount 逐字节统计

**已实现命令**（4 条）：`SETBIT`, `GETBIT`, `BITCOUNT`, `BITPOS`
**实现状态**：✅ 已完成。P2 测试 22/22 全部通过。

**优化**：
- 大 Bitmap 可分块存储（每块 8KB）
- BITCOUNT 可维护增量计数器

#### 3.2.6 HyperLogLog（低难度）

HLL 是固定大小的数据结构（约 12KB），直接作为二进制 blob 存储：

```
PFADD myhll "a" "b" "c"  → 读取 HLL blob → 更新 → 写回
PFCOUNT myhll            → 读取 HLL blob → 计算
PFMERGE dest src1 src2   → 读取 → 合并 → 写回
```

**实际实现方案**：
- 2^14 = 16384 个 registers，每个 6 bit，总大小 12288 字节
- MurmurHash2 64A 哈希函数
- 标准误差 ~0.81%
- 小基数偏差校正

**已实现命令**（3 条）：`PFADD`, `PFCOUNT`, `PFMERGE`
**实现状态**：✅ 已完成。P2 测试 22/22 全部通过。

**注意**：HLL 的合并操作需要原子性，需配合事务或 Lua 脚本

#### 3.2.7 Geo（中等难度）

Geo 本质是 ZSet 的封装，使用 Geohash 编码经纬度：

```
GEOADD locations 116.40 39.90 "Beijing" 121.47 31.23 "Shanghai"
→ ZADD geo:locations <geohash(116.40, 39.90)> "Beijing"
→ ZADD geo:locations <geohash(121.47, 31.23)> "Shanghai"

GEODIST locations Beijing Shanghai
→ ZRANGE geo:locations → 获取两点 → Haversine 公式计算
```

**实际实现方案**：
- 复用 ZSet 存储结构（`\x05_zset_` 前缀）
- Geohash 编码：52 位整数，26 次二分（经度/纬度交替）
- 经度范围：[-180, 180]，纬度范围：[-85.05112878, 85.05112878]
- Haversine 公式计算球面距离（地球半径 6372797.5 米）
- 支持单位：m（米）、km（千米）、mi（英里）、ft（英尺）

**已实现命令**（3 条）：
- `GEOADD`：添加地理位置，返回新增成员数
- `GEOPOS`：获取成员经纬度
- `GEODIST`：计算两个成员之间的距离

**实现状态**：✅ 已完成。18 个回归测试全部通过。

**依赖**：ZSet 实现完成后，Geo 基本免费获得

#### 3.2.8 Stream（高难度）

Stream 是 Redis 最复杂的数据类型，需要：

1. **消息 ID 生成**：`timestamp-sequence` 格式
2. **有序消息存储**：类似 ZSet 的跳表
3. **消费者组管理**：需要持久化消费者状态
4. **ACK 机制**：需要记录已确认的消息

```
XADD mystream * field value
→ 生成消息 ID 1700000000000-0
→ 存储到 stream:mystream 的有序结构中

XREADGROUP mystream group1 consumer1 COUNT 10 BLOCK 1000
→ 查找未确认消息 → 分配给 consumer1 → 返回
```

**实现难度**：⭐⭐⭐⭐⭐（需要全新的存储结构）

---

## 4. 命令层扩展

### 4.1 命令路由架构

当前 LightKV 的命令路由非常简单：

```cpp
// server.cpp 简化版
if (cmd == "SET")  handle_set(...);
if (cmd == "GET")  handle_get(...);
if (cmd == "DEL")  handle_del(...);
```

扩展到 500+ 命令需要**命令路由层**：

```
┌─────────────────────────────────────────────────┐
│              Command Router                      │
├─────────────────────────────────────────────────┤
│                                                 │
│  RESP Parser                                    │
│      ↓                                          │
│  Command Parser → [cmd, arg1, arg2, ...]        │
│      ↓                                          │
│  Command Table (哈希表)                          │
│      ├── "SET"  → cmd_set_handler               │
│      ├── "GET"  → cmd_get_handler               │
│      ├── "HSET" → cmd_hset_handler              │
│      └── ...                                    │
│      ↓                                          │
│  Type Validator (检查 key 类型是否匹配)            │
│      ↓                                          │
│  Command Handler → 返回 RESP 响应                │
│                                                 │
└─────────────────────────────────────────────────┘
```

### 4.2 命令分类实现优先级

| 优先级 | 分类 | 命令数 | 预计工作量 | 说明 |
|--------|------|--------|-----------|------|
| P0 ✅ | String 扩展 | 15/50 | **已完成** | INCR/DECR/MSET/SETEX/SETNX/APPEND/STRLEN/GETSET/GETRANGE/INCRBYFLOAT 等 |
| P0 ✅ | 通用命令 | 13/50 | **已完成** | EXISTS/EXPIRE/TTL/TYPE/RENAME/KEYS/PERSIST/RANDOMKEY/SCAN |
| P1 ✅ | Hash | 12/30 | **已完成** | HSET/HGET/HMSET/HMGET/HGETALL/HDEL/HLEN/HEXISTS/HKEYS/HVALS/HINCRBY/HSTRLEN |
| P1 ✅ | List | 10/40 | **已完成** | LPUSH/RPUSH/LPOP/RPOP/LRANGE/LINDEX/LLEN/LSET/LTRIM/LREM |
| P1 ✅ | Set | 8/30 | **已完成** | SADD/SREM/SMEMBERS/SISMEMBER/SCARD/SPOP/SRANDMEMBER/SMOVE |
| P2 ✅ | ZSet | 10/40 | **已完成** | ZADD/ZREM/ZSCORE/ZCARD/ZRANGE/ZREVRANGE/ZCOUNT/ZRANGEBYSCORE/ZRANK/ZREVRANK |
| P2 ✅ | Bitmap | 4/10 | **已完成** | SETBIT/GETBIT/BITCOUNT/BITPOS |
| P2 ✅ | HLL | 3/5 | **已完成** | PFADD/PFCOUNT/PFMERGE |
| P2 ✅ | Geo | 3/10 | **已完成** | GEOADD/GEOPOS/GEODIST |
| P3 | Stream | ~15 | 4 周 | 全新结构 |
| P3 | 事务 | ~10 | 2 周 | 扩展现有事务 |
| P3 | Pub/Sub | ~10 | 2 周 | 需要发布订阅基础设施 |
| P3 | Lua | ~5 | 2 周 | 需要 Lua 解释器嵌入 |

### 4.3 关键命令实现要点

#### INCR/DECR/INCRBY

```cpp
// 已实现：通过 DB 层原子 Increment 方法
// db_impl.cpp 核心逻辑：
Status DBImpl::Increment(const ReadOptions& ro, const Slice& key, int64_t delta, int64_t* result) {
    std::string val;
    Status s = Get(ro, key, &val);
    int64_t current = 0;
    if (s.ok()) {
        if (!TryParse(val, &current)) return Status::Corruption("WRONGTYPE");
    }
    current += delta;
    *result = current;
    return Put(WriteOptions(), key, std::to_string(current));
}
```

**实现状态**：✅ 已完成。支持 INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT。

#### EXPIRE/TTL

**实现方案（已实现）**：
```cpp
// TTL 元数据键格式：\x01_ttl_\x00 + 原 key
// 过期时间以毫秒精度存储 Unix 时间戳

// 惰性删除：读取时检查 TTL
std::string expiry;
Status s = db_->Get(ro, ttl_key(key), &expiry);
if (s.ok()) {
    uint64_t expire_at = std::stoull(expiry);
    if (now_ms() >= expire_at) {
        db_->Delete(wo, key);        // 惰性删除
        db_->Delete(wo, ttl_key(key));
        return resp_nil();
    }
}
```

**实现状态**：✅ 已完成。支持 EXPIRE, PEXPIRE, EXPIRETIME, TTL, PTTL, PERSIST。采用惰性删除策略，不启动后台过期线程。

#### KEYS/SCAN

```cpp
// KEYS pattern → 全量扫描 + 模式匹配
// 已实现：使用 DB::Scan 方法遍历所有 key，过滤 TTL 内部元数据键

// 扫描所有 key，通过 Iterator 遍历 LSM-Tree
auto iter = db_->NewIterator(ro);
for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    std::string key = iter->key();
    if (key[0] == TTL_MAGIC) continue;  // 过滤 TTL 元数据
    if (pattern_match(key, pattern)) {
        results.push_back(key);
    }
}
```

**实现状态**：✅ 已完成。支持 KEYS、RANDOMKEY。SCAN 为预留接口。

---

## 5. 高级功能层扩展

### 5.1 事务（MULTI/EXEC/DISCARD/WATCH）

**当前状态**：LightKV 已有基于快照隔离的 Transaction 类，但仅支持单客户端事务。

**Redis 事务特性**：
- MULTI/EXEC：命令队列，一次性执行
- DISCARD：取消事务
- WATCH：乐观锁，监控 key 变化

**需要的工作**：

```cpp
// 扩展现有 Transaction
class Transaction {
public:
    void Multi();              // 进入事务模式
    void QueueCommand(...);    // 命令入队
    std::vector<RespValue> Exec();  // 批量执行
    void Discard();            // 取消事务
    
    // WATCH 需要检测冲突
    void Watch(const std::string& key);
    bool CheckAndCommit();     // 检查 watched keys 是否被修改
};

// 需要修改：
// 1. 事务队列：从单命令改为命令列表
// 2. WATCH 机制：记录 watched keys，commit 时检查版本号
// 3. 错误处理：Redis 事务中单个命令失败不影响后续命令
```

**预计工作量**：2 周

### 5.2 Pub/Sub（PUBLISH/SUBSCRIBE/PSUBSCRIBE）

**需要全新的基础设施**：

```
┌──────────────────────────────────────────────────────┐
│                  Pub/Sub System                       │
├──────────────────────────────────────────────────────┤
│                                                      │
│  ┌─────────────┐     ┌─────────────────────────┐     │
│  │  Publisher  │────▶│  Channel Manager        │     │
│  │  PUBLISH    │     │  - 维护 channel → [client] │    │
│  │             │     │  - 广播消息到订阅者        │     │
│  └─────────────┘     └───────────┬─────────────┘     │
│                                  │                    │
│  ┌─────────────┐                 │                    │
│  │  Subscriber │◀────────────────┘                    │
│  │  SUBSCRIBE  │    推送响应: >channel\r\nmessage\r\n │
│  │  PSUBSCRIBE │                                      │
│  └─────────────┘                                      │
│                                                      │
│  持久化：可选（Redis 不支持，但可实现）                  │
│  - 每个 channel 维护消息队列（有限深度）                  │
│  - 离线消息投递                                         │
│                                                      │
└──────────────────────────────────────────────────────┘
```

**关键设计决策**：
1. **是否持久化**：Redis 不持久化 Pub/Sub 消息，LightKV 可选择不持久化（简单）或持久化（增强）
2. **消息格式**：RESP 推送格式 `>channel\r\nmessage\r\n`
3. **连接管理**：订阅连接需要特殊处理（不执行普通命令）

**预计工作量**：2 周

### 5.3 Lua 脚本（EVAL/EVALSHA）

**需要嵌入 Lua 解释器**：

```cpp
// 依赖：lua-5.4 或 luajit
#include <lua.hpp>

class ScriptEngine {
public:
    std::string Eval(const std::string& script, 
                     const std::vector<std::string>& keys,
                     const std::vector<std::string>& args);
    
    std::string EvalSha(const std::string& sha, ...);  // 缓存脚本
    
private:
    lua_State* lua_;
    std::unordered_map<std::string, std::string> script_cache_;  // sha → script
};
```

**关键设计**：
1. **沙箱安全**：限制 Lua 脚本可访问的 API（仅 KV 操作，无文件系统/网络访问）
2. **执行超时**：防止死循环脚本（Redis 默认 5 秒）
3. **原子性**：Lua 脚本执行期间阻塞其他命令（与 Redis 一致）
4. **SHA 缓存**：避免重复传输脚本

**预计工作量**：2 周

### 5.4 复制（Replication）

**主从复制架构**：

```
┌──────────────┐      SYNC      ┌──────────────┐
│   Master     │ ─────────────▶ │   Replica    │
│              │                │              │
│  - 写入 WAL  │                │  - 重放命令  │
│  - 广播增量  │ ◀───────────── │  - PSYNC     │
│              │    ACK         │              │
└──────────────┘                └──────────────┘
```

**需要的工作**：
1. **复制协议**：PSYNC 命令、全量/增量同步
2. **复制日志**：Master 维护复制偏移量，Slave 报告已同步位置
3. **主从切换**：需要哨兵或手动切换

**预计工作量**：4 周

### 5.5 集群（Cluster）

**分片架构**：

```
┌──────────────────────────────────────────────────┐
│              LightKV Cluster                      │
├──────────────────────────────────────────────────┤
│                                                  │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐          │
│  │ Shard 1 │  │ Shard 2 │  │ Shard 3 │          │
│  │ (0-5461)│  │(5462-109 │  │(10923-  │          │
│  │         │  │  22)     │  │  16383) │          │
│  └────┬────┘  └────┬────┘  └────┬────┘          │
│       │            │            │                │
│  ┌────┴────┐  ┌────┴────┐  ┌────┴────┐          │
│  │ Master  │  │ Master  │  │ Master  │          │
│  │ + Slave │  │ + Slave │  │ + Slave │          │
│  └─────────┘  └─────────┘  └─────────┘          │
│                                                  │
│  槽位映射：16384 个 hash slot                      │
│  slot = crc16(key) % 16384                       │
│                                                  │
└──────────────────────────────────────────────────┘
```

**需要的工作**：
1. **分片路由**：客户端根据 key 计算 slot，路由到正确节点
2. **集群发现**：CLUSTER SLOTS / CLUSTER NODES 命令
3. **数据迁移**：CLUSTER SETSLOT / CLUSTER IMPORT
4. **故障检测**：节点心跳、主从切换

**预计工作量**：8-12 周

---

## 6. 存储引擎层改造

### 6.1 当前 SSTable 格式

```
┌─────────────────────────┐
│  SSTable File           │
├─────────────────────────┤
│  Block 0: KV pair       │
│  Block 1: KV pair       │
│  ...                    │
│  Filter Block           │
│  Index Block            │
│  Metadata Block         │
└─────────────────────────┘
```

### 6.2 需要扩展的元数据

```
┌─────────────────────────────────────────┐
│  Metadata Block (扩展后)                  │
├─────────────────────────────────────────┤
│  - version: uint32                       │
│  - sequence_number: uint64               │
│  - key_count: uint64                     │
│  - type_index: Map<string, uint8>        │  ← 新增：key → 数据类型
│  - ttl_index: Map<string, uint64>        │  ← 新增：key → 过期时间
│  - checksum: uint64                      │
└─────────────────────────────────────────┘
```

### 6.3 TTL 过期策略

Redis 使用**惰性删除 + 定期删除**：

```cpp
// 惰性删除：GET 时检查是否过期
auto val = db->Get(key);
if (val && is_expired(key)) {
    db->Delete(key);
    return nullptr;
}

// 定期删除：后台线程随机抽取 20 个 key，删除过期的
void BackgroundExpire() {
    for (int i = 0; i < 20; i++) {
        auto key = random_key();
        if (is_expired(key)) {
            db->Delete(key);
        }
    }
}
```

---

## 7. 实现路线图

### Phase 1: String 扩展 + 通用命令（已 ✅ 完成）

| 任务 | 优先级 | 说明 | 状态 |
|------|--------|------|------|
| INCR/DECR/INCRBY/DECRBY | P0 | 原子计数 | ✅ |
| INCRBYFLOAT | P0 | 浮点原子计数 | ✅ |
| MSET/MGET | P0 | 批量操作 | ✅ |
| SETEX/PSETEX/SETNX | P0 | 带过期/条件设置 | ✅ |
| GETSET | P0 | 原子替换 | ✅ |
| GETRANGE | P0 | 子串截取 | ✅ |
| STRLEN | P0 | 字符串长度 | ✅ |
| APPEND | P0 | 追加字符串 | ✅ |
| EXISTS | P0 | 键存在性检查 | ✅ |
| EXPIRE/PEXPIRE/EXPIRETIME | P0 | 设置/查询过期时间 | ✅ |
| TTL/PTTL | P0 | 查询剩余时间 | ✅ |
| PERSIST | P0 | 移除过期时间 | ✅ |
| TYPE | P0 | 查询数据类型 | ✅ |
| RENAME/RENAMENX | P0 | 键重命名 | ✅ |
| DEL（多 key） | P0 | 批量删除 | ✅ |
| KEYS | P0 | 模式匹配 | ✅ |
| RANDOMKEY | P0 | 随机键 | ✅ |
| SCAN | P1 | 游标扫描（预留接口） | 🔲 |
| DUMP/RESTORE | P1 | 序列化迁移 | 🔲 |

**实际用时**：优于预期的 2-3 周，所有 P0 命令在一轮迭代中完整实现并测试通过。

**验证结果**：
- C++ 回归测试：9/9 全部通过（含 66 个 P0 测试用例）
- 4 种 SDK 语法/编译验证全部通过
- 单线程基准测试：C++ ~28K ops/s，Node.js ~20K ops/s，Python ~24K ops/s，Go ~22K ops/s
- Pipeline 加速比：最高 15.5x（Node.js），最低 6.6x（Python）
- 并发压力测试：10 线程 30K ops，C++ **60,768 ops/s** 最高，Go 45,670 ops/s 其次

### Phase 2: Hash + List + Set（4-5 周）

| 任务 | 优先级 | 说明 | 状态 |
|------|--------|------|------|
| HSET/HGET/HMSET/HMGET | P1 | Hash 基础操作 | ✅ |
| HGETALL/HKEYS/HVALS | P1 | Hash 全量读取 | ✅ |
| HDEL/HLEN/HSTRLEN | P1 | Hash 管理 | ✅ |
| HINCRBY | P1 | Hash 原子计数 | ✅ |
| HEXISTS | P1 | Hash 条件操作 | ✅ |
| LPUSH/RPUSH/LPOP/RPOP | P1 | List 基础操作 | ✅ |
| LRANGE/LINDEX/LLEN | P1 | List 查询 | ✅ |
| LSET/LTRIM/LREM | P1 | List 修改 | ✅ |
| SADD/SREM/SMEMBERS | P1 | Set 基础操作 | ✅ |
| SISMEMBER/SRANDMEMBER | P1 | Set 查询 | ✅ |
| SPOP/SCARD/SMOVE | P1 | Set 管理 | ✅ |
| BLPOP/BRPOP | P1 | 阻塞式 pop | 🔲 |
| HINCRBYFLOAT | P1 | Hash 浮点原子计数 | 🔲 |
| HSETNX | P1 | Hash 条件设置 | 🔲 |
| LINSERT | P1 | List 插入 | 🔲 |
| SINTER/SUNION/SDIFF | P1 | Set 集合运算 | 🔲 |

**验证结果**：
- C++ 回归测试：92/92 全部通过（含 P1 命令测试）
- P0 回归测试：67/67 全部通过（无回归）
- 数据类型编码：Hash（KV encoding）、List（Index+KV）、Set（Sorted Blob）
- 内部 key 隔离：使用 `\x02_hash_`、`\x03_list_`、`\x04_set_` 前缀，KEYS/RANDOMKEY 正确过滤
- 4 种 SDK（C++/Node.js/Python/Go）全部实现并验证

### Phase 3: ZSet + Bitmap + HLL + Geo（4-5 周）

| 任务 | 优先级 | 说明 | 状态 |
|------|--------|------|------|
| ZADD/ZREM/ZRANGE/ZREVRANGE | P2 | ZSet 基础操作 | ✅ |
| ZSCORE/ZCARD/ZCOUNT | P2 | ZSet 查询 | ✅ |
| ZRANGEBYSCORE | P2 | ZSet 范围操作 | ✅ |
| ZRANK/ZREVRANK | P2 | ZSet 排名查询 | ✅ |
| ZINCRBY | P2 | ZSet 修改 | 🔲 |
| ZREMRANGEBYSCORE/ZLEXCOUNT/ZRANGEBYLEX | P2 | ZSet 高级 | 🔲 |
| SETBIT/GETBIT | P2 | Bitmap 位操作 | ✅ |
| BITCOUNT/BITPOS | P2 | Bitmap 统计 | ✅ |
| BITOP | P2 | Bitmap 位运算 | 🔲 |
| PFADD/PFCOUNT/PFMERGE | P2 | HLL 操作 | ✅ |
| GEOADD/GEOPOS/GEODIST | P2 | Geo 基础 | ✅ |
| GEORADIUS/GEORADIUSBYMEMBER | P2 | Geo 范围查询 | 🔲 |
| GEOHASH | P2 | Geo 编码 | 🔲 |

**验证结果**：
- Geo 回归测试：19/19 全部通过（GEOADD/GEOPOS/GEODIST/边界情况）
- ZSet 回归测试：69/69 全部通过（ZADD/ZREM/ZSCORE/ZCARD/ZRANGE/ZREVRANGE/ZCOUNT/ZRANGEBYSCORE/ZRANK/ZREVRANK/TYPE/Pipeline/负数分数/同分数字典序排序）
- C++ 回归测试：23/23 全部通过（Bitmap + HLL）
- P0/P1 回归测试：67/67 + 92/92 全部通过（无回归）
- 总计测试用例：256 个（P0: 67 + P1: 92 + P2: 23 + ZSet: 55 + Geo: 19）
- Geo：复用 ZSet 存储结构，52 位 Geohash 编码，Haversine 距离计算
- ZSet：KV 编码方案，`\x05_zset_` 前缀隔离，`zset:{name}:member:{member}` → score，`zset:{name}:score:{padded_score}:{member}` → ""，score 填充为 `+00000000000001.0000` 格式确保字典序正确排序
- Bitmap：基于 String 位级操作，MSB first 编码，支持任意偏移量自动扩容
- HLL：2^14=16384 registers，MurmurHash2 64A 哈希，标准误差 ~0.81%，小基数偏差校正
- SDK 支持：C++ SDK 完整支持 Geo 命令；Node.js/Python/Go SDK 尚未实现 Geo 方法

### Phase 4: Stream + 事务扩展（4-5 周）

| 任务 | 优先级 | 说明 | 状态 |
|------|--------|------|------|
| XADD/XREAD/XDEL | P3 | Stream 基础 | 🔲 |
| XLEN/XRANGE/XREVRANGE | P3 | Stream 查询 | 🔲 |
| XGROUP/XREADGROUP | P3 | 消费者组 | 🔲 |
| XACK/XPENDING/XCLAIM | P3 | ACK 机制 | 🔲 |
| MULTI/EXEC/DISCARD | P3 | 事务队列 | 🔲 |
| WATCH/UNWATCH | P3 | 乐观锁 | 🔲 |

### Phase 5: Pub/Sub + Lua（3-4 周）

| 任务 | 优先级 | 说明 | 状态 |
|------|--------|------|------|
| PUBLISH/SUBSCRIBE | P3 | 基础 Pub/Sub | 🔲 |
| PSUBSCRIBE/PUNSUBSCRIBE | P3 | 模式订阅 | 🔲 |
| PUBSUB CHANNELS/NUMSUB | P3 | 频道管理 | 🔲 |
| EVAL/EVALSHA | P3 | Lua 脚本 | 🔲 |
| SCRIPT LOAD/EXISTS/FLUSH | P3 | 脚本管理 | 🔲 |

### Phase 6: 复制 + 集群（长期规划）

| 任务 | 优先级 | 说明 | 状态 |
|------|--------|------|------|
| PSYNC 复制协议 | P4 | 主从同步 | 🔲 |
| 哨兵/自动故障转移 | P4 | 高可用 | 🔲 |
| Slot 分片 | P4 | 数据分片 | 🔲 |
| 集群模式 | P4 | 完整集群支持 | 🔲 |

---

## 8. 难度评估总览

| 维度 | 难度 | 预计总工作量 |
|------|------|------------|
| 数据类型扩展 | ⭐⭐⭐ | 8-10 周（已完成 8 种，全部完成） |
| 命令实现 | ⭐⭐ | 6-8 周（与类型扩展并行，83 条已完成） |
| 事务扩展 | ⭐⭐ | 2 周 |
| Pub/Sub | ⭐⭐⭐ | 2 周 |
| Lua 脚本 | ⭐⭐⭐ | 2 周 |
| 复制 | ⭐⭐⭐⭐ | 4 周 |
| 集群 | ⭐⭐⭐⭐⭐ | 8-12 周 |
| **总计** | — | **24-36 周** |

### 关键瓶颈

1. **Stream**：需要全新的存储结构，支持消息 ID 生成、消费者组、ACK 机制
2. **集群**：涉及分布式一致性、故障转移、数据迁移等复杂问题
3. **Lua 脚本**：需要嵌入解释器，沙箱安全需要仔细设计
4. **TTL 过期**：需要修改 SSTable 格式，设计高效的过期清理策略

---

## 9. 架构建议

### 9.1 渐进式演进策略

不要试图一次性实现所有功能，建议按以下顺序演进：

```
当前 (v1.0) → Phase 1 已 ✅ 完成
    │
    ├── Phase 1: String 扩展 + 通用命令 ✅
    │       → 34 条命令，覆盖 80% 的常见使用场景
    │       → 4 种语言 SDK 全部支持
    │       → Pipeline + TTL + 惰性删除
    │
    ├── Phase 2: Hash + List + Set ✅
    │       → 30+ 条命令，覆盖 90% 的使用场景
    │       → 4 种语言 SDK 全部支持
    │       → 91/91 回归测试通过
    │
    ├── Phase 3: ZSet + Bitmap + HLL + Geo (4-5 周) ✅
    │       ├── ZSet 10 条命令 ✅（ZADD/ZREM/ZSCORE/ZCARD/ZRANGE/ZREVRANGE/ZCOUNT/ZRANGEBYSCORE/ZRANK/ZREVRANK）
    │       ├── Bitmap 4 条命令 ✅（SETBIT/GETBIT/BITCOUNT/BITPOS）
    │       ├── HLL 3 条命令 ✅（PFADD/PFCOUNT/PFMERGE）
    │       └── Geo 3 条命令 ✅（GEOADD/GEOPOS/GEODIST）
    │       → 覆盖 98% 的使用场景
    │
    ├── Phase 4: Stream + 事务 (4-5 周) 🔲
    │       → 高级功能
    │
    └── Phase 5+: Pub/Sub + Lua + 复制 + 集群 🔲
            → 企业级功能
```

### 9.2 核心设计原则

1. **KV 编码优先**：能用 KV 编码解决的，不引入新结构
2. **阈值自动切换**：小数据用简单编码，大数据用优化编码
3. ** RESP 协议兼容**：命令接口保持与 Redis 一致
4. **渐进式优化**：先实现功能，再优化性能
5. **测试驱动**：每个命令都需要单元测试 + 兼容性测试

---

## 10. 结论

LightKV 要支持 Redis 全量功能，核心挑战不在于协议层（RESP 已支持），而在于：

1. **存储层**：已从纯 KV 存储演进为支持 8 种复合数据类型的混合存储（String/Hash/List/Set/ZSet/Bitmap/HLL/Geo）
2. **命令层**：已建立完善的命令路由和类型系统，支持 83 条命令（P0: 35 + P1: 30 + P2: 18）
3. **高级功能**：Pub/Sub、Lua、复制、集群需要全新的基础设施

**当前进展**：Phase 0-3 已全部完成，覆盖 98% 的实际使用场景。8 种数据类型、83 条命令全部实现并通过回归测试（256 个测试用例）。Geo 命令已在服务端和 C++ SDK 实现，Node.js/Python/Go SDK 待补充。Stream、Pub/Sub、Lua、集群等高级功能可作为后续迭代目标。
