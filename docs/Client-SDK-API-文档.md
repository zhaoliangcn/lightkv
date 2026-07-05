# LightKV Client SDK API 文档

LightKV 提供多语言 Client SDK，通过 TCP 连接 LightKV Server，使用 Redis RESP 协议进行通信。

## 目录

- [协议说明](#协议说明)
- [C++ SDK](#c-sdk)
- [Node.js SDK](#nodejs-sdk)
- [Python SDK](#python-sdk)
- [Go SDK](#go-sdk)
- [Pipeline 批量操作](#pipeline-批量操作)
- [RESP 协议格式](#resp-协议格式)

---

## 协议说明

- **传输协议**: TCP
- **数据格式**: Redis RESP (REdis Serialization Protocol)
- **默认端口**: 16379
- **支持命令**: SET, GET, DEL, DELRANGE, PING, STATS, QUIT, INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT, MSET, MGET, SETEX, PSETEX, SETNX, GETSET, GETRANGE, APPEND, STRLEN, EXISTS, EXPIRE, PEXPIRE, EXPIRETIME, TTL, PTTL, PERSIST, TYPE, RENAME, RENAMENX, KEYS, SCAN, RANDOMKEY, HSET, HGET, HMSET, HMGET, HGETALL, HDEL, HEXISTS, HLEN, HKEYS, HVALS, HINCRBY, HSTRLEN, LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN, LINDEX, LSET, LTRIM, LREM, SADD, SREM, SMEMBERS, SISMEMBER, SCARD, SPOP, SRANDMEMBER, SMOVE

---

## C++ SDK

### 安装

```cpp
#include "lightkv/client.h"
```

### 连接管理

#### `Client()`

创建客户端实例。

```cpp
lightkv::Client client;
```

#### `Connect(host, port)`

连接到 LightKV 服务器。

- **参数**:
  - `host` (string): 服务器地址，默认 `"127.0.0.1"`
  - `port` (uint16_t): 服务器端口，默认 `6379`
- **返回**: `bool` — 连接是否成功

```cpp
bool ok = client.Connect("127.0.0.1", 6379);
```

#### `Disconnect()`

断开连接。

```cpp
client.Disconnect();
```

#### `IsConnected()`

检查是否已连接。

- **返回**: `bool`

```cpp
if (client.IsConnected()) { /* ... */ }
```

#### `GetLastError()`

获取最后一次错误信息。

- **返回**: `std::string`

```cpp
std::string err = client.GetLastError();
```

### 核心操作

#### `Set(key, value)`

设置键值对。

- **参数**:
  - `key` (string): 键
  - `value` (string): 值
- **返回**: `bool` — 是否成功

```cpp
bool ok = client.Set("user:1", "Alice");
```

#### `Get(key)`

获取键对应的值。

- **参数**:
  - `key` (string): 键
- **返回**: `std::optional<std::string>` — 值存在时返回 `std::string`，不存在时返回 `std::nullopt`

```cpp
auto val = client.Get("user:1");
if (val.has_value()) {
    std::cout << *val << std::endl;  // "Alice"
}
```

#### `Delete(key)`

删除键。

- **参数**:
  - `key` (string): 键
- **返回**: `bool` — 是否成功删除

```cpp
bool ok = client.Delete("user:1");
```

#### `DeleteRange(begin, end)`

删除范围 `[begin, end)` 内的所有键。

- **参数**:
  - `begin` (string): 起始键（包含）
  - `end` (string): 结束键（不包含）
- **返回**: `bool` — 是否成功

```cpp
bool ok = client.DeleteRange("user:1", "user:10");
```

### 工具命令

#### `Ping()`

检测服务器是否存活。

- **返回**: `bool` — 收到 PONG 时返回 `true`

```cpp
bool alive = client.Ping();
```

#### `Stats()`

获取服务器统计信息。

- **返回**: `std::vector<std::pair<std::string, std::string>>` — 键值对列表

```cpp
auto stats = client.Stats();
for (const auto& [k, v] : stats) {
    std::cout << k << ": " << v << std::endl;
}
```

#### `Quit()`

发送退出命令并断开连接。

- **返回**: `bool`

```cpp
client.Quit();
```

### String 扩展命令

#### `Incr(key)`

key 的值自增 1。

- **参数**:
  - `key` (string): 键
- **返回**: `std::optional<int64_t>` — 自增后的值

```cpp
auto val = client.Incr("counter");  // 返回 1（初始）
auto val2 = client.Incr("counter"); // 返回 2
```

#### `Decr(key)`

key 的值自减 1。

- **参数**:
  - `key` (string): 键
- **返回**: `std::optional<int64_t>`

```cpp
auto val = client.Decr("counter");
```

#### `IncrBy(key, delta)`

key 的值增加 `delta`。

- **参数**:
  - `key` (string): 键
  - `delta` (int64_t): 增量
- **返回**: `std::optional<int64_t>`

```cpp
auto val = client.IncrBy("counter", 10); // +10
```

#### `DecrBy(key, delta)`

key 的值减少 `delta`。

- **参数**:
  - `key` (string): 键
  - `delta` (int64_t): 减量
- **返回**: `std::optional<int64_t>`

```cpp
auto val = client.DecrBy("counter", 5); // -5
```

#### `IncrByFloat(key, delta)`

key 的值增加浮点数 `delta`。

- **参数**:
  - `key` (string): 键
  - `delta` (double): 浮点数增量
- **返回**: `std::optional<std::string>` — 结果的字符串表示

```cpp
auto val = client.IncrByFloat("counter", 1.5);
// 返回 "1.5"
```

#### `MSet(kvs)`

批量设置多个键值对。

- **参数**:
  - `kvs` (vector<pair<string,string>>): 键值对列表
- **返回**: `bool`

```cpp
bool ok = client.MSet({{"a", "1"}, {"b", "2"}, {"c", "3"}});
```

#### `MGet(keys)`

批量获取多个键的值。

- **参数**:
  - `keys` (vector&lt;string&gt;): 键列表
- **返回**: `vector<optional<string>>` — 值与键一一对应，不存在的 key 为 nullopt

```cpp
auto vals = client.MGet({"a", "b", "d"});
// vals: ["1", "2", nullopt]
```

#### `SetEx(key, seconds, value)`

设置键值对并指定过期时间（秒）。

- **参数**:
  - `key` (string): 键
  - `seconds` (int64_t): 过期时间（秒）
  - `value` (string): 值
- **返回**: `bool`

```cpp
bool ok = client.SetEx("session", 3600, "token123");
```

#### `SetNx(key, value)`

仅当 key 不存在时设置。

- **参数**:
  - `key` (string): 键
  - `value` (string): 值
- **返回**: `bool` — `true` 设置成功，`false` key 已存在

```cpp
bool ok = client.SetNx("lock:task", "1"); // 分布式锁
```

#### `GetSet(key, value)`

设置新值并返回旧值。

- **参数**:
  - `key` (string): 键
  - `value` (string): 新值
- **返回**: `std::optional<std::string>` — 旧值

```cpp
auto old = client.GetSet("counter", "100"); // 返回旧值
```

#### `Append(key, value)`

追加值到 key 末尾。

- **参数**:
  - `key` (string): 键
  - `value` (string): 追加的值
- **返回**: `int64_t` — 追加后的字符串长度

```cpp
int64_t len = client.Append("log", "new entry\n");
```

#### `StrLen(key)`

返回 key 值的字节长度。

- **参数**:
  - `key` (string): 键
- **返回**: `int64_t` — 长度，key 不存在时返回 0

```cpp
int64_t len = client.StrLen("hello"); // 返回 13
```

### 通用命令

#### `Exists(keys)`

检查一个或多个 key 是否存在。

- **参数**:
  - `keys` (vector&lt;string&gt;): 键列表
- **返回**: `int64_t` — 存在的 key 数量

```cpp
int64_t n = client.Exists({"a", "b", "c"});
```

#### `Expire(key, seconds)`

设置 key 的过期时间（秒）。

- **参数**:
  - `key` (string): 键
  - `seconds` (int64_t): 过期秒数
- **返回**: `bool` — `true` 设置成功，`false` key 不存在

```cpp
bool ok = client.Expire("session", 3600);
```

#### `Ttl(key)`

获取 key 的剩余生存时间（秒）。

- **参数**:
  - `key` (string): 键
- **返回**: `int64_t` — `-2` 不存在，`-1` 无 TTL，`>=0` 剩余秒数

```cpp
int64_t ttl = client.Ttl("session");
```

#### `Pttl(key)`

获取 key 的剩余生存时间（毫秒）。

- **参数**:
  - `key` (string): 键
- **返回**: `int64_t`

```cpp
int64_t pttl = client.Pttl("session");
```

#### `Persist(key)`

移除 key 的过期时间。

- **参数**:
  - `key` (string): 键
- **返回**: `bool` — `true` 成功移除，`false` key 不存在或无 TTL

```cpp
bool ok = client.Persist("session");
```

#### `Type(key)`

获取 key 的数据类型。

- **参数**:
  - `key` (string): 键
- **返回**: `std::string` — `"string"`、`"none"`

```cpp
std::string type = client.Type("key1");
```

#### `Rename(key, newkey)`

重命名 key，若 newkey 已存在则覆盖。

- **参数**:
  - `key` (string): 旧键名
  - `newkey` (string): 新键名
- **返回**: `bool`

```cpp
bool ok = client.Rename("old_name", "new_name");
```

#### `RenameNx(key, newkey)`

仅当 newkey 不存在时重命名。

- **参数**:
  - `key` (string): 旧键名
  - `newkey` (string): 新键名
- **返回**: `bool` — `true` 重命名成功，`false` 目标已存在

```cpp
bool ok = client.RenameNx("key", "new_key");
```

#### `Keys(pattern)`

查找所有匹配 pattern 的 key。

- **参数**:
  - `pattern` (string): 匹配模式，如 `"*"`、`"user:*"`
- **返回**: `vector<string>` — key 列表

```cpp
auto keys = client.Keys("*");        // 所有 key
auto users = client.Keys("user:*"); // 匹配前缀
```

### Hash 命令

#### `HSet(key, fields)`

设置一个或多个哈希字段。

- **参数**:
  - `key` (string): 哈希键名
  - `fields` (vector<pair<string,string>>): 字段-值对列表
- **返回**: `int64_t` — 新增字段数量

```cpp
int64_t n = client.HSet("user:1", {{"name", "Alice"}, {"age", "30"}});
```

#### `HGet(key, field)`

获取哈希字段值。

- **参数**:
  - `key` (string): 哈希键名
  - `field` (string): 字段名
- **返回**: `optional<string>` — 字段值

```cpp
auto val = client.HGet("user:1", "name"); // "Alice"
```

#### `HMSet(key, fields)`

批量设置哈希字段。

- **参数**:
  - `key` (string): 哈希键名
  - `fields` (vector<pair<string,string>>): 字段-值对列表
- **返回**: `bool`

```cpp
bool ok = client.HMSet("user:1", {{"x", "1"}, {"y", "2"}});
```

#### `HMGet(key, fields)`

批量获取哈希字段值。

- **参数**:
  - `key` (string): 哈希键名
  - `fields` (vector<string>): 字段名列表
- **返回**: `vector<optional<string>>` — 值与字段一一对应

```cpp
auto vals = client.HMGet("user:1", {"name", "age", "missing"});
```

#### `HGetAll(key)`

获取所有哈希字段和值。

- **参数**:
  - `key` (string): 哈希键名
- **返回**: `vector<pair<string,string>>` — 字段-值对列表

```cpp
auto all = client.HGetAll("user:1");
for (auto& [f, v] : all) { /* ... */ }
```

#### `HDel(key, fields)`

删除一个或多个哈希字段。

- **参数**:
  - `key` (string): 哈希键名
  - `fields` (vector<string>): 字段名列表
- **返回**: `int64_t` — 成功删除的字段数量

```cpp
int64_t n = client.HDel("user:1", {"age", "missing"});
```

#### `HExists(key, field)`

检查哈希字段是否存在。

- **返回**: `bool`

```cpp
bool ok = client.HExists("user:1", "name");
```

#### `HLen(key)`

获取哈希字段数量。

- **返回**: `int64_t`

```cpp
int64_t n = client.HLen("user:1");
```

#### `HKeys(key)`

获取所有哈希字段名。

- **返回**: `vector<string>`

```cpp
auto keys = client.HKeys("user:1");
```

#### `HVals(key)`

获取所有哈希字段值。

- **返回**: `vector<string>`

```cpp
auto vals = client.HVals("user:1");
```

#### `HIncrBy(key, field, delta)`

哈希字段值增加整数。

- **返回**: `int64_t` — 新值

```cpp
int64_t v = client.HIncrBy("user:1", "score", 10);
```

#### `HStrLen(key, field)`

获取哈希字段值的长度。

- **返回**: `int64_t`

```cpp
int64_t len = client.HStrLen("user:1", "name");
```

### List 命令

#### `LPush(key, values)`

从列表左侧推入值。

- **参数**:
  - `key` (string): 列表键名
  - `values` (vector<string>): 值列表
- **返回**: `int64_t` — 列表长度

```cpp
int64_t len = client.LPush("mylist", {"a", "b", "c"});
```

#### `RPush(key, values)`

从列表右侧推入值。

- **返回**: `int64_t` — 列表长度

```cpp
int64_t len = client.RPush("mylist", {"x", "y"});
```

#### `LPop(key)`

从列表左侧弹出值。

- **返回**: `optional<string>` — 弹出的值

```cpp
auto val = client.LPop("mylist");
```

#### `RPop(key)`

从列表右侧弹出值。

- **返回**: `optional<string>` — 弹出的值

```cpp
auto val = client.RPop("mylist");
```

#### `LRange(key, start, stop)`

获取列表指定范围的元素。

- **参数**:
  - `start` (int64_t): 起始索引（支持负数）
  - `stop` (int64_t): 结束索引（支持负数）
- **返回**: `vector<string>`

```cpp
auto items = client.LRange("mylist", 0, -1); // 所有元素
```

#### `LLen(key)`

获取列表长度。

- **返回**: `int64_t`

```cpp
int64_t len = client.LLen("mylist");
```

#### `LIndex(key, idx)`

获取列表指定索引的元素。

- **返回**: `optional<string>`

```cpp
auto val = client.LIndex("mylist", 0); // 第一个元素
```

#### `LSet(key, idx, value)`

设置列表指定索引的值。

- **返回**: `bool`

```cpp
bool ok = client.LSet("mylist", 0, "NEW");
```

#### `LTrim(key, start, stop)`

修剪列表到指定范围。

- **返回**: `bool`

```cpp
bool ok = client.LTrim("mylist", 0, 9); // 保留前10个
```

#### `LRem(key, count, value)`

从列表中移除值。

- **参数**:
  - `count` (int64_t): >0 从头删, <0 从尾删, 0 删所有
  - `value` (string): 要移除的值
- **返回**: `int64_t` — 移除数量

```cpp
int64_t n = client.LRem("mylist", 2, "a");
```

### Set 命令

#### `SAdd(key, members)`

向集合添加成员。

- **参数**:
  - `key` (string): 集合键名
  - `members` (vector<string>): 成员列表
- **返回**: `int64_t` — 新增成员数量

```cpp
int64_t n = client.SAdd("myset", {"a", "b", "c"});
```

#### `SRem(key, members)`

从集合移除成员。

- **返回**: `int64_t` — 移除成员数量

```cpp
int64_t n = client.SRem("myset", {"a", "missing"});
```

#### `SMembers(key)`

获取集合所有成员。

- **返回**: `vector<string>`

```cpp
auto members = client.SMembers("myset");
```

#### `SIsMember(key, member)`

检查成员是否在集合中。

- **返回**: `bool`

```cpp
bool ok = client.SIsMember("myset", "a");
```

#### `SCard(key)`

获取集合基数（成员数量）。

- **返回**: `int64_t`

```cpp
int64_t n = client.SCard("myset");
```

#### `SPop(key)`

随机移除并返回一个成员。

- **返回**: `optional<string>`

```cpp
auto val = client.SPop("myset");
```

#### `SRandMember(key)`

随机返回一个成员（不移除）。

- **返回**: `optional<string>`

```cpp
auto val = client.SRandMember("myset");
```

#### `SMove(src, dst, member)`

将成员从一个集合移动到另一个。

- **返回**: `bool`

```cpp
bool ok = client.SMove("srcset", "dstset", "x");
```

### ZSet 命令

#### `ZAdd(key, members)`

向有序集合添加成员或更新既有成员分数。

- **参数**: `members` (vector<pair<double, string>>)：(score, member) 列表
- **返回**: `int64_t` — 新增成员数（不含更新）

```cpp
int64_t n = client.ZAdd("myzset", {{1.0, "a"}, {2.5, "b"}});
```

#### `ZRem(key, members)`

- **返回**: `int64_t` — 实际移除数

#### `ZScore(key, member)`

- **返回**: `optional<string>` — 分数字符串，不存在返回 nullopt

```cpp
auto s = client.ZScore("myzset", "a");
if (s) double v = std::stod(*s);
```

#### `ZCard(key)`

- **返回**: `int64_t` — 成员总数

#### `ZRange(key, start, stop, withscores = false)`

按升序排名返回成员范围，支持负索引与 WITHSCORES。`0` = 最低分，`-1` = 最高分。

- **返回**: `vector<string>` — 不带 scores 时为成员名列表；带 scores 时为 [member, score, ...]

```cpp
auto r = client.ZRange("myzset", 0, -1);
auto ws = client.ZRangeWithScores("myzset", 0, 2);
```

#### `ZRevRange(key, start, stop, withscores = false)`

按降序排名返回成员范围。

- **返回**: `vector<string>`

#### `ZCount(key, min, max)`

按分数区间计数，`min`/`max` 支持 `(num` 开区间。

- **返回**: `int64_t`

#### `ZRangeByScore(key, min, max, offset = 0, count = -1, withscores = false)`

按分数区间返回成员，支持 LIMIT 分页与 WITHSCORES。

- **返回**: `vector<string>`

#### `ZRank(key, member)`

返回成员升序排名（0 = 最低分），不存在返回 nullopt。

- **返回**: `optional<int64_t>`

```cpp
auto r = client.ZRank("myzset", "a");
if (r) std::cout << "rank=" << *r << std::endl;
```

#### `ZRevRank(key, member)`

返回成员降序排名（0 = 最高分），不存在返回 nullopt。

- **返回**: `optional<int64_t>`

```cpp
auto r = client.ZRevRank("myzset", "a");
if (r) std::cout << "rev_rank=" << *r << std::endl;
```

---

## Node.js SDK

### 安装

```bash
npm install lightkv
# 或直接引用
const LightKVClient = require('./clients/nodejs/src/client.js');
```

### 连接管理

#### `new LightKVClient(options)`

创建客户端实例。

- **参数**:
  - `options.host` (string): 服务器地址，默认 `'127.0.0.1'`
  - `options.port` (number): 服务器端口，默认 `6379`
  - `options.timeout` (number): 连接超时（毫秒），默认 `5000`

```javascript
const LightKVClient = require('lightkv');
const client = new LightKVClient({ host: '127.0.0.1', port: 6379 });
```

#### `connect()`

连接到服务器（异步）。

- **返回**: `Promise<void>`

```javascript
await client.connect();
```

#### `disconnect()`

断开连接。

```javascript
client.disconnect();
```

#### `isConnected()`

检查是否已连接。

- **返回**: `boolean`

```javascript
if (client.isConnected()) { /* ... */ }
```

#### `getLastError()`

获取最后一次错误信息。

- **返回**: `string | null`

```javascript
const err = client.getLastError();
```

### 核心操作

#### `set(key, value)`

设置键值对（异步）。

- **参数**:
  - `key` (string): 键
  - `value` (string): 值
- **返回**: `Promise<boolean>`

```javascript
const ok = await client.set('user:1', 'Alice');
```

#### `get(key)`

获取键对应的值（异步）。

- **参数**:
  - `key` (string): 键
- **返回**: `Promise<string | null>` — 不存在时返回 `null`

```javascript
const val = await client.get('user:1');
```

#### `delete(key)`

删除键（异步）。

- **参数**:
  - `key` (string): 键
- **返回**: `Promise<boolean>`

```javascript
const ok = await client.delete('user:1');
```

#### `deleteRange(begin, end)`

删除范围 `[begin, end)` 内的所有键（异步）。

- **参数**:
  - `begin` (string): 起始键（包含）
  - `end` (string): 结束键（不包含）
- **返回**: `Promise<boolean>`

```javascript
const ok = await client.deleteRange('user:1', 'user:10');
```

### 工具命令

#### `ping()`

检测服务器是否存活（异步）。

- **返回**: `Promise<boolean>`

```javascript
const alive = await client.ping();
```

#### `stats()`

获取服务器统计信息（异步）。

- **返回**: `Promise<Object>`

```javascript
const stats = await client.stats();
console.log(stats);
// { "db_size": "1000", "memtable_size": "2048", ... }
```

#### `quit()`

发送退出命令并断开连接（异步）。

- **返回**: `Promise<void>`

```javascript
await client.quit();
```

### String 扩展命令

#### `incr(key)`

- **参数**: `key` (string)
- **返回**: `Promise<number>`

```javascript
const val = await client.incr('counter');  // 1
```

#### `decr(key)`

- **参数**: `key` (string)
- **返回**: `Promise<number>`

```javascript
const val = await client.decr('counter');
```

#### `incrBy(key, delta)`

- **参数**: `key` (string), `delta` (number)
- **返回**: `Promise<number>`

```javascript
const val = await client.incrBy('counter', 10);
```

#### `decrBy(key, delta)`

- **参数**: `key` (string), `delta` (number)
- **返回**: `Promise<number>`

```javascript
const val = await client.decrBy('counter', 5);
```

#### `incrByFloat(key, delta)`

- **参数**: `key` (string), `delta` (number)
- **返回**: `Promise<string>`

```javascript
const val = await client.incrByFloat('counter', 1.5); // "1.5"
```

#### `mset(kvs)`

批量设置多个键值对。

- **参数**: `kvs` (Array&lt;[string, string]&gt;) — 键值对数组
- **返回**: `Promise<boolean>`

```javascript
const ok = await client.mset([['a', '1'], ['b', '2']]);
```

#### `mget(keys)`

批量获取多个键的值。

- **参数**: `keys` (string[])
- **返回**: `Promise<Array<string|null>>`

```javascript
const vals = await client.mget(['a', 'b', 'd']);
// ['1', '2', null]
```

#### `setEx(key, seconds, value)`

- **参数**: `key` (string), `seconds` (number), `value` (string)
- **返回**: `Promise<boolean>`

```javascript
const ok = await client.setEx('session', 3600, 'token');
```

#### `setNx(key, value)`

- **参数**: `key` (string), `value` (string)
- **返回**: `Promise<boolean>`

```javascript
const ok = await client.setNx('lock', '1');
```

#### `getSet(key, value)`

- **参数**: `key` (string), `value` (string)
- **返回**: `Promise<string|null>`

```javascript
const old = await client.getSet('counter', '100');
```

#### `append(key, value)`

- **参数**: `key` (string), `value` (string)
- **返回**: `Promise<number>` — 新长度

```javascript
const len = await client.append('log', 'data\n');
```

#### `strLen(key)`

- **参数**: `key` (string)
- **返回**: `Promise<number>`

```javascript
const len = await client.strLen('hello');
```

### 通用命令

#### `exists(keys)`

- **参数**: `keys` (string[])
- **返回**: `Promise<number>`

```javascript
const n = await client.exists(['a', 'b']);
```

#### `expire(key, seconds)`

- **参数**: `key` (string), `seconds` (number)
- **返回**: `Promise<boolean>`

```javascript
const ok = await client.expire('session', 3600);
```

#### `ttl(key)`

- **参数**: `key` (string)
- **返回**: `Promise<number>` — -2 不存在, -1 无 TTL, >=0 剩余秒数

```javascript
const ttl = await client.ttl('session');
```

#### `pttl(key)`

- **参数**: `key` (string)
- **返回**: `Promise<number>` — 毫秒

```javascript
const pttl = await client.pttl('session');
```

#### `persist(key)`

- **参数**: `key` (string)
- **返回**: `Promise<boolean>`

```javascript
const ok = await client.persist('session');
```

#### `type(key)`

- **参数**: `key` (string)
- **返回**: `Promise<string>`

```javascript
const t = await client.type('key1'); // 'string'
```

#### `rename(key, newKey)`

- **参数**: `key` (string), `newKey` (string)
- **返回**: `Promise<boolean>`

```javascript
const ok = await client.rename('old', 'new');
```

#### `renameNx(key, newKey)`

- **参数**: `key` (string), `newKey` (string)
- **返回**: `Promise<boolean>`

```javascript
const ok = await client.renameNx('key', 'newKey');
```

#### `keys(pattern)`

- **参数**: `pattern` (string)
- **返回**: `Promise<string[]>`

```javascript
const all = await client.keys('*');
const users = await client.keys('user:*');
```

### ZSet 命令

#### `zadd(key, members)`

向有序集合添加成员或更新既有成员分数。

- **参数**: `members` (Array<[number, string]>)：(score, member) 对数组
- **返回**: `Promise<number>` — 新增成员数（不含更新）

```javascript
const n = await client.zadd('myzset', [[1.0, 'a'], [2.5, 'b']]);
```

#### `zrem(key, members)`

- **返回**: `Promise<number>` — 实际移除数

#### `zscore(key, member)`

- **返回**: `Promise<number | null>` — 分数，不存在返回 null

```javascript
const s = await client.zscore('myzset', 'a');
if (s !== null) console.log('score', s);
```

#### `zcard(key)`

- **返回**: `Promise<number>` — 成员总数

#### `zrange(key, start, stop, withscores = false)`

按升序排名返回成员范围，支持负索引与 WITHSCORES。`0` = 最低分，`-1` = 最高分。

- **返回**: `Promise<string[]>` — 不带 scores 时为成员名列表；带 scores 时为 [member, score, ...]

```javascript
const r = await client.zrange('myzset', 0, -1);
const ws = await client.zrangeWithScores('myzset', 0, 2);
```

#### `zrevrange(key, start, stop, withscores = false)`

按降序排名返回成员范围。

- **返回**: `Promise<string[]>`

#### `zcount(key, min, max)`

按分数区间计数，`min`/`max` 支持 `(num` 开区间。

- **返回**: `Promise<number>`

#### `zrangebyscore(key, min, max, offset = 0, count = -1, withscores = false)`

按分数区间返回成员，支持 LIMIT 分页与 WITHSCORES。

- **返回**: `Promise<string[]>`

#### `zrank(key, member)`

返回成员升序排名（0 = 最低分），不存在返回 null。

- **返回**: `Promise<number | null>`

```javascript
const r = await client.zrank('myzset', 'a');
if (r !== null) console.log('rank', r);
```

#### `zrevrank(key, member)`

返回成员降序排名（0 = 最高分），不存在返回 null。

- **返回**: `Promise<number | null>`

```javascript
const r = await client.zrevrank('myzset', 'a');
if (r !== null) console.log('rev_rank', r);
```

---

## Python SDK

### 安装

```bash
pip install -e clients/python/
# 或直接引用
from lightkv.client import LightKVClient
```

### 连接管理

#### `LightKVClient(host, port, timeout)`

创建客户端实例。

- **参数**:
  - `host` (str): 服务器地址，默认 `'127.0.0.1'`
  - `port` (int): 服务器端口，默认 `6379`
  - `timeout` (float): 连接超时（秒），默认 `5.0`

```python
from lightkv.client import LightKVClient
client = LightKVClient(host='127.0.0.1', port=6379)
```

#### `connect()`

连接到服务器。

- **返回**: `bool`

```python
ok = client.connect()
```

#### `disconnect()`

断开连接。

```python
client.disconnect()
```

#### `is_connected()`

检查是否已连接。

- **返回**: `bool`

```python
if client.is_connected():
    # ...
```

#### `get_last_error()`

获取最后一次错误信息。

- **返回**: `str | None`

```python
err = client.get_last_error()
```

### 核心操作

#### `set(key, value)`

设置键值对。

- **参数**:
  - `key` (str): 键
  - `value` (str): 值
- **返回**: `bool`

```python
ok = client.set('user:1', 'Alice')
```

#### `get(key)`

获取键对应的值。

- **参数**:
  - `key` (str): 键
- **返回**: `str | None` — 不存在时返回 `None`

```python
val = client.get('user:1')
```

#### `delete(key)`

删除键。

- **参数**:
  - `key` (str): 键
- **返回**: `bool`

```python
ok = client.delete('user:1')
```

#### `delete_range(begin, end)`

删除范围 `[begin, end)` 内的所有键。

- **参数**:
  - `begin` (str): 起始键（包含）
  - `end` (str): 结束键（不包含）
- **返回**: `bool`

```python
ok = client.delete_range('user:1', 'user:10')
```

### 工具命令

#### `ping()`

检测服务器是否存活。

- **返回**: `bool`

```python
alive = client.ping()
```

#### `stats()`

获取服务器统计信息。

- **返回**: `Dict[str, str]`

```python
stats = client.stats()
print(stats)
# {'db_size': '1000', 'memtable_size': '2048', ...}
```

#### `quit()`

发送退出命令并断开连接。

```python
client.quit()
```

### String 扩展命令

#### `incr(key)`

- **参数**: `key` (str)
- **返回**: `int | None`

```python
val = client.incr('counter')  # 1
```

#### `decr(key)`

- **参数**: `key` (str)
- **返回**: `int | None`

```python
val = client.decr('counter')
```

#### `incr_by(key, delta)`

- **参数**: `key` (str), `delta` (int)
- **返回**: `int | None`

```python
val = client.incr_by('counter', 10)
```

#### `decr_by(key, delta)`

- **参数**: `key` (str), `delta` (int)
- **返回**: `int | None`

```python
val = client.decr_by('counter', 5)
```

#### `incr_by_float(key, delta)`

- **参数**: `key` (str), `delta` (float)
- **返回**: `str | None`

```python
val = client.incr_by_float('counter', 1.5)  # "1.5"
```

#### `mset(kvs)`

- **参数**: `kvs` (List[List[str]]) — [key, value] 对列表
- **返回**: `bool`

```python
ok = client.mset([['a', '1'], ['b', '2']])
```

#### `mget(keys)`

- **参数**: `keys` (List[str])
- **返回**: `List[str | None]`

```python
vals = client.mget(['a', 'b', 'd'])
# ['1', '2', None]
```

#### `set_ex(key, seconds, value)`

- **参数**: `key` (str), `seconds` (int), `value` (str)
- **返回**: `bool`

```python
ok = client.set_ex('session', 3600, 'token')
```

#### `set_nx(key, value)`

- **参数**: `key` (str), `value` (str)
- **返回**: `bool`

```python
ok = client.set_nx('lock', '1')
```

#### `get_set(key, value)`

- **参数**: `key` (str), `value` (str)
- **返回**: `str | None`

```python
old = client.get_set('counter', '100')
```

#### `append(key, value)`

- **参数**: `key` (str), `value` (str)
- **返回**: `int | None` — 新长度

```python
length = client.append('log', 'data\n')
```

#### `str_len(key)`

- **参数**: `key` (str)
- **返回**: `int | None`

```python
length = client.str_len('hello')
```

### 通用命令

#### `exists(keys)`

- **参数**: `keys` (List[str])
- **返回**: `int | None`

```python
n = client.exists(['a', 'b'])
```

#### `expire(key, seconds)`

- **参数**: `key` (str), `seconds` (int)
- **返回**: `bool`

```python
ok = client.expire('session', 3600)
```

#### `ttl(key)`

- **参数**: `key` (str)
- **返回**: `int | None` — -2 不存在, -1 无 TTL, >=0 剩余秒数

```python
ttl = client.ttl('session')
```

#### `pttl(key)`

- **参数**: `key` (str)
- **返回**: `int | None` — 毫秒

```python
pttl = client.pttl('session')
```

#### `persist(key)`

- **参数**: `key` (str)
- **返回**: `bool`

```python
ok = client.persist('session')
```

#### `type(key)`

- **参数**: `key` (str)
- **返回**: `str | None`

```python
t = client.type('key1')  # 'string'
```

#### `rename(key, new_key)`

- **参数**: `key` (str), `new_key` (str)
- **返回**: `bool`

```python
ok = client.rename('old', 'new')
```

#### `rename_nx(key, new_key)`

- **参数**: `key` (str), `new_key` (str)
- **返回**: `bool`

```python
ok = client.rename_nx('key', 'new_key')
```

#### `keys(pattern)`

- **参数**: `pattern` (str)
- **返回**: `List[str]`

```python
all_keys = client.keys('*')
user_keys = client.keys('user:*')
```

### ZSet 命令

#### `zadd(key, members)`

向有序集合添加成员或更新既有成员分数。

- **参数**: `members` (List[tuple])：(score, member) 元组列表
- **返回**: `int` — 新增成员数（不含更新）

```python
n = client.zadd('myzset', [(1.0, 'a'), (2.5, 'b')])
```

#### `zrem(key, members)`

- **返回**: `int` — 实际移除数

#### `zscore(key, member)`

- **返回**: `Optional[float]` — 分数，不存在返回 None

```python
s = client.zscore('myzset', 'a')
if s is not None: print('score', s)
```

#### `zcard(key)`

- **返回**: `int` — 成员总数

#### `zrange(key, start, stop, withscores = False)`

按升序排名返回成员范围，支持负索引与 WITHSCORES。`0` = 最低分，`-1` = 最高分。

- **返回**: `List[str]` — 不带 scores 时为成员名列表；带 scores 时为 [member, score, ...]

```python
r = client.zrange('myzset', 0, -1)
ws = client.zrange_withscores('myzset', 0, 2)
```

#### `zrevrange(key, start, stop, withscores = False)`

按降序排名返回成员范围。

- **返回**: `List[str]`

#### `zcount(key, min_score, max_score)`

按分数区间计数，`min`/`max` 支持 `(num` 开区间。

- **返回**: `int`

#### `zrangebyscore(key, min_score, max_score, offset = 0, count = -1, withscores = False)`

按分数区间返回成员，支持 LIMIT 分页与 WITHSCORES。

- **返回**: `List[str]`

#### `zrank(key, member)`

返回成员升序排名（0 = 最低分），不存在返回 None。

- **返回**: `Optional[int]`

```python
r = client.zrank('myzset', 'a')
if r is not None: print('rank', r)
```

#### `zrevrank(key, member)`

返回成员降序排名（0 = 最高分），不存在返回 None。

- **返回**: `Optional[int]`

```python
r = client.zrevrank('myzset', 'a')
if r is not None: print('rev_rank', r)
```

### Context Manager 支持

Python SDK 支持 `with` 语句自动管理连接。

```python
with LightKVClient() as client:
    client.set('key', 'value')
    val = client.get('key')
# 连接自动关闭
```

---

## Go SDK

### 安装

```go
import "github.com/yourorg/lightkv/clients/go"
// 或本地引用
import "lightkv/clients/go"
```

### 连接管理

#### `NewClient(addr, timeout)`

创建客户端实例。

- **参数**:
  - `addr` (string): 服务器地址（`host:port`），默认 `"127.0.0.1:6379"`
  - `timeout` (time.Duration): 连接超时，默认 `5 * time.Second`
- **返回**: `*Client`

```go
import "lightkv/clients/go"

client := lightkv.NewClient("127.0.0.1:6379", 5*time.Second)
```

#### `Connect()`

连接到服务器。

- **返回**: `error`

```go
err := client.Connect()
```

#### `Close()`

关闭连接。

- **返回**: `error`

```go
err := client.Close()
```

#### `IsConnected()`

检查是否已连接。

- **返回**: `bool`

```go
if client.IsConnected() {
    // ...
}
```

#### `GetLastError()`

获取最后一次错误信息。

- **返回**: `string`

```go
err := client.GetLastError()
```

### 核心操作

#### `Set(key, value)`

设置键值对。

- **参数**:
  - `key` (string): 键
  - `value` (string): 值
- **返回**: `(bool, error)`

```go
ok, err := client.Set("user:1", "Alice")
```

#### `Get(key)`

获取键对应的值。

- **参数**:
  - `key` (string): 键
- **返回**: `(string, bool, error)` — `bool` 表示键是否存在

```go
val, exists, err := client.Get("user:1")
if exists {
    fmt.Println(val)
}
```

#### `Delete(key)`

删除键。

- **参数**:
  - `key` (string): 键
- **返回**: `(bool, error)`

```go
ok, err := client.Delete("user:1")
```

#### `DeleteRange(begin, end)`

删除范围 `[begin, end)` 内的所有键。

- **参数**:
  - `begin` (string): 起始键（包含）
  - `end` (string): 结束键（不包含）
- **返回**: `(bool, error)`

```go
ok, err := client.DeleteRange("user:1", "user:10")
```

### 工具命令

#### `Ping()`

检测服务器是否存活。

- **返回**: `(bool, error)`

```go
alive, err := client.Ping()
```

#### `Stats()`

获取服务器统计信息。

- **返回**: `(map[string]string, error)`

```go
stats, err := client.Stats()
```

#### `Quit()`

发送退出命令并关闭连接。

- **返回**: `error`

```go
err := client.Quit()
```

### String 扩展命令

#### `Incr(key)`

- **参数**: `key` (string)
- **返回**: `(int64, error)`

```go
val, err := client.Incr("counter") // 1
```

#### `Decr(key)`

- **参数**: `key` (string)
- **返回**: `(int64, error)`

```go
val, err := client.Decr("counter")
```

#### `IncrBy(key, delta)`

- **参数**: `key` (string), `delta` (int64)
- **返回**: `(int64, error)`

```go
val, err := client.IncrBy("counter", 10)
```

#### `DecrBy(key, delta)`

- **参数**: `key` (string), `delta` (int64)
- **返回**: `(int64, error)`

```go
val, err := client.DecrBy("counter", 5)
```

#### `IncrByFloat(key, delta)`

- **参数**: `key` (string), `delta` (float64)
- **返回**: `(string, error)`

```go
val, err := client.IncrByFloat("counter", 1.5) // "1.5"
```

#### `MSet(kvs)`

- **参数**: `kvs` ([][2]string)
- **返回**: `error`

```go
err := client.MSet([][2]string{{"a", "1"}, {"b", "2"}})
```

#### `MGet(keys)`

- **参数**: `keys` ([]string)
- **返回**: `([]any, error)`

```go
vals, err := client.MGet([]string{"a", "b", "d"})
// vals: ["1", "2", nil]
```

#### `SetEx(key, seconds, value)`

- **参数**: `key` (string), `seconds` (int64), `value` (string)
- **返回**: `error`

```go
err := client.SetEx("session", 3600, "token")
```

#### `SetNx(key, value)`

- **参数**: `key` (string), `value` (string)
- **返回**: `(bool, error)`

```go
ok, err := client.SetNx("lock", "1")
```

#### `GetSet(key, value)`

- **参数**: `key` (string), `value` (string)
- **返回**: `(any, error)`

```go
old, err := client.GetSet("counter", "100")
```

#### `Append(key, value)`

- **参数**: `key` (string), `value` (string)
- **返回**: `(int64, error)`

```go
length, err := client.Append("log", "data\n")
```

#### `StrLen(key)`

- **参数**: `key` (string)
- **返回**: `(int64, error)`

```go
length, err := client.StrLen("hello")
```

### 通用命令

#### `Exists(keys)`

- **参数**: `keys` ([]string)
- **返回**: `(int64, error)`

```go
n, err := client.Exists([]string{"a", "b"})
```

#### `Expire(key, seconds)`

- **参数**: `key` (string), `seconds` (int64)
- **返回**: `(bool, error)`

```go
ok, err := client.Expire("session", 3600)
```

#### `Ttl(key)`

- **参数**: `key` (string)
- **返回**: `(int64, error)` — -2 不存在, -1 无 TTL, >=0 剩余秒数

```go
ttl, err := client.Ttl("session")
```

#### `Pttl(key)`

- **参数**: `key` (string)
- **返回**: `(int64, error)` — 毫秒

```go
pttl, err := client.Pttl("session")
```

#### `Persist(key)`

- **参数**: `key` (string)
- **返回**: `(bool, error)`

```go
ok, err := client.Persist("session")
```

#### `Type(key)`

- **参数**: `key` (string)
- **返回**: `(string, error)`

```go
t, err := client.Type("key1") // "string"
```

#### `Rename(key, newKey)`

- **参数**: `key` (string), `newKey` (string)
- **返回**: `error`

```go
err := client.Rename("old", "new")
```

#### `RenameNx(key, newKey)`

- **参数**: `key` (string), `newKey` (string)
- **返回**: `(bool, error)`

```go
ok, err := client.RenameNx("key", "newKey")
```

#### `Keys(pattern)`

- **参数**: `pattern` (string)
- **返回**: `([]string, error)`

```go
all, err := client.Keys("*")
users, err := client.Keys("user:*")
```

### ZSet 命令

#### `ZAdd(key, members)`

向有序集合添加成员或更新既有成员分数。

- **参数**: `members` ([]struct{ Score float64; Member string })：(score, member) 切片
- **返回**: `(int64, error)` — 新增成员数（不含更新）

```go
n, err := client.ZAdd("myzset", []struct{ Score float64; Member string }{
    {1.0, "a"}, {2.5, "b"},
})
```

#### `ZRem(key, members)`

- **返回**: `(int64, error)` — 实际移除数

#### `ZScore(key, member)`

- **返回**: `(float64, bool, error)` — (分数, 是否存在, 错误)

```go
s, ok, err := client.ZScore("myzset", "a")
if ok { fmt.Println("score", s) }
```

#### `ZCard(key)`

- **返回**: `(int64, error)` — 成员总数

#### `ZRange(key, start, stop)`

按升序排名返回成员范围，支持负索引。`0` = 最低分，`-1` = 最高分。

- **返回**: `([]string, error)`

```go
r, err := client.ZRange("myzset", 0, -1)
ws, err := client.ZRangeWithScores("myzset", 0, 2)
```

#### `ZRevRange(key, start, stop, withScores)`

按降序排名返回成员范围。

- **返回**: `([]string, error)`

#### `ZCount(key, min, max)`

按分数区间计数，`min`/`max` 支持 `(num` 开区间。

- **返回**: `(int64, error)`

#### `ZRangeByScore(key, min, max, offset, count, withScores)`

按分数区间返回成员，支持 LIMIT 分页与 WITHSCORES。

- **返回**: `([]string, error)`

#### `ZRank(key, member)`

返回成员升序排名（0 = 最低分），不存在时第二返回值为 false。

- **返回**: `(int64, bool, error)` — (排名, 是否存在, 错误)

```go
r, ok, err := client.ZRank("myzset", "a")
if ok { fmt.Println("rank", r) }
```

#### `ZRevRank(key, member)`

返回成员降序排名（0 = 最高分），不存在时第二返回值为 false。

- **返回**: `(int64, bool, error)`

```go
r, ok, err := client.ZRevRank("myzset", "a")
if ok { fmt.Println("rev_rank", r) }
```

---

## Pipeline 批量操作

Pipeline 允许客户端缓冲多个命令，一次性发送到服务器，减少网络往返次数，显著提升吞吐量。

### 使用流程

1. 调用 `Pipeline()` 开始缓冲
2. 调用 `Queue(args)` 添加命令到队列
3. 调用 `ExecPipeline()` 发送并获取所有响应

### C++ Pipeline

```cpp
client.Pipeline();
client.Queue({"SET", "key1", "value1"});
client.Queue({"SET", "key2", "value2"});
client.Queue({"GET", "key1"});

auto results = client.ExecPipeline();
// results: ["OK", "OK", "value1"]
```

### Node.js Pipeline

```javascript
client.pipeline();
client.queue(['SET', 'key1', 'value1']);
client.queue(['SET', 'key2', 'value2']);
client.queue(['GET', 'key1']);

const results = await client.execPipeline();
// results: ['OK', 'OK', 'value1']
```

### Python Pipeline

```python
client.pipeline()
client.queue(['SET', 'key1', 'value1'])
client.queue(['SET', 'key2', 'value2'])
client.queue(['GET', 'key1'])

results = client.exec_pipeline()
# results: ['OK', 'OK', 'value1']
```

### Go Pipeline

```go
client.Pipeline()
client.Queue([]string{"SET", "key1", "value1"})
client.Queue([]string{"SET", "key2", "value2"})
client.Queue([]string{"GET", "key1"})

results, err := client.ExecPipeline()
// results: []any{"OK", "OK", "value1"}
```

### Pipeline 性能对比

| SDK | 常规 SET | Pipeline SET | 提升 |
|-----|----------|--------------|------|
| C++ | 27,732 ops/s | 229,343 ops/s | **8.3x** |
| Node.js | 20,121 ops/s | 312,500 ops/s | **15.5x** |
| Python | 32,246 ops/s | 214,161 ops/s | **6.6x** |
| Go | 25,375 ops/s | 194,862 ops/s | **7.7x** |

---

## RESP 协议格式

LightKV 使用 Redis RESP 协议，支持以下数据类型：

### 请求格式

```
*<参数数量>\r\n
$<参数1长度>\r\n
<参数1>\r\n
$<参数2长度>\r\n
<参数2>\r\n
...
```

示例 — SET key value:
```
*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
```

### 响应格式

| 类型 | 格式 | 示例 |
|------|------|------|
| Simple String | `+<value>\r\n` | `+OK\r\n` |
| Error | `-<message>\r\n` | `-ERR unknown command\r\n` |
| Integer | `:<number>\r\n` | `:1\r\n` |
| Bulk String | `$<len>\r\n<data>\r\n` | `$5\r\nhello\r\n` |
| Bulk String (nil) | `$-1\r\n` | `$-1\r\n` |
| Array | `*<count>\r\n<元素>...` | `*2\r\n$3\r\nkey\r\n$5\r\nvalue\r\n` |

### 支持命令列表

| 命令 | 参数 | 响应 | 说明 |
|------|------|------|------|
| `SET` | key, value | `+OK` | 设置键值对 |
| `GET` | key | `$<len>\r\n<value>` 或 `$-1` | 获取值 |
| `DEL` | key [key ...] | `:count` | 删除一个或多个键 |
| `DELRANGE` | begin, end | `:1` 或 `:0` | 范围删除 |
| `PING` | 无 | `+PONG` | 心跳检测 |
| `STATS` | 无 | `*[key, value, ...]` | 服务器统计 |
| `QUIT` | 无 | `+OK` | 退出连接 |
| `INCR` | key | `:value` | 自增 1 |
| `DECR` | key | `:value` | 自减 1 |
| `INCRBY` | key, delta | `:value` | 增加 delta |
| `DECRBY` | key, delta | `:value` | 减少 delta |
| `INCRBYFLOAT` | key, delta | `$value` | 浮点自增 |
| `MSET` | key value [kv...] | `+OK` | 批量设置 |
| `MGET` | key [key...] | `*array` | 批量获取 |
| `SETEX` | key sec val | `+OK` | 设值并设 TTL |
| `SETNX` | key value | `:1` 或 `:0` | 不存在时设置 |
| `GETSET` | key value | `$old_value` 或 `$-1` | 设新返回旧 |
| `GETRANGE` | key start end | `$substr` | 子串 |
| `APPEND` | key value | `:new_len` | 追加 |
| `STRLEN` | key | `:len` | 字符串长度 |
| `EXISTS` | key [key...] | `:count` | 键存在计数 |
| `EXPIRE` | key sec | `:1` 或 `:0` | 设置 TTL |
| `PEXPIRE` | key ms | `:1` 或 `:0` | 设置 TTL (ms) |
| `EXPIRETIME` | key | `:timestamp` | 过期时间戳 |
| `TTL` | key | `:ttl` | 剩余秒数 |
| `PTTL` | key | `:pttl` | 剩余毫秒数 |
| `PERSIST` | key | `:1` 或 `:0` | 移除 TTL |
| `TYPE` | key | `+string` / `+none` | 数据类型 |
| `RENAME` | key, newkey | `+OK` | 重命名 |
| `RENAMENX` | key, newkey | `:1` 或 `:0` | 不存在时重命名 |
| `KEYS` | pattern | `*array` | 匹配 key 列表 |
| `RANDOMKEY` | 无 | `$key` 或 `$-1` | 随机 key |
| `HSET` | key field value [fv...] | `:count` | 设置哈希字段 |
| `HGET` | key field | `$value` 或 `$-1` | 获取哈希字段 |
| `HMSET` | key field value [fv...] | `+OK` | 批量设置哈希字段 |
| `HMGET` | key field [field...] | `*array` | 批量获取哈希字段 |
| `HGETALL` | key | `*[field, value...]` | 获取所有字段和值 |
| `HDEL` | key field [field...] | `:count` | 删除哈希字段 |
| `HEXISTS` | key field | `:1` 或 `:0` | 字段是否存在 |
| `HLEN` | key | `:count` | 哈希字段数量 |
| `HKEYS` | key | `*array` | 所有字段名 |
| `HVALS` | key | `*array` | 所有字段值 |
| `HINCRBY` | key field delta | `:value` | 哈希字段整数自增 |
| `HSTRLEN` | key field | `:len` | 哈希字段值长度 |
| `LPUSH` | key value [value...] | `:len` | 左侧推入列表 |
| `RPUSH` | key value [value...] | `:len` | 右侧推入列表 |
| `LPOP` | key | `$value` 或 `$-1` | 左侧弹出列表 |
| `RPOP` | key | `$value` 或 `$-1` | 右侧弹出列表 |
| `LRANGE` | key start stop | `*array` | 列表范围查询 |
| `LLEN` | key | `:len` | 列表长度 |
| `LINDEX` | key idx | `$value` 或 `$-1` | 列表索引访问 |
| `LSET` | key idx value | `+OK` | 列表索引设置 |
| `LTRIM` | key start stop | `+OK` | 列表修剪 |
| `LREM` | key count value | `:count` | 列表元素移除 |
| `SADD` | key member [member...] | `:count` | 集合添加成员 |
| `SREM` | key member [member...] | `:count` | 集合移除成员 |
| `SMEMBERS` | key | `*array` | 集合所有成员 |
| `SISMEMBER` | key member | `:1` 或 `:0` | 成员是否存在 |
| `SCARD` | key | `:count` | 集合基数 |
| `SPOP` | key | `$member` 或 `$-1` | 随机弹出成员 |
| `SRANDMEMBER` | key | `$member` 或 `$-1` | 随机获取成员 |
| `SMOVE` | src dst member | `:1` 或 `:0` | 移动成员 |

### 使用原生 RESP 通信

任何支持 RESP 协议的客户端都可以直接连接 LightKV Server：

```python
# 使用 redis-py 直接连接 LightKV
import redis
r = redis.Redis(host='127.0.0.1', port=6379)
r.set('key', 'value')
r.get('key')
```

---

## 错误处理

| 语言 | 错误处理方式 |
|------|-------------|
| C++ | 返回 `bool`，通过 `GetLastError()` 获取错误信息 |
| Node.js | 返回 `Promise`，通过 `.catch()` 或 `try/catch` 捕获 |
| Python | 抛出 `ConnectionError` / `TimeoutError` 异常 |
| Go | 返回 `(value, error)` 元组 |

## 线程安全

| 语言 | 线程安全 | 说明 |
|------|---------|------|
| C++ | 否 | 需要外部同步 |
| Node.js | 是 | 单线程事件循环 |
| Python | 是 | 内置 `threading.Lock` |
| Go | 是 | 内置 `sync.Mutex` |
