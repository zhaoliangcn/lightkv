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
- **默认端口**: 6379
- **支持命令**: SET, GET, DEL, DELRANGE, PING, STATS, QUIT

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
| C++ | 49,400 ops/s | 331,325 ops/s | **6.7x** |
| Node.js | 24,510 ops/s | 196,850 ops/s | **8.0x** |
| Python | 27,402 ops/s | 234,621 ops/s | **8.6x** |
| Go | 31,384 ops/s | 203,046 ops/s | **6.5x** |

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
| `DEL` | key | `:1` 或 `:0` | 删除键 |
| `DELRANGE` | begin, end | `:1` 或 `:0` | 范围删除 |
| `PING` | 无 | `+PONG` | 心跳检测 |
| `STATS` | 无 | `*[key, value, ...]` | 服务器统计 |
| `QUIT` | 无 | `+OK` | 退出连接 |

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
