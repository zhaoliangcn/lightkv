# LightKV IOCP 重写计划

## 1. 当前架构问题分析

### 1.1 核心矛盾
当前 server.cpp 基于 epoll/kqueue/select（**就绪通知模型**）构建，IOCP 是**完成通知模型**，两者编程范式完全不同：

| 维度 | 就绪通知 (epoll/kqueue) | 完成通知 (IOCP) |
|------|------------------------|-----------------|
| 注册 | 告诉内核"监控 fd X 的可读事件" | 告诉内核"在 fd X 上执行 N 字节读取" |
| 通知 | 内核说"fd X 可读了" | 内核说"fd X 的读取完成了" |
| 数据位置 | 应用主动调用 recv() | 数据已在缓冲区，直接处理 |
| 状态机 | IDLE → READ_READY → PROCESS | POSTING_RECV → RECV_COMPLETE → PROCESS |
| 连接数扩展 | O(n) 遍历所有 fd | O(1) 完成通知 |

### 1.2 当前实现的具体缺陷
1. **accept 线程与主循环割裂**：accept 线程用同步 `::accept()` + sleep(1ms)，主循环用 `GetQueuedCompletionStatus`
2. **`handle_client` 按就绪模型设计**：检查 `readable` 标志后调用 `::recv()`，IOCP 路径下数据已在缓冲区
3. **`add_event`/`mod_event`/`del_event` 空操作**：Windows 路径直接 `(void)fd`
4. **进程立即退出**：accept 线程或 IOCP 操作使用无效句柄导致崩溃
5. **`_get_osfhandle`/`static_cast<int>(sock)` 类型截断**：SOCKET 是 UINT_PTR，int 是 32 位

### 1.3 服务器进程退出根因
- accept 线程中 `::accept()` 返回的 SOCKET 被截断为 int
- `_get_osfhandle(fd)` 对未通过 `_open_osfhandle` 注册的 fd 返回 INVALID_HANDLE_VALUE
- `CreateIoCompletionPort` 收到无效句柄后静默失败
- 后续 IOCP 操作访问无效内存 → 崩溃 → `std::terminate`

## 2. IOCP 重写方案

### 2.1 设计原则
- **完成通知驱动**：所有 I/O 操作通过 OVERLAPPED 投递，完成事件由 IOCP 分发
- **零拷贝 accept**：使用 AcceptEx 实现异步 accept，避免同步 accept 的阻塞
- **per-connection 状态机**：每个连接维护 `POSTING_RECV → RECV_COMPLETE → PROCESSING → POSTING_SEND → SEND_COMPLETE` 循环
- **最小改动**：保留现有 Connection 结构和 RESP 协议处理逻辑

### 2.2 新架构概览

```
┌─────────────────────────────────────────────────┐
│                   IOCP Event Loop                │
│  GetQueuedCompletionStatus() ← 阻塞等待完成      │
│                                                  │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐       │
│  │AcceptEx  │  │WSARecv   │  │WSASend   │       │
│  │Complete  │  │Complete  │  │Complete  │       │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘       │
│       │              │              │             │
│       ▼              ▼              ▼             │
│  创建连接      处理请求       发送响应             │
│  投递 Recv     解析 RESP      投递下一次 Recv      │
└─────────────────────────────────────────────────┘
```

### 2.3 核心数据结构

```cpp
// IOCP 操作类型
enum class IOCPOp { kAccept, kRecv, kSend };

// per-connection 状态
struct Connection {
    SOCKET raw_sock;
    IOCPOp current_op;
    IocpOverlapped recv_ol;   // RECV 的 OVERLAPPED
    IocpOverlapped send_ol;   // SEND 的 OVERLAPPED
    std::string recv_buf;
    std::string send_buf;
    bool closed;
    // ... 其他字段
};

// per-listening-socket 状态
struct ListenContext {
    SOCKET raw_sock;
    IocpOverlapped accept_ol;  // AcceptEx 的 OVERLAPPED
    char accept_buf[2 * (sizeof(SOCKADDR_IN) + 16)];  // AcceptEx 缓冲区
};
```

### 2.4 连接生命周期（IOCP 版）

```
AcceptEx 完成
    │
    ▼
GetQueuedCompletionStatus(kAccept)
    │
    ├──▶ 创建 Connection
    ├──▶ 启用新 socket (Setsockopt)
    ├──▶ 关联 IOCP (CreateIoCompletionPort)
    ├──▶ 投递 WSARecv (kRecv)
    └──▶ 重新投递 AcceptEx (监听新连接)

WSARecv 完成
    │
    ▼
GetQueuedCompletionStatus(kRecv)
    │
    ├──▶ 数据已在 recv_buf
    ├──▶ 解析 RESP 命令
    ├──▶ 执行命令，生成响应
    ├──▶ 投递 WSASend (kSend)
    └──▶ 投递下一次 WSARecv (kRecv)

WSASend 完成
    │
    ▼
GetQueuedCompletionStatus(kSend)
    │
    ├──▶ 检查是否还有待发送数据
    ├──▶ 有 → 投递下一次 WSASend (kSend)
    └──▶ 无 → 投递 WSARecv (kRecv)
```

## 3. 重写步骤和文件影响范围

### 3.1 Phase 1: 清理现有 IOCP 代码（预计 0.5 天）
**文件**: `server.cpp`

操作：
- 删除所有 `#ifdef _WIN32` 的 select/epoll 占位代码
- 删除 `add_event`/`mod_event`/`del_event` 的 Windows 空实现
- 删除 `accept_loop` 线程（将用 AcceptEx 替代）
- 删除旧的 `handle_completion`/`do_recv`/`do_send`/`post_recv`/`post_send`
- 删除 `process_recv_buf`（将内联到完成处理中）

### 3.2 Phase 2: 实现 AcceptEx 异步 accept（预计 1 天）
**文件**: `server.cpp`

操作：
- 实现 `PostAccept()`：投递 AcceptEx 到监听 socket
- 实现 `AcceptComplete()`：AcceptEx 完成后
  - 获取新 socket 的 sockaddr
  - 调用 `Setsockopt(SO_UPDATE_ACCEPT_CONTEXT)`
  - 关联新 socket 到 IOCP
  - 投递初始 WSARecv
  - 重新投递 AcceptEx
- 实现 `ListenContext` 结构管理监听 socket 状态

### 3.3 Phase 3: 实现 WSARecv/WSASend 完成处理（预计 1 天）
**文件**: `server.cpp`

操作：
- 实现 `PostRecv(Connection&)`：投递 WSARecv
- 实现 `RecvComplete(Connection&, DWORD bytes)`：
  - 数据已在 recv_buf
  - 调用 `process_tcp(conn)` 或 `process_http(conn)`
  - 如果有响应数据，投递 WSASend
  - 如果无待处理工作，投递下一次 WSARecv
- 实现 `PostSend(Connection&)`：投递 WSASend
- 实现 `SendComplete(Connection&, DWORD bytes)`：
  - 检查是否还有待发送数据
  - 有 → 投递下一次 WSASend
  - 无 → 投递 WSARecv

### 3.4 Phase 4: 重写主事件循环（预计 0.5 天）
**文件**: `server.cpp`

操作：
- 重写 `Run()` 中的事件循环
- 单一线程 `GetQueuedCompletionStatus` 循环
- 根据 `completion_key` 和 `OVERLAPPED.op` 分发到对应处理函数
- 移除所有 `#ifdef _WIN32` 条件编译（Windows 专用路径）

### 3.5 Phase 5: 修复 Connection 生命周期管理（预计 0.5 天）
**文件**: `server.cpp`, `platform.h`

操作：
- Connection 中存储原始 `SOCKET`（不再用 int fd 做 key）
- 使用 `SOCKET` 作为 `connections_` 的 key（或用 `ULONG_PTR` completion_key）
- `close_connection` 中正确调用 `CancelIoEx` + `closesocket`
- 修复 `platform.h` 中的 `socket_close`（Windows 用 `closesocket`）

### 3.6 Phase 6: 修复平台兼容层（预计 0.5 天）
**文件**: `platform.h`

操作：
- `socket_t` 统一为 `SOCKET`（Windows）/ `int`（POSIX）
- `socket_close` 统一为 `closesocket`（Windows）/ `close`（POSIX）
- `socket_set_nonblocking` 用 `ioctlsocket`（Windows）/ `fcntl`（POSIX）
- 移除 `#define open _open` 等不安全的宏映射

### 3.7 Phase 7: 测试验证（预计 1 天）
**文件**: `tests/server_test.cpp`, `tests/bench_cpp.cpp`, `tests/stress_cpp.cpp`

操作：
- 单元测试：ServerTest PING/PONG
- 基准测试：bench_cpp SET/GET 吞吐量
- 压力测试：stress_cpp 10线程 × 1000 ops
- 并发测试：100+ 连接同时连接

## 4. 预估工作量

| Phase | 内容 | 预计时间 |
|-------|------|----------|
| Phase 1 | 清理旧代码 | 0.5 天 |
| Phase 2 | AcceptEx 异步 accept | 1 天 |
| Phase 3 | WSARecv/WSASend 完成处理 | 1 天 |
| Phase 4 | 重写主事件循环 | 0.5 天 |
| Phase 5 | Connection 生命周期 | 0.5 天 |
| Phase 6 | 平台兼容层修复 | 0.5 天 |
| Phase 7 | 测试验证 | 1 天 |
| **合计** | | **5 天** |

## 5. 风险和缓解措施

| 风险 | 缓解措施 |
|------|----------|
| AcceptEx API 复杂度高 | 使用 `GetAcceptExSockaddrs` 简化地址解析 |
| IOCP 句柄泄漏 | RAII 包装 IOCP 句柄，析构时自动关闭 |
| 连接状态机死锁 | 每个连接用原子标志防止并发投递 |
| 性能不如 epoll | IOCP 在高并发下性能优于 epoll，无需担心 |
| 回归测试覆盖不足 | 每个 Phase 完成后运行全量测试 |
