# Changelog

## [Unreleased]

### 缺陷修复（F-34 ~ F-41）

按「文档声称实现、实际未实现」这一条主线审计引擎与服务端，逐项修复。完整
清单与每条的踩坑记录见 [docs/缺陷修复方案.md](docs/缺陷修复方案.md)。

#### 修复
- **slave 侧复制客户端（F-34）**：`REPLICAOF` 此前只会报「没有 slave 客户端」。
  现在 slave 会主动建连、完成握手、加载全量快照并持续接收增量命令，
  带 ACK、指数退避重连与断点续传。`REPLICAOF` 采用同步握手，连不上就返回
  `-ERR`，不再假装成功。
- **复制 offset 记账（F-35）**：`handle_psync` 先处理再注册，而处理函数靠遍历
  `slaves_` 写 offset —— 新连接此刻还不在表里，查找全部落空，随后被以 0 插入。
  master 会把 backlog 从 0 全量重放到刚加载完快照的副本上；SET 幂等所以看不出来，
  INCR 会被重复累加。
- **全量同步丢 TTL（F-36）**：过期时间以 `\x01_ttl_\x00<key>` 存在库里，而快照
  生成时有一行 `if (key[0] == '\x01') continue;` 把唯一的一份过期时间跳掉了。
  副本上从全量同步拿到的 key 从此永远不会过期。
- **副本只读白名单（F-37）**：`readonly_allowed` 是允许清单却被当成「哪些不许写」
  维护，`STRLEN` / `GETRANGE` / `BITCOUNT` 等纯读命令因未列入而在副本上返回
  `-READONLY` —— 同一个 key 上 `GET` 能读、`STRLEN` 不能。
- **MANIFEST 增量追加（F-38）**：此前每次 flush 都全量重写全部 SSTable 元数据，
  代价 O(总文件数)。改为首次快照 + 追加帧；帧式编码用长度前缀，因为 key 是任意
  字节，含逗号的 key 在旧的 `range: [a, b]` 格式里会把后续字段全部错位。
- **写入侧压缩背压（F-39）**：写入路径此前完全不看 L0，速率超过 compaction 时
  L0 只增不减。新增 slow down / stop 两级阈值。
- **Manifest 折返阈值随文件数伸缩（F-40）**：固定的 512 条记录阈值在文件多时会让
  折返被无限推迟——恰恰是快照最贵的时候最不容易折返。
- **背压等待收敛（F-41）**：限速改用挂钟节流而非每操作次数；等待轮数上限下调。

#### 重构
- **server.cpp 拆分（Phase 4 第一步）**：`Connection` / `ThreadPool` /
  `ReplMaster` / 协议 helpers 各自独立成文件，5545 → 4489 行。

#### 文档
- TODO 中 ZRANK / ZREVRANK「未实现」与「paranoid_checks 仅传参未校验」两条与
  实际不符，已更正；README 补充 MANIFEST 增量追加、背压阈值与复制客户端能力。

## [2.0.0-rc] - 2026-07-23

### Phase 3 — 分布式集群 + 迁移工具

#### 新增
- **Raft 共识引擎**：完整的 Raft 共识算法实现
  - Leader 选举（随机超时、多数派投票）
  - 日志复制（AppendEntries RPC + 快速回退优化）
  - 安全性保证（选举限制、日志匹配）
  - NoOp 日志提交（新 Leader 上任自动提交）
  - 持久化存储（Raft 状态写入文件）
- **Raft 网络层**：基于 TCP 的 Raft RPC 通信
  - AppendEntries RPC 序列化/反序列化
  - RequestVote RPC 序列化/反序列化
  - 节点间连接管理与自动重连
- **集群分片（Cluster Sharding）**
  - CRC16 哈希槽计算（兼容 Redis Cluster）
  - Hash tag 支持（{tag} 内 key 在同一 slot）
  - 16384 Slot 映射管理
  - CLUSTER 命令（KEYSLOT / NODES / SLOTS / INFO / COUNTKEYSINSLOT / GETKEYSINSLOT / MYID）
  - MOVED 重定向支持
- **Redis → LightKV 迁移工具（lightkv_migrate）**
  - 全量迁移（SCAN/KEYS 遍历所有 key）
  - 按前缀迁移（--prefix 过滤）
  - 多数据类型支持（String/Hash/List/Set/ZSet）
  - Pipeline 批量写入优化
  - 进度回调与统计输出
  - 断点续传（去重机制）
- **集群启动参数**
  - `--cluster` 启用 Raft 集群模式
  - `--node-id` 节点 ID
  - `--raft-host` / `--raft-port` Raft 内部通信地址
  - `--raft-peers` 集群节点列表
- **生产环境部署文档**：涵盖架构、集群部署、配置优化、监控告警、容量规划、故障恢复、性能调优、Docker 部署

#### 优化
- Release 编译开启 -O3 -march=native 优化

#### 依赖
- 基于 Phase 1/2 已交付能力：大 Value 分离存储、原子批量提交、Watch 机制、Compaction 限速、RESP3 协议、嵌入式 C API、SDK 连接池

## [1.0.0] - 2024-12

### 新增
- **P0 阶段**：String / Hash / List / Set 核心命令完整实现
- **P1/P2 阶段**：Bitmap / HyperLogLog / Geo / ZSet 高级命令
- **ZRANK / ZREVRANK**：ZSet 排名查询命令
- **LSM-Tree 引擎**：MemTable（无锁跳表）、WAL、SSTable、Compaction
- **多线程 TCP 服务器**：基于 epoll/kqueue 的事件循环，支持多工作线程
- **Redis RESP 协议**：完整兼容，支持 redis-cli 直连
- **HTTP 监控**：实时查看服务器运行状态
- **认证机制**：AUTH 命令 + 密码验证
- **TTL 主动过期**：定时扫描过期键
- **主从复制**：支持 Master-Slave 模式
- **优雅信号处理**：SIGINT/SIGTERM 安全关闭
- **LZ4 压缩**：SSTable Block 级别压缩
- **Block Cache**：LRU 淘汰策略
- **Bloom Filter**：加速 SSTable 键查找
- **MANIFEST**：元数据管理与崩溃恢复
- **WAL**：预写日志，Crash-safe 持久化
- **Range Tombstone**：高效范围删除
- **四语言 SDK**：C++ / Go / Python / Node.js
- **完整测试套件**：P0/P1/P2 命令测试、压力测试、性能基准
- **JSON 配置**：支持从配置文件加载选项

### 优化
- 多线程并发写入，提升 3-5 倍 QPS
- 无锁跳表实现，降低锁竞争
- Block Cache + Bloom Filter 联合加速读路径
- SSTable 前缀压缩，减少存储空间
- Compaction 多路归并，保证有序性

### 修复
- WAL 日志解析边界检查，防止空指针解引用和内存越界
- 跨平台编译兼容性问题
- LZ4 库缺失时编译错误
- 压力测试中发现的并发竞态条件

## [0.9.0] - 2024-11

### 新增
- 基础 LSM-Tree 引擎框架
- 单线程 TCP 服务器
- 基础 String 操作

### 说明
- 此版本为早期原型，不推荐用于生产环境
