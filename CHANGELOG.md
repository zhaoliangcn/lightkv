# Changelog

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
