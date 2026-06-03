# LightKV 详细设计文档

## 1. 系统概述

LightKV 是一个基于 **LSM-Tree**（Log-Structured Merge-Tree）的嵌入式键值存储引擎。它将随机写转化为顺序写，通过分层合并（Compaction）维持读性能，适合写密集型场景。

### 核心特性

| 特性 | 实现方式 |
|------|----------|
| 存储引擎 | LSM-Tree（7 层） |
| 内存索引 | 无锁跳表（SkipList）+ Arena 内存池 |
| 持久化 | WAL（mmap）+ SSTable（pwrite） |
| 并发模型 | mutex + atomic + 异步后台线程 |
| 缓存 | LRU Block Cache（默认 512MB） |
| 过滤 | Bloom Filter（10 bits/key） |
| 压缩 | Data Block 前缀压缩（16 条/重启点） |
| MVCC | 单调递增序列号 + 快照读 |

---

## 2. 架构总览

```
                          ┌──────────────────────┐
                          │     DB Interface      │
                          │  Put / Get / Delete   │
                          └──────────┬───────────┘
                                     │
                          ┌──────────▼───────────┐
                          │       DBImpl          │
                          │  ┌─────────────────┐  │
                          │  │   Write Path     │  │
                          │  │ Slice→WAL→Mem    │  │
                          │  └────────┬────────┘  │
                          │           │Flush      │
                          │  ┌────────▼────────┐  │
                          │  │ Immutable Mem   │  │
                          │  └────────┬────────┘  │
                          │           │Background │
                          │  ┌────────▼────────┐  │
                          │  │  TableBuilder    │  │
                          │  │  → L0 SSTable    │  │
                          │  └─────────────────┘  │
                          │  ┌─────────────────┐  │
                          │  │   Read Path      │  │
                          │  │ Mem→Imm→L0...L6  │  │
                          │  └─────────────────┘  │
                          └────────────────────────┘

存储介质层次:
┌─────────────────────────────────────────────────┐
│  MemTable (SkipList + Arena, 内存, 可读写)        │
├─────────────────────────────────────────────────┤
│  Immutable MemTable (只读, 等待 Flush)            │
├─────────────────────────────────────────────────┤
│  WAL (mmap 文件, 崩溃恢复)                        │
├─────────────────────────────────────────────────┤
│  SSTable L0 (可能重叠)                            │
│  SSTable L1 (有序, 10x L0)                       │
│  SSTable L2 (有序, 10x L1)                       │
│  ...                                             │
│  SSTable L6 (有序, 10x L5)                       │
├─────────────────────────────────────────────────┤
│  Block Cache (LRU, 内存)                         │
└─────────────────────────────────────────────────┘
```

---

## 3. 核心数据结构

### 3.1 Slice — 零拷贝字节视图

[slice.h](file:///Users/macmima1234/code/mykvdb/lightkv/include/lightkv/slice.h) 定义轻量级字节切片，不持有内存，仅包含指针和长度：

```cpp
class Slice {
    const char* data_;   // 指向外部内存（不拥有）
    size_t      size_;
public:
    Slice(const char* d, size_t n);     // 从原始指针
    Slice(const std::string& str);      // 从 std::string（零拷贝）
    Slice(std::string_view sv);         // 从 string_view（零拷贝）
    std::string ToString() const;       // 仅在必要时拷贝
};
```

**设计要点**：整个 API 层均使用 `Slice` 传参，避免了 `std::string` 的堆分配开销。写入路径上 key/value 以原始指针形式流经 WAL → MemTable，直到 SkipList 内部才构造 `std::string` 存入 Arena。

### 3.2 SkipList — 无锁跳表

[skiplist.h](file:///Users/macmima1234/code/mykvdb/lightkv/include/lightkv/skiplist.h) 实现概率平衡的多层链表索引。

```
Level 2:  head ──────────────────────→ node7 ──→ NULL
Level 1:  head ──→ node3 ──→ node5 ──→ node7 ──→ NULL
Level 0:  head → node1 → node3 → node5 → node7 → NULL
```

**节点结构**（柔性数组）：

```cpp
template<typename Key, typename Value>
struct Node {
    Key key;
    Value value;
    uint64_t seq;
    int height;
    bool is_deleted;
    std::atomic<Node*> next[1];  // 柔性数组，实际分配 height 个指针
};
```

**高度随机化**：分支因子 4（每层概率 1/4），最大高度 12。期望节点数：1 + 1/4 + 1/16 + ... ≈ 1.33 层/节点。

**插入算法**（`Insert(const Key&, const Value&, uint64_t)`）：

```
1. 从最高层开始，记录每层的前驱节点 prev[height]
2. 在 L0 检查 key 是否已存在
   - 存在且 seq 更大 → 原地更新 value 和 seq（MVCC 覆盖）
   - 不存在 → 分配新节点，随机高度 h
3. 对 i = 0..h-1: node->next[i] = prev[i]->next[i]; prev[i]->next[i] = node
```

**并发安全**：使用 `std::atomic<Node*>` 和 `memory_order_acquire/release`，无锁但非完全 lock-free（存在竞争条件，但 MVCC 保证最终一致性）。

**删除**：`InsertDeletion()` 标记 `is_deleted = true`，不物理删除节点（由 Compaction 清理）。

### 3.3 Arena — 内存池

跳表节点的内存由 `Arena` 统一管理，避免频繁 `new/delete`：

```cpp
class Arena {
    static constexpr size_t kBlockSize = 4096;   // 4KB 块
    struct Block { char data[4096]; size_t used; Block* next; };

    char* Allocate(size_t size) {
        if (size > kBlockSize/2) {
            // 大对象 → 独立分配
        }
        if (current_->used + size > kBlockSize) {
            // 当前块不够 → 分配新块
        }
        // 从当前块中切出 size 字节
    }
};
```

**生命周期**：Arena 内所有内存在 Arena 析构时统一释放，节点无需单独释放。

### 3.4 MemTable — 内存表

[memtable.h](file:///Users/macmima1234/code/mykvdb/lightkv/include/lightkv/memtable.h) 封装 SkipList 为 KV 接口：

```cpp
class MemTable {
    SkipList table_;  // SkipList<std::string, std::string>
public:
    void Insert(uint64_t seq, const Slice& key, const Slice& value);
    void InsertDeletion(uint64_t seq, const Slice& key);
    bool Get(const Slice& key, std::string* value, uint64_t snapshot) const;
    size_t ApproximateMemoryUsage() const;  // 遍历 Arena blocks
};
```

**Get 语义**：
1. `Find(key)` 定位节点
2. 检查 `iter.Valid()` — 键不存在返回 false
3. 检查 `iter.seq() <= snapshot` — MVCC 快照过滤
4. 检查 `!iter.IsDeleted()` — 删除标记过滤
5. 返回 `iter.value()`

### 3.5 WAL — 预写日志

[wal.h](file:///Users/macmima1234/code/mykvdb/lightkv/include/lightkv/wal.h) 使用 mmap 实现顺序追加写。

**记录格式**：

```
┌────────┬──────────┬────────┬───────────┬─────┬───────────┬─────┬────────┐
│ CRC32  │  Length  │  Type  │ KeyLen(V) │ Key │ ValLen(V) │ Val │  Seq   │
│ 4 bytes│  4 bytes │ 1 byte │  1-5 var  │ ... │  1-5 var  │ ... │ 8 bytes│
└────────┴──────────┴────────┴───────────┴─────┴───────────┴─────┴────────┘
```

- **CRC32**：覆盖 Type + KeyLen + Key + ValLen + Val + Seq，保护数据完整性
- **Length**：record_len = 1 + VarInt(KLen) + KLen + VarInt(VLen) + VLen + 8
- **Type**：1 = kTypeValue, 2 = kTypeDeletion
- **VarInt**：变长编码，小值 1 字节，最大值 5 字节（uint32_t）

**mmap 扩容策略**：

```cpp
Status WALWriter::Append(...) {
    if (write_pos_ + total + kBlockSize >= file_size_) {
        GrowFile();  // 文件扩大为 2 倍，重新 mmap
    }
    // 直接写入 mmap 缓冲区（零拷贝）
    char* buf = static_cast<char*>(mmap_base_) + write_pos_;
    // ... 编码 CRC + 记录 ...
    write_pos_ += total;
}
```

**恢复流程**（`WALReader::ReadRecord`）：
1. 循环读取 `CRC(4) + Length(4)`
2. CRC = 0（`kCRC_EOF`）→ 到达日志末尾，停止
3. 校验 CRC32
4. 解析 Type, Key, Value, Seq → 回放至 MemTable

### 3.6 SSTable — 磁盘表

[sstable.h](file:///Users/macmima1234/code/mykvdb/lightkv/include/lightkv/sstable.h) 定义 SSTable 文件格式。

**文件布局**：

```
┌──────────┬──────────┬─────┬───────────┬───────────┬──────────┐
│  Data    │  Data    │ ... │  Bloom    │  Index    │  Footer  │
│  Block 0 │  Block 1 │     │  Filter   │  Block    │  48 bytes│
└──────────┴──────────┴─────┴───────────┴───────────┴──────────┘
```

**Footer 格式**（48 bytes）：

```
┌──────────────────────────┬──────────────────────────┬──────────┐
│  meta_index_block_offset │  index_block_offset      │  Magic   │
│  (8 bytes)               │  (8 bytes)               │ (8 bytes)│
└──────────────────────────┴──────────────────────────┴──────────┘
```

- `meta_index_block_offset`：Bloom Filter 在文件中的偏移
- `index_block_offset`：Index Block 在文件中的偏移

**读取流程**（`SSTable::Get`）：
1. **Bloom Filter**：`MayMatch(key)` 快速排除不存在的键
2. **Index Block**：通过 Index Block 二分查找定位目标 Data Block
   - Index Block 条目：`块内最后 Key → BlockHandle{offset, size}`
3. **Data Block 内查找**：加载目标 Data Block（优先 LRU Cache），在块内二分+线性扫描定位
4. **返回**：找到则返回 value + seq

### 3.7 Data Block — 前缀压缩数据块

[block.h](file:///Users/macmima1234/code/mykvdb/lightkv/include/lightkv/block.h)、[block_builder.h](file:///Users/macmima1234/code/mykvdb/lightkv/include/lightkv/block_builder.h) 定义 Data Block 的编码格式。

**块内条目编码**：

```
每条记录: ┌──────────────┬───────────────┬───────────┬──────────────┬─────────┐
          │ shared_len   │ non_shared_len│ value_len │ key_suffix   │  value  │
          │ (VarInt32)   │ (VarInt32)    │(VarInt32) │ (non_shared) │         │
          └──────────────┴───────────────┴───────────┴──────────────┴─────────┘
```

- `shared_len`：与前一条 key 的公共前缀长度
- `non_shared_len`：key 差异部分长度
- `value_len`：value 长度
- `key_suffix`：差异部分字节
- `value`：完整 value

**示例**：
```
key1: "user:00001"  → shared=0, non_shared=10, suffix="user:00001"
key2: "user:00002"  → shared=5, non_shared=5,  suffix="00002"
key3: "user:00100"  → shared=5, non_shared=5,  suffix="00100"
```
前缀压缩比约 50%，连续有序 key 压缩效果显著。

**重启点（Restart Points）**：

```
Block 末尾:
┌──────────────────┬─────┬──────────┬──────────────┐
│ KV Entries ...   │ R0  │ R1 ... Rn │ num_restarts │
│                  │ 4B  │   4B      │    4B        │
└──────────────────┴─────┴──────────┴──────────────┘
```

- 每 `restart_interval`（默认 16）条记录设置一个重启点
- 重启点处的记录 `shared_len = 0`（完整 key）
- 查找时先二分定位最近的重启点，再线性扫描最多 16 条

### 3.8 Bloom Filter — 布隆过滤器

[bloom_filter.h](file:///Users/macmima1234/code/mykvdb/lightkv/include/lightkv/bloom_filter.h) 实现双重哈希布隆过滤器，默认 10 bits/key。

- `Add(key)`：计算两个哈希值，设置 k 个位
- `MayMatch(key)`：检查 k 个位是否全部为 1
- 哈希函数数量 `k = bits_per_key * ln(2) ≈ 7`（10 bits/key 时）
- 误报率 ≈ 1%（10 bits/key 时）

### 3.9 Block Cache — LRU 缓存

[cache.h](file:///Users/macmima1234/code/mykvdb/lightkv/include/lightkv/cache.h) 实现基于 `(file_id, block_offset)` 的 LRU 缓存。

- 底层：`std::list` + `std::unordered_map`，O(1) 查找和淘汰
- 默认容量 512MB，可配置
- 线程安全（mutex 保护）

---

## 4. 写入路径详解

### 4.1 Put 流程

`DBImpl::Put(WriteOptions, Slice key, Slice value)` 的执行过程：

```
Put(Slice key, Slice value)
│
├─ 1. 分配序列号
│     seq = last_seq_.fetch_add(1, memory_order_relaxed)
│
├─ 2. 获取互斥锁（保护 mem_/imm_ 切换）
│     lock_guard<mutex> lock(mutex_)
│
├─ 3. 写 WAL（持久化，mmap 零拷贝）
│     wal_->Append(seq, kTypeValue, key, value)
│     ├── 计算 CRC32（Type + KeyLen + Key + ValLen + Val + Seq）
│     ├── 检查 mmap 文件空间（不足则 2x 扩容重新 mmap）
│     └── 直接写入 mmap 缓冲区
│
├─ 4. 写 MemTable（内存索引）
│     mem_->Insert(seq, key, value)
│     └── SkipList::Insert()
│         ├── 从最高层向下搜索插入位置
│         ├── 同 key 且 seq 更大 → 原地更新
│         └── 新 key → Arena 分配 Node，链入各层
│
├─ 5. Sync（可选）
│     if (options.sync) wal_->Sync()  // msync 刷盘
│
└─ 6. 触发 Flush（节流检查，每 1024 次写入）
      if ((write_count_ & 1023) == 0 &&
          mem_->ApproximateMemoryUsage() > memtable_size)
          TriggerFlush()
```

**关键性能优化点**：
- key/value 以 `Slice` 传递，零拷贝直到 SkipList 内部
- WAL 直接在 mmap 缓冲区编码，无中间 `std::string`
- `ApproximateMemoryUsage()` 被节流到每 1024 次写入才检查一次

### 4.2 Flush 流程

当 MemTable 大小超过阈值（64MB），触发 `FlushMemTable()`：

```
FlushMemTable()
│
├─ 1. 切换 MemTable
│     imm_ = mem_           // 当前 MemTable 变为不可变
│     mem_ = new MemTable() // 创建新的活跃 MemTable
│     has_imm_ = true
│
├─ 2. 通知后台线程
│     bg_cv_.notify_one()
│
└─ 后台线程执行:
      WriteLevel0Table(imm, &file_id)
      ├── TableBuilder::Add(key, value) × N
      │   ├── BloomFilter::Add(key)
      │   ├── BlockBuilder::Add(key, value)   // 前缀压缩
      │   └── 块满 (≥4KB) → FlushDataBlock()  // 写入磁盘
      ├── TableBuilder::Finish()
      │   ├── 写入最后的数据块
      │   ├── 写入 Bloom Filter
      │   ├── 写入 Index Block
      │   └── 写入 Footer
      └── 将新 SSTable 加入 levels_[0]
      imm_.reset()
      has_imm_ = false
```

### 4.3 Delete 流程

删除操作写入一个 `kTypeDeletion` 类型的记录，不物理删除数据：

```
Delete(Slice key)
│
├─ seq = last_seq_.fetch_add(1)
├─ wal_->Append(seq, kTypeDeletion, key, Slice())
├─ mem_->InsertDeletion(seq, key)  // is_deleted = true
└─ （同样节流检查 Flush）
```

读取时遇到 `is_deleted = true` 的节点，返回"不存在"。

---

## 5. 读取路径详解

`DBImpl::Get(ReadOptions, Slice key, string* value)` 的执行过程：

```
Get(Slice key)
│
├─ 1. 获取快照序列号
│     snapshot = last_seq_.load(memory_order_acquire)
│
├─ 2. 查找 MemTable（持锁）
│     lock_guard<mutex> lock(mutex_)
│     mem_->Get(key, value, snapshot)
│     ├── SkipList::Find(key) → Iterator
│     ├── 检查 iter.Valid()
│     ├── 检查 iter.seq() <= snapshot（MVCC）
│     └── 检查 !iter.IsDeleted()
│     → 命中则返回 OK
│
├─ 3. 查找 Immutable MemTable
│     imm_->Get(key, value, snapshot)
│     → 命中则返回 OK
│     → 释放锁
│
├─ 4. 逐层查找 SSTable
│     for level = 0..6:
│         for each sstable in levels_[level]:
│             SearchSSTable(level, key, value, snapshot)
│             ├── SSTable::Get(key, value, seq)
│             │   ├── BloomFilter::MayMatch(key) → 否: 跳过
│             │   ├── Index Block 二分查找 → 定位 Data Block
│             │   ├── ReadBlock(handle) → 优先 LRU Cache
│             │   └── Data Block 内二分+线性扫描 → 找到 key
│             └── 检查 seq <= snapshot
│             → 命中则返回 OK
│
└─ 5. 未找到
      return NotFound()
```

**查找优先级**：MemTable > Immutable MemTable > L0 SSTable > L1 ... > L6
**相同 key**：序列号更大的（更新的）在前，MVCC 快照读保证一致性。

---

## 6. Compaction（合并压缩）

### 6.1 触发条件

- **L0 Compaction**：`levels_[0].size() >= l0_file_num_trigger`（默认 4）
- **Ln Compaction**：`levels_[n] 总大小 >= level_multiplier^n * 基准大小`（默认 10^n）

### 6.2 Compaction 流程（简化版）

```
DoCompaction(level)
│
├─ 选取 Ln 中与下层 key 范围重叠最小的文件
├─ 打开所有相关 SSTable
├─ 多路归并排序（按 key + seq 降序）
│   ├── 相同 key 保留最新版本（最大 seq）
│   ├── 删除标记（is_deleted）丢弃
│   └── 超过 TTL 的旧版本丢弃
├─ 输出到 Ln+1 新 SSTable（TableBuilder）
└─ 原子替换元数据，删除旧文件
```

**当前状态**：Compaction 为简化桩实现，完整版待开发。

---

## 7. 崩溃恢复

LightKV 保证 **Crash-Safe**：崩溃后重启，所有已确认写入的数据不丢失。

### 恢复流程（`DBImpl::Initialize()`）

```
Initialize()
│
├─ 1. 检查/创建数据库目录
│
├─ 2. 创建空 MemTable
│
├─ 3. WAL 恢复
│     if (wal.log 文件存在且非空):
│         WALReader reader(wal_path)
│         while (reader.ReadRecord(&record)):
│             if record.type == kTypeDeletion:
│                 mem_->InsertDeletion(record.seq, record.key)
│             else:
│                 mem_->Insert(record.seq, record.key, record.value)
│             last_seq_ = max(last_seq_, record.seq + 1)
│         警告: 恢复期间不触发 Flush
│
├─ 4. 重新创建 WALWriter（覆盖旧 wal.log）
│
├─ 5. 扫描已有 SSTable 文件（0.sst, 1.sst, ...）
│     ├── 打开并加载 Index Block + Bloom Filter
│     └── 加入 levels_[0]
│
└─ 6. 启动后台线程
```

**WAL 恢复日志格式**：当 MemTable 成功 Flush 为 SSTable 后，可截断 WAL。当前实现未做 WAL 截断（简化版）。

---

## 8. 并发模型

```
┌─────────────────────────────────────────────────────┐
│  Put / Delete / Get (多线程调用)                      │
│  ┌───────────────────────────────────────────────┐  │
│  │  mutex_ (互斥锁)                               │  │
│  │  保护: mem_, imm_, levels_[], 元数据切换        │  │
│  │  粒度: 粗粒度全局锁（简化实现）                  │  │
│  └───────────────────────────────────────────────┘  │
│  ┌───────────────────────────────────────────────┐  │
│  │  last_seq_ (atomic)                            │  │
│  │  保护: 序列号单调递增                           │  │
│  │  粒度: lock-free atomic fetch_add              │  │
│  └───────────────────────────────────────────────┘  │
│  ┌───────────────────────────────────────────────┐  │
│  │  SkipList::Node::next[i] (atomic<Node*>)       │  │
│  │  保护: 跳表层间指针                             │  │
│  │  粒度: per-pointer atomic                     │  │
│  └───────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────┐
│  Background Thread (单线程)                          │
│  ┌───────────────────────────────────────────────┐  │
│  │  while (!shutting_down_):                      │  │
│  │      wait(bg_cv_, lock)                        │  │
│  │      if has_imm_: FlushMemTable()              │  │
│  │      MaybeScheduleCompaction()                 │  │
│  └───────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────┘
```

**锁竞争分析**：
- `mutex_` 保护整个写路径（WAL + MemTable + Flush 触发），当前采用粗粒度锁
- 未来可优化为细粒度锁（读写锁分离）：Get 只需共享读锁，Put/Delete 需排他写锁
- 序列号分配在锁外（atomic），减少临界区长度

---

## 9. 文件组织

### SSTable 命名

L0 文件以递增 ID 命名：`/db_path/0.sst`, `/db_path/1.sst`, ..., `/db_path/N.sst`
- `next_file_id_` 跟踪下一个可用文件 ID
- Flush 后自动递增

### WAL 文件

固定路径：`/db_path/wal.log`
- 恢复阶段：读取旧 wal.log
- 正常运行：WALWriter 打开/创建 wal.log，追加写入

---

## 10. 配置参数

[options.h](file:///Users/macmima1234/code/mykvdb/lightkv/include/lightkv/options.h)

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `memtable_size` | 64MB | MemTable 大小阈值，触发 Flush |
| `wal_file_size` | 256MB | WAL 文件初始大小 |
| `block_cache_size` | 512MB | Block Cache 容量 |
| `block_size` | 4KB | Data Block 大小 |
| `max_level` | 7 | LSM-Tree 层数 |
| `l0_file_num_trigger` | 4 | L0 Compaction 触发阈值 |
| `level_multiplier` | 10 | 层间大小倍数 |
| `bloom_bits_per_key` | 10 | Bloom Filter 每 key 位数 |
| `restart_interval` | 16 | 前缀压缩重启点间隔 |

---

## 11. 关键算法总结

| 算法 | 复杂度 | 适用场景 |
|------|--------|----------|
| SkipList 查找 | O(log N) 期望 | MemTable 读写 |
| SkipList 插入 | O(log N) 期望 | MemTable 写入 |
| WAL 追加 | O(1) | 每次写入 |
| SSTable Get | O(log M) M=块数 | 磁盘读取 |
| Data Block Seek | O(log R + I) R=重启点, I=16 | 块内查找 |
| Bloom Filter | O(k) k=哈希次数 | 快速排除 |
| Block Cache | O(1) | 热数据缓存 |
| Flush | O(N) N=MemTable 条目数 | 后台 |
| Compaction | O(N log N) | 后台 |

---

## 12. 当前限制与未来工作

### 当前实现限制

| 模块 | 限制 |
|------|------|
| Compaction | 简化桩实现，未做真正的多路归并 |
| WAL | 未做日志截断（Flush 后应删除旧 WAL） |
| SSTable | 不支持压缩（snappy/zstd），不支持按 key 范围分文件 |
| 迭代器 | DB 级迭代器占位（`iterator.cpp`） |
| 并发 | 全局互斥锁，高并发下存在瓶颈 |

### 未来扩展

1. **完整 Compaction**：多路归并 + Key 范围分文件 + 写放大优化
2. **SSTable 压缩**：集成 Snappy/LZ4/ZSTD 压缩
3. **WAL 截断**：MemTable Flush 后标记可回收的 WAL 区域
4. **读写锁**：用 shared_mutex 替换 mutex，读多写少场景提升并发
5. **DB 级迭代器**：归并 MemTable + Imm + SSTable 的多源迭代器
6. **Checksum 校验**：SSTable Data Block CRC 校验（paranoid_checks）
7. **Range Delete**：支持按范围批量删除（RangeTombstone）