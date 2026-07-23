#pragma once

#include "slice.h"
#include "status.h"
#include <cstdint>
#include <string>
#include <memory>
#include <mutex>

namespace lightkv {

// vlog（Value Log）— 大 Value 分离存储的独立追加日志
//
// 设计目标：
// - Value 超过阈值（默认 64KB）时，写入 vlog 而非 SSTable
// - SSTable 中只存指针 (vlog_offset, vlog_length)，compaction 只搬指针
// - vlog 文件可增长，达到上限后滚动到新文件
// - GC 由上层触发（详见设计草案 7.5）
//
// 线程安全：所有公开方法持 mutex_
class VLog {
public:
    // vlog 文件路径形如 {db_path}/vlog-{id}.log
    explicit VLog(const std::string& db_path, uint64_t file_id, size_t initial_size = 64 * 1024 * 1024);
    ~VLog();

    // 打开/创建 vlog 文件（mmap + O_RDWR | O_CREAT）
    Status Open();

    // 追加一个 value，返回其在 vlog 中的偏移与长度
    // 自动扩容（ GrowFile ）当空间不足
    Status Append(const Slice& value, uint64_t* out_offset, uint64_t* out_length);

    // 从指定偏移读取 length 字节
    Status Read(uint64_t offset, uint64_t length, std::string* out) const;

    // 当前写入位置（文件有效长度）
    uint64_t Size() const { return write_pos_; }

    // 文件 ID（用于 manifest 的 indirection layer）
    uint64_t file_id() const { return file_id_; }

    // Sync 落盘
    Status Sync();

    // 关闭文件
    void Close();

private:
    Status GrowFile();

    std::string path_;
    uint64_t file_id_;
    int fd_;
    void* mmap_base_;
    size_t file_size_;   // mmap 映射的总大小
    uint64_t write_pos_; // 已写入位置
    mutable std::mutex mu_;
};

// VLogManager — 管理 db_path 下所有 vlog 文件
//
// 职责：
// - 维护活动 vlog（当前追加目标）
// - 启动时扫描已有 vlog 文件，恢复 file_id 序号
// - GC 触发接口（Phase 1 仅留占位，Phase 2 实装）
class VLogManager {
public:
    explicit VLogManager(const std::string& db_path, size_t file_size_limit = 512 * 1024 * 1024);
    ~VLogManager();

    // 启动时调用：扫描 db_path 下已有 vlog 文件，记录 next_file_id_
    // 不重新打开旧 vlog（旧 vlog 由 SSTable 通过指针按需读取）
    Status Initialize();

    // 写入一个大 value，返回 (vlog_file_id, vlog_offset, vlog_length) 三元组
    // 当活动 vlog 空间不足时滚动到新文件
    Status Append(const Slice& value,
                  uint64_t* out_file_id,
                  uint64_t* out_offset,
                  uint64_t* out_length);

    // 从指定 (file_id, offset, length) 读取 value
    // 按 file_id 打开对应 vlog 文件并缓存（LRU）
    Status Read(uint64_t file_id, uint64_t offset, uint64_t length, std::string* out);

    // Phase 1 仅占位，Phase 2 实装 GC
    // GC 触发：vlog 文件 usage < 50% 时启动
    Status TriggerGC();

    // 关闭所有持有的 vlog 文件
    void Close();

private:
    std::string db_path_;
    size_t file_size_limit_;
    std::mutex mu_;
    std::unique_ptr<VLog> active_;     // 当前追加目标
    uint64_t next_file_id_;

    // 读取用 vlog 文件缓存：file_id → VLog
    // 简化版：无 LRU 淘汰，Phase 2 改造为带容量限制的 cache
    std::unordered_map<uint64_t, std::unique_ptr<VLog>> read_cache_;
};

} // namespace lightkv
