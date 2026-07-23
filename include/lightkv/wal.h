#pragma once

#include "slice.h"
#include "status.h"
#include "options.h"
#include <cstdint>
#include <string>
#include <memory>
#include <vector>

namespace lightkv {

struct WALRecord {
    enum Type : uint8_t {
        kTypeValue = 1,
        kTypeDeletion = 2,
        kTypeRangeDeletion = 3,
        kTypeBatch = 4,           // v2.0: 原子批量提交
    };

    uint64_t seq;
    Type type;
    std::string key;
    std::string value;
    std::string begin_key;  // For range deletion
    std::string end_key;    // For range deletion

    // v2.0: 批量记录展开后的子操作（仅 kTypeBatch 时填充）
    struct BatchOp {
        Type type;
        std::string key;
        std::string value;
        std::string begin_key;  // 仅 kTypeRangeDeletion
        std::string end_key;    // 仅 kTypeRangeDeletion
    };
    std::vector<BatchOp> batch_ops;  // 仅 kTypeBatch 时非空
};

class WALWriter {
public:
    explicit WALWriter(const std::string& filename);
    ~WALWriter();

    Status Open();

    Status Append(uint64_t seq, WALRecord::Type type, const Slice& key, const Slice& value);

    Status AppendRangeDelete(uint64_t seq, const Slice& begin_key, const Slice& end_key);

    // v2.0: 原子批量追加 — 单次写入包含多个操作，要么全部可见要么全部丢弃
    // 每个操作可以是 kTypeValue / kTypeDeletion / kTypeRangeDeletion
    // 所有操作共用同一个 seq（语义：本次批量是一个原子单元）
    Status AppendBatch(uint64_t seq, const std::vector<WALRecord::BatchOp>& ops);

    Status Sync();

    uint64_t Offset() const { return write_pos_; }

    void Close();

    // Truncate WAL to current write position (removes stale data after restart recovery)
    Status Truncate();

private:
    Status GrowFile();

    std::string filename_;
    int fd_;
    void* mmap_base_;
    size_t file_size_;
    size_t write_pos_;
};

class WALReader {
public:
    explicit WALReader(const std::string& filename);
    ~WALReader();

    Status Open();

    bool ReadRecord(WALRecord* record);

    Status Close();

private:
    std::string filename_;
    int fd_;
    void* mmap_base_;
    size_t file_size_;
    size_t read_pos_;
};

} // namespace lightkv