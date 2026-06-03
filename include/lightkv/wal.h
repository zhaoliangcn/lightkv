#pragma once

#include "slice.h"
#include "status.h"
#include "options.h"
#include <cstdint>
#include <string>
#include <memory>

namespace lightkv {

struct WALRecord {
    enum Type : uint8_t {
        kTypeValue = 1,
        kTypeDeletion = 2,
    };

    uint64_t seq;
    Type type;
    std::string key;
    std::string value;
};

class WALWriter {
public:
    explicit WALWriter(const std::string& filename);
    ~WALWriter();

    Status Open();

    Status Append(uint64_t seq, WALRecord::Type type, const Slice& key, const Slice& value);

    Status Sync();

    uint64_t Offset() const { return write_pos_; }

    void Close();

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