#pragma once

#include "slice.h"
#include "status.h"
#include <cstdint>
#include <string>
#include <vector>

namespace lightkv {

struct FileMetaData {
    uint64_t file_id;
    int level;
    std::string smallest_key;
    std::string largest_key;
    uint64_t file_size;
};

struct Manifest {
    uint32_t format_version = 1;
    uint64_t last_seq = 0;
    uint64_t flushed_seq = 0;
    uint64_t next_file_id = 1;

    // Files organized by level
    std::vector<FileMetaData> files[7];

    Status WriteToFile(const std::string& db_path) const;
    Status ReadFromFile(const std::string& db_path);

    void AddFile(int level, uint64_t file_id,
                 const std::string& smallest, const std::string& largest,
                 uint64_t file_size);
    void RemoveFile(int level, uint64_t file_id);
};

} // namespace lightkv
