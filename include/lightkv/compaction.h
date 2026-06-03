#pragma once

#include "sstable.h"
#include "options.h"
#include "slice.h"
#include "status.h"
#include <vector>
#include <memory>
#include <mutex>
#include <string>
#include <cstdint>

namespace lightkv {

// VersionEdit: tracks metadata changes from a compaction
class VersionEdit {
public:
    struct FileMeta {
        int level;
        uint64_t file_id;
        std::string smallest_key;
        std::string largest_key;
        uint64_t file_size;
    };

    void AddFile(int level, uint64_t file_id, const std::string& smallest_key,
                 const std::string& largest_key, uint64_t file_size) {
        added_files_.push_back({level, file_id, smallest_key, largest_key, file_size});
    }

    void RemoveFile(int level, uint64_t file_id) {
        removed_files_.emplace_back(level, file_id);
    }

    const std::vector<FileMeta>& added_files() const { return added_files_; }
    const std::vector<std::pair<int, uint64_t>>& removed_files() const { return removed_files_; }

    void Clear() {
        added_files_.clear();
        removed_files_.clear();
    }

private:
    std::vector<FileMeta> added_files_;
    std::vector<std::pair<int, uint64_t>> removed_files_;
};

// CompactionScore: score > 1.0 means compaction is needed
struct CompactionScore {
    int level = -1;
    double score = 0.0;  // > 1.0 triggers compaction
};

// Compaction: describes one compaction task
class Compaction {
public:
    struct InputFile {
        int level;
        std::shared_ptr<SSTable> table;
    };

    std::vector<InputFile> inputs_;
    int output_level_;

    Compaction(int output_level) : output_level_(output_level) {}
};

// CompactionWorker: performs the actual merge
class CompactionWorker {
public:
    CompactionWorker(const Options& options, const std::string& db_path,
                     uint64_t* next_file_id);

    // Do a compaction: merge input files, write output files
    // Returns the list of new file IDs in new_file_ids
    Status DoCompaction(const std::vector<std::shared_ptr<SSTable>>& inputs,
                        int output_level,
                        std::vector<uint64_t>* new_file_ids);

    const VersionEdit& edit() const { return edit_; }

private:
    Options options_;
    std::string db_path_;
    uint64_t* next_file_id_;
    VersionEdit edit_;
};

} // namespace lightkv