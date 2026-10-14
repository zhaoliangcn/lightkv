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
    uint32_t format_version = 2;
    uint64_t last_seq = 0;
    uint64_t flushed_seq = 0;
    uint64_t next_file_id = 1;

    // Files organized by level
    std::vector<FileMetaData> files[7];

    // Rewrite the whole manifest as a fresh snapshot (used at creation, after
    // the append log grows past kMaxAppendBytes, and for backups).
    Status WriteToFile(const std::string& db_path) const;

    // Persist only what changed since the last call: append a framed record per
    // added/removed file plus the sequence counters, then fsync. Rewriting the
    // entire file on every flush made the write cost O(total SSTables), which is
    // exactly the regime where flushes are frequent.
    Status AppendChanges(const std::string& db_path);

    Status ReadFromFile(const std::string& db_path);

    void AddFile(int level, uint64_t file_id,
                 const std::string& smallest, const std::string& largest,
                 uint64_t file_size);
    void RemoveFile(int level, uint64_t file_id);

    // Number of append records written since the last full snapshot.
    size_t appended_records() const { return appended_records_; }

private:
    void RecordAdd(int level, uint64_t file_id,
                   const std::string& smallest, const std::string& largest,
                   uint64_t file_size);
    void RecordRemove(int level, uint64_t file_id);
    void RemoveFileDirect(int level, uint64_t file_id);
    // Replay the framed append log that follows the snapshot. Returns false on
    // a malformed record. Takes the raw bytes so the cursor type stays internal.
    bool ReplayLog(const std::string& log);

    std::vector<FileMetaData> pending_adds_[7];
    std::vector<std::pair<int, uint64_t>> pending_removals_;
    size_t appended_records_ = 0;
    size_t replayed_records_ = 0;
};

} // namespace lightkv
