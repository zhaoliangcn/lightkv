#pragma once

#include "sstable.h"
#include "options.h"
#include <vector>
#include <memory>
#include <mutex>
#include <string>

namespace lightkv {

class Compaction {
public:
    // L0 files + one level's files
    std::vector<std::shared_ptr<SSTable>> inputs_[2]; // [0] = level, [1] = level+1
    int level_;

    Compaction(int level) : level_(level) {}

    bool IsTrivialMove() const {
        return inputs_[0].size() == 1 && inputs_[1].empty();
    }
};

class CompactionWorker {
public:
    CompactionWorker(const Options& options);

    void ScheduleCompaction(int level,
                            std::vector<std::shared_ptr<SSTable>> level_files,
                            std::vector<std::shared_ptr<SSTable>> next_level_files);

private:
    Options options_;

    void DoCompaction(Compaction* compaction);
};

} // namespace lightkv