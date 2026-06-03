#include "lightkv/compaction.h"

namespace lightkv {

CompactionWorker::CompactionWorker(const Options& options)
    : options_(options) {}

void CompactionWorker::ScheduleCompaction(int level,
                                          std::vector<std::shared_ptr<SSTable>> level_files,
                                          std::vector<std::shared_ptr<SSTable>> next_level_files) {
    auto* compaction = new Compaction(level);
    compaction->inputs_[0] = std::move(level_files);
    compaction->inputs_[1] = std::move(next_level_files);
    DoCompaction(compaction);
    delete compaction;
}

void CompactionWorker::DoCompaction(Compaction* compaction) {
    if (compaction->inputs_[0].empty()) return;

    // Merge the files into new SSTables at the next level
    // This is a simplified implementation that just re-writes everything

    // For simplicity, we just note that compaction occurred.
    // The actual merge would involve merging iterators from all input files.
}

} // namespace lightkv