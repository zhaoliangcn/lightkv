#include "lightkv/compaction.h"
#include "lightkv/table_builder.h"
#include <algorithm>
#include <cinttypes>
#include <memory>
#include <queue>
#include <sstream>
#include <string>
#include <vector>

namespace lightkv {

CompactionWorker::CompactionWorker(const Options& options, const std::string& db_path,
                                   uint64_t* next_file_id)
    : options_(options), db_path_(db_path), next_file_id_(next_file_id) {}

Status CompactionWorker::DoCompaction(const std::vector<InputFile>& inputs,
                                      int output_level,
                                      std::vector<uint64_t>* new_file_ids) {
    if (inputs.empty()) return Status::OK();

    edit_.Clear();

    // Collect iterators from all input files
    struct IterSource {
        int level;
        std::shared_ptr<SSTable> table;
        std::unique_ptr<SSTable::Iterator> iter;
        bool Valid() const { return iter && iter->Valid(); }
    };

    std::vector<IterSource> sources;
    sources.reserve(inputs.size());
    for (auto& input : inputs) {
        auto iter = std::make_unique<SSTable::Iterator>(input.table);
        sources.push_back({input.level, input.table, std::move(iter)});
    }

    // Use a priority queue for k-way merge (O(log N) per key instead of O(N))
    struct HeapEntry {
        std::string key;
        std::string value;
        uint64_t seq;
        int source_idx;

        bool operator>(const HeapEntry& other) const {
            int cmp = key.compare(other.key);
            if (cmp != 0) return cmp > 0;
            return seq < other.seq;  // prefer larger seq (newer version)
        }
    };

    std::priority_queue<HeapEntry, std::vector<HeapEntry>, std::greater<HeapEntry>> heap;

    // Initialize heap with first entry from each source
    for (int i = 0; i < static_cast<int>(sources.size()); ++i) {
        if (sources[i].Valid()) {
            heap.push({sources[i].iter->key().ToString(),
                       sources[i].iter->value().ToString(),
                       sources[i].iter->seq(), i});
        }
    }

    auto advance_source = [&sources, &heap](int idx) {
        sources[idx].iter->Next();
        if (sources[idx].Valid()) {
            heap.push({sources[idx].iter->key().ToString(),
                       sources[idx].iter->value().ToString(),
                       sources[idx].iter->seq(), idx});
        }
    };

    // Compute output file size target
    // For L0->L1: target = memtable_size * level_multiplier (rough L1 target)
    // For Ln->Ln+1: target = memtable_size * level_multiplier^n / ~10 files per level
    uint64_t base_size = static_cast<uint64_t>(options_.memtable_size) * options_.level_multiplier;
    uint64_t target_file_size = base_size;
    if (output_level > 1) {
        for (int i = 1; i < output_level; ++i) {
            target_file_size *= options_.level_multiplier;
        }
        target_file_size /= 10;  // ~10 files per level
    }
    if (target_file_size < 2 * 1024 * 1024) {
        target_file_size = 2 * 1024 * 1024;  // minimum 2MB
    }

    // Seek all iterators to first
    for (auto& src : sources) {
        src.iter->SeekToFirst();
    }

    // Start merging
    int best_idx;
    std::string current_output_path;
    std::unique_ptr<TableBuilder> builder;
    std::string last_key;
    uint64_t last_seq_written = 0;
    uint64_t output_count = 0;
    uint64_t file_id = 0;

    auto finish_current_output = [&]() {
        if (!builder) return;
        builder->Finish();
        // Mark added file
        uint64_t added_id = file_id;
        // Re-open to get key range
        auto new_table = std::make_shared<SSTable>(options_, current_output_path, added_id);
        if (new_table->Open().ok()) {
            edit_.AddFile(output_level, added_id, new_table->SmallestKey(),
                          new_table->LargestKey(), new_table->FileSize());
        }
        builder.reset();
    };

    auto start_new_output = [&]() {
        finish_current_output();
        file_id = (*next_file_id_)++;
        std::stringstream ss;
        ss << db_path_ << "/" << file_id << ".sst";
        current_output_path = ss.str();
        builder = std::make_unique<TableBuilder>(options_, current_output_path);
        new_file_ids->push_back(file_id);
        last_key.clear();
    };

    start_new_output();

    // Periodically refresh oldest_snapshot to avoid retaining stale versions
    static const int kSnapshotRefreshInterval = 1000;
    int entries_since_refresh = 0;

    while (!heap.empty()) {
        HeapEntry entry = std::move(const_cast<HeapEntry&>(heap.top()));
        heap.pop();

        // Periodically refresh oldest snapshot from provider
        if (snapshot_provider_ && ++entries_since_refresh >= kSnapshotRefreshInterval) {
            oldest_snapshot_ = snapshot_provider_();
            entries_since_refresh = 0;
        }

        // Handle duplicate keys: keep versions visible to any active snapshot
        if (!last_key.empty() && entry.key == last_key) {
            // This is an older version of the same key
            // Keep it only if it's visible to some active snapshot
            if (entry.seq <= oldest_snapshot_) {
                // This version is too old and no snapshot needs it, skip it
                int src_idx = entry.source_idx;
                while (sources[src_idx].Valid() && sources[src_idx].iter->key() == last_key) {
                    sources[src_idx].iter->Next();
                }
                if (sources[src_idx].Valid()) {
                    heap.push({sources[src_idx].iter->key().ToString(),
                               sources[src_idx].iter->value().ToString(),
                               sources[src_idx].iter->seq(), src_idx});
                }
                continue;
            }
            // Otherwise, this version might be needed by a snapshot, write it
        }

        // Write to output
        builder->Add(entry.key, entry.value);
        last_key = entry.key;
        last_seq_written = entry.seq;
        ++output_count;

        // Advance this source
        advance_source(entry.source_idx);

        // Check if output file is large enough
        if (builder->FileSize() >= target_file_size) {
            start_new_output();
        }
    }

    finish_current_output();

    // Mark all input files as removed with their correct source levels
    for (auto& src : sources) {
        edit_.RemoveFile(src.level, src.table->file_id());
    }

    return Status::OK();
}

} // namespace lightkv