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
    //
    // Ordering for the SAME key must put the NEWEST version first. SSTable
    // entries carry a real sequence number (LevelDB-style internal keys), and
    // the file id breaks any residual tie.
    struct HeapEntry {
        std::string key;
        std::string value;
        uint64_t seq;
        uint8_t type;
        uint64_t file_number;   // monotonic; higher == newer
        int source_idx;

        bool operator>(const HeapEntry& other) const {
            int cmp = key.compare(other.key);
            if (cmp != 0) return cmp > 0;
            if (seq != other.seq) return seq < other.seq;   // prefer larger seq
            return file_number < other.file_number;         // then newer file
        }
    };

    std::priority_queue<HeapEntry, std::vector<HeapEntry>, std::greater<HeapEntry>> heap;

    // Seek BEFORE priming the heap. These iterators are freshly constructed and
    // have never been positioned, so reading key()/value()/seq() off them yields
    // whatever the last Read left behind -- the merge then starts from bogus
    // entries and silently drops keys. SeekToFirst below used to run *after*
    // this loop, which meant the seeded entries and the iterators disagreed.
    for (auto& src : sources) {
        src.iter->SeekToFirst();
    }

    // Initialize heap with first entry from each source
    for (int i = 0; i < static_cast<int>(sources.size()); ++i) {
        if (sources[i].Valid()) {
            heap.push({sources[i].iter->key().ToString(),
                       sources[i].iter->value().ToString(),
                       sources[i].iter->seq(),
                       sources[i].iter->type(),
                       sources[i].table->file_id(), i});
        }
    }

    auto advance_source = [&sources, &heap](int idx) {
        sources[idx].iter->Next();
        if (sources[idx].Valid()) {
            heap.push({sources[idx].iter->key().ToString(),
                       sources[idx].iter->value().ToString(),
                       sources[idx].iter->seq(),
                       sources[idx].iter->type(),
                       sources[idx].table->file_id(), idx});
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

        // Duplicate key: this is an older version of the key just written.
        //
        // Keep it only if some active snapshot could still need it. With real
        // sequence numbers this is a meaningful test: a version is droppable
        // once every live snapshot is newer than it.
        if (!last_key.empty() && entry.key == last_key) {
            if (entry.seq <= oldest_snapshot_) {
                // Too old for any live snapshot — drop it.
                int src_idx = entry.source_idx;
                while (sources[src_idx].Valid() && sources[src_idx].iter->key() == last_key) {
                    sources[src_idx].iter->Next();
                }
                if (sources[src_idx].Valid()) {
                    heap.push({sources[src_idx].iter->key().ToString(),
                               sources[src_idx].iter->value().ToString(),
                               sources[src_idx].iter->seq(),
                               sources[src_idx].iter->type(),
                               sources[src_idx].table->file_id(), src_idx});
                }
                continue;
            }
            // Still reachable by a live snapshot: carry it forward.
        }

        // Write to output
        // v2.0: compaction 限速 — 每写一个 entry 前请求配额（详见设计草案 4.2.2）
        if (rate_limiter_) {
            rate_limiter_->Request(entry.key.size() + entry.value.size());
        }
        builder->Add(entry.key, entry.value, entry.seq, entry.type);
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