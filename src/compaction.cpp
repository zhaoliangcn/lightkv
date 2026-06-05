#include "lightkv/compaction.h"
#include "lightkv/table_builder.h"
#include <algorithm>
#include <cinttypes>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

namespace lightkv {

CompactionWorker::CompactionWorker(const Options& options, const std::string& db_path,
                                   uint64_t* next_file_id)
    : options_(options), db_path_(db_path), next_file_id_(next_file_id) {}

Status CompactionWorker::DoCompaction(const std::vector<std::shared_ptr<SSTable>>& inputs,
                                      int output_level,
                                      std::vector<uint64_t>* new_file_ids) {
    if (inputs.empty()) return Status::OK();

    edit_.Clear();

    // Collect iterators from all input files
    struct IterSource {
        std::shared_ptr<SSTable> table;
        std::unique_ptr<SSTable::Iterator> iter;
        bool Valid() const { return iter && iter->Valid(); }
    };

    std::vector<IterSource> sources;
    sources.reserve(inputs.size());
    for (auto& table : inputs) {
        auto iter = std::make_unique<SSTable::Iterator>(table.get());
        sources.push_back({table, std::move(iter)});
    }

    // Helper to find the minimum key among valid sources
    auto find_minimum = [&sources](int* best_idx) -> bool {
        *best_idx = -1;
        for (int i = 0; i < static_cast<int>(sources.size()); ++i) {
            if (!sources[i].Valid()) continue;
            if (*best_idx < 0) {
                *best_idx = i;
                continue;
            }
            // Compare (key, seq): prefer smaller key, then larger seq
            int cmp = sources[i].iter->key().compare(sources[*best_idx].iter->key());
            if (cmp < 0 || (cmp == 0 && sources[i].iter->seq() > sources[*best_idx].iter->seq())) {
                *best_idx = i;
            }
        }
        return *best_idx >= 0;
    };

    // Helper to advance all sources past a given key
    auto advance_past_key = [&sources](const Slice& key) {
        for (auto& src : sources) {
            while (src.Valid() && src.iter->key() == key) {
                src.iter->Next();
            }
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

    while (find_minimum(&best_idx)) {
        const auto& src = sources[best_idx];
        std::string cur_key = src.iter->key().ToString();  // Copy key to avoid dangling pointer after SwitchToBlock

        // Skip if same as last written key (keep newest)
        if (!last_key.empty() && cur_key == last_key) {
            advance_past_key(cur_key);
            continue;
        }

        // Write to output
        builder->Add(cur_key, src.iter->value());
        last_key = cur_key;
        ++output_count;

        // Advance all sources past this key
        advance_past_key(cur_key);

        // Check if output file is large enough
        if (builder->FileSize() >= target_file_size) {
            start_new_output();
        }
    }

    finish_current_output();

    // Mark all input files as removed
    for (auto& src : sources) {
        edit_.RemoveFile(0, src.table->file_id());  // level 0 for all input files
    }

    return Status::OK();
}

} // namespace lightkv