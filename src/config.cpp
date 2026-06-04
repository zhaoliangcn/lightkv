#include "lightkv/config.h"
#include <fstream>
#include <sstream>
#include <string>
#include <unordered_map>

namespace lightkv {

// Simple JSON parser for flat key-value objects
static std::unordered_map<std::string, std::string> ParseJson(const std::string& json) {
    std::unordered_map<std::string, std::string> result;
    size_t pos = 0;

    // Skip whitespace
    auto skip_ws = [&]() {
        while (pos < json.size() && (json[pos] == ' ' || json[pos] == '\t' || json[pos] == '\n' || json[pos] == '\r'))
            ++pos;
    };

    // Skip opening brace
    skip_ws();
    if (pos >= json.size() || json[pos] != '{') return result;
    ++pos;

    while (pos < json.size()) {
        skip_ws();
        if (pos >= json.size()) break;
        if (json[pos] == '}') break;

        // Parse key
        if (json[pos] != '"') break;
        ++pos;
        size_t key_start = pos;
        while (pos < json.size() && json[pos] != '"') ++pos;
        std::string key = json.substr(key_start, pos - key_start);
        if (pos < json.size()) ++pos; // skip closing quote

        // Skip colon
        skip_ws();
        if (pos < json.size() && json[pos] == ':') ++pos;

        // Parse value
        skip_ws();
        std::string value;
        if (pos < json.size() && json[pos] == '"') {
            // String value
            ++pos;
            size_t val_start = pos;
            while (pos < json.size() && json[pos] != '"') ++pos;
            value = json.substr(val_start, pos - val_start);
            if (pos < json.size()) ++pos;
        } else {
            // Number or boolean
            size_t val_start = pos;
            while (pos < json.size() && json[pos] != ',' && json[pos] != '}' && json[pos] != ' ' && json[pos] != '\n' && json[pos] != '\t')
                ++pos;
            value = json.substr(val_start, pos - val_start);
        }

        result[key] = value;

        // Skip comma
        skip_ws();
        if (pos < json.size() && json[pos] == ',') ++pos;
    }

    return result;
}

Status LoadOptionsFromFile(const std::string& config_path, Options* options) {
    std::ifstream file(config_path);
    if (!file.is_open()) {
        return Status::IOError("cannot open config file: " + config_path);
    }

    std::stringstream ss;
    ss << file.rdbuf();
    std::string content = ss.str();

    auto kv = ParseJson(content);

    if (kv.count("db_path")) options->db_path = kv["db_path"];
    if (kv.count("memtable_size_mb")) options->memtable_size = std::stoull(kv["memtable_size_mb"]) << 20;
    if (kv.count("block_cache_size_mb")) options->block_cache_size = std::stoull(kv["block_cache_size_mb"]) << 20;
    if (kv.count("block_size_kb")) options->block_size = std::stoull(kv["block_size_kb"]) << 10;
    if (kv.count("max_level")) options->max_level = std::stoi(kv["max_level"]);
    if (kv.count("l0_file_num_trigger")) options->l0_file_num_trigger = std::stoi(kv["l0_file_num_trigger"]);
    if (kv.count("level_multiplier")) options->level_multiplier = std::stoi(kv["level_multiplier"]);
    if (kv.count("bloom_bits_per_key")) options->bloom_bits_per_key = std::stoi(kv["bloom_bits_per_key"]);
    if (kv.count("sync_writes")) {
        // sync_writes is a WriteOptions field, not Options
        // We store it as a hint in db_path for now (or ignore)
        (void)kv["sync_writes"];
    }
    if (kv.count("paranoid_checks")) options->paranoid_checks = (kv["paranoid_checks"] == "true");
    if (kv.count("create_if_missing")) options->create_if_missing = (kv["create_if_missing"] == "true");

    if (kv.count("compression")) {
        if (kv["compression"] == "lz4") {
            options->compression = CompressionType::kLZ4Compression;
        } else {
            options->compression = CompressionType::kNoCompression;
        }
    }

    return Status::OK();
}

} // namespace lightkv
