// RESP encoding, key-prefix construction and the Raft log codec.
//
// These used to be file-static helpers at the top of server.cpp. They are now
// shared (rather than duplicated per translation unit) so that splitting the
// server across files cannot silently give two files different definitions.
#include "server_internal.h"

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>

namespace lightkv {
namespace internal {

const char kTtlMagic = '\x01';
const char HASH_PREFIX[] = "\x02_hash_";
const char LIST_PREFIX[] = "\x03_list_";
const char SET_PREFIX[] = "\x04_set_";
const char ZSET_PREFIX[] = "\x05_zset_";

namespace {

constexpr char TTL_MAGIC = kTtlMagic;
constexpr char TTL_SEP = '\x00';

}  // namespace

// ─── RESP Protocol Helpers ───
std::string resp_ok() { return "+OK\r\n"; }
std::string resp_nil() { return "$-1\r\n"; }
std::string resp_integer(int64_t n) { return ":" + std::to_string(n) + "\r\n"; }
std::string resp_error(const std::string& msg) { return "-" + msg + "\r\n"; }
std::string resp_bulk_string(const std::string& s) {
    return "$" + std::to_string(s.size()) + "\r\n" + s + "\r\n";
}
std::string resp_empty_array() { return "*0\r\n"; }
std::string resp_array(const std::vector<std::string>& arr) {
    std::string r = "*" + std::to_string(arr.size()) + "\r\n";
    for (auto& s : arr) r += resp_bulk_string(s);
    return r;
}
// v2.0: RESP3 map 类型 — %<n>\r\n 后接 n 对 key/value bulk string
// 用于 HELLO 命令返回 server 信息（Redis 7.0 兼容）
std::string resp_map(const std::vector<std::string>& keys,
                     const std::vector<std::string>& values) {
    std::string r = "%" + std::to_string(keys.size()) + "\r\n";
    for (size_t i = 0; i < keys.size(); ++i) {
        r += resp_bulk_string(keys[i]);
        r += resp_bulk_string(values[i]);
    }
    return r;
}

// ─── TTL Metadata key prefix ───
std::string ttl_key(const std::string& key) {
    std::string r;
    r += TTL_MAGIC;
    r += "_ttl_";
    r += TTL_SEP;
    r += key;
    return r;
}

int64_t now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

// ─── Type Prefix Helpers ───
std::string hash_field_key(const std::string& name, const std::string& field) {
    return std::string(HASH_PREFIX) + name + ":" + field;
}
std::string hash_meta_key(const std::string& name) {
    return std::string(HASH_PREFIX) + name + ":__meta__";
}

std::string list_idx_key(const std::string& name, int64_t idx) {
    return std::string(LIST_PREFIX) + name + ":idx:" + std::to_string(idx);
}
std::string list_meta_key(const std::string& name) {
    return std::string(LIST_PREFIX) + name + ":__meta__";
}

std::string set_blob_key(const std::string& name) {
    return std::string(SET_PREFIX) + name;
}

// ZSet key helpers: use score-padded key for range scan ordering
// zset:{name}:member → score (for ZSCORE)
// zset:{name}:score:{padded_score}:{member} → "" (for ZRANGE by score)
// zset:{name}:__meta__ → count
std::string zset_member_key(const std::string& name, const std::string& member) {
    return std::string(ZSET_PREFIX) + name + ":member:" + member;
}
std::string zset_score_key(const std::string& name, double score, const std::string& member) {
    // Pad score to 20 chars with leading zeros for lexicographic ordering
    // Use format: sign + 15 digits + . + 4 digits
    char buf[32];
    if (score < 0) {
        snprintf(buf, sizeof(buf), "-%020.4f", -score);
    } else {
        snprintf(buf, sizeof(buf), "+%020.4f", score);
    }
    return std::string(ZSET_PREFIX) + name + ":score:" + buf + ":" + member;
}
std::string zset_meta_key(const std::string& name) {
    return std::string(ZSET_PREFIX) + name + ":__meta__";
}
std::string zset_score_prefix(const std::string& name) {
    return std::string(ZSET_PREFIX) + name + ":score:";
}

// Parse set blob: format is "len\0elem1\0elem2\0..."
std::vector<std::string> parse_set_blob(const std::string& blob) {
    std::vector<std::string> result;
    if (blob.empty()) return result;
    size_t pos = 0;
    while (pos < blob.size()) {
        size_t end = blob.find('\0', pos);
        if (end == std::string::npos) {
            if (pos < blob.size()) result.push_back(blob.substr(pos));
            break;
        }
        result.push_back(blob.substr(pos, end - pos));
        pos = end + 1;
    }
    return result;
}

std::string encode_set_blob(const std::vector<std::string>& elems) {
    std::string blob;
    for (auto& e : elems) {
        blob += e;
        blob += '\0';
    }
    return blob;
}

// Check if a key is an internal type key
bool is_internal_key(const std::string& key) {
    if (key.empty()) return false;
    if (key[0] == TTL_MAGIC) return true;
    if (key.size() >= 7 && key.substr(0, 7) == std::string(HASH_PREFIX)) return true;
    if (key.size() >= 7 && key.substr(0, 7) == std::string(LIST_PREFIX)) return true;
    if (key.size() >= 6 && key.substr(0, 6) == std::string(SET_PREFIX)) return true;
    if (key.size() >= 7 && key.substr(0, 7) == std::string(ZSET_PREFIX)) return true;
    return false;
}

// ─── Shared numeric formatting ───
// Shortest round-tripping representation with no trailing zeros ("4", "4.5"),
// instead of std::to_string's fixed 6 decimals ("4.000000").
std::string format_double_shortest(double v) {
    char buf[64];
    for (int precision = 1; precision <= 17; ++precision) {
        snprintf(buf, sizeof(buf), "%.*g", precision, v);
        if (std::strtod(buf, nullptr) == v) return buf;
    }
    snprintf(buf, sizeof(buf), "%.17g", v);
    return buf;
}

// ─── Raft log payload codec ───
// Wire format: [n_args:4][len0:4][arg0][len1:4][arg1]...
std::string encode_raft_command(const std::vector<std::string>& args) {
    std::string out;
    uint32_t n = static_cast<uint32_t>(args.size());
    out.append(reinterpret_cast<const char*>(&n), 4);
    for (const auto& a : args) {
        uint32_t len = static_cast<uint32_t>(a.size());
        out.append(reinterpret_cast<const char*>(&len), 4);
        out.append(a);
    }
    return out;
}

bool decode_raft_command(const std::string& data,
                         std::vector<std::string>* args) {
    args->clear();
    if (data.size() < 4) return false;
    uint32_t n;
    ::memcpy(&n, data.data(), 4);
    // Guard against a corrupt length claiming more args than could fit.
    if (n > data.size()) return false;
    size_t off = 4;
    for (uint32_t i = 0; i < n; ++i) {
        if (off + 4 > data.size()) return false;
        uint32_t len;
        ::memcpy(&len, data.data() + off, 4);
        off += 4;
        if (off + len > data.size()) return false;
        args->emplace_back(data.data() + off, len);
        off += len;
    }
    return true;
}

}  // namespace internal
}  // namespace lightkv