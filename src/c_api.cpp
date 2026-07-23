#include "lightkv/c_api.h"
#include "lightkv/db.h"
#include "lightkv/db_impl.h"
#include "lightkv/slice.h"
#include "lightkv/status.h"
#include "lightkv/options.h"
#include "lightkv/wal.h"

#include <string>
#include <vector>
#include <thread>

// 线程本地错误描述
static thread_local std::string g_last_error;

static void set_error(const std::string& msg) { g_last_error = msg; }

// 不透明句柄实现
struct lightkv_handle {
    lightkv::DB* db;
};

#ifdef __cplusplus
extern "C" {
#endif

const char* lightkv_last_error(void) {
    return g_last_error.empty() ? "" : g_last_error.c_str();
}

lightkv_t* lightkv_open(const char* path) {
    if (!path) {
        set_error("path is null");
        return nullptr;
    }
    lightkv::Options opts;
    opts.db_path = path;
    opts.create_if_missing = true;
    lightkv::DB* db = nullptr;
    auto s = lightkv::DB::Open(opts, &db);
    if (!s.ok()) {
        set_error("Failed to open DB: " + s.ToString());
        return nullptr;
    }
    auto* h = new lightkv_handle{db};
    return h;
}

void lightkv_close(lightkv_t* db) {
    if (!db) return;
    // DBImpl 通过 DB::Open 返回，需要 delete 释放
    delete db->db;
    delete db;
}

int lightkv_put(lightkv_t* db, const char* key, size_t key_len,
                 const char* value, size_t value_len) {
    if (!db || !db->db || !key) return LIGHTKV_ERR_INVALID;
    lightkv::WriteOptions wo;
    auto s = db->db->Put(wo, lightkv::Slice(key, key_len),
                         lightkv::Slice(value, value_len));
    if (!s.ok()) { set_error(s.ToString()); return LIGHTKV_ERR_IO; }
    return LIGHTKV_OK;
}

int lightkv_get(lightkv_t* db, const char* key, size_t key_len,
                char* buf, size_t* buf_len) {
    if (!db || !db->db || !key || !buf_len) return LIGHTKV_ERR_INVALID;
    lightkv::ReadOptions ro;
    std::string value;
    auto s = db->db->Get(ro, lightkv::Slice(key, key_len), &value);
    if (s.IsNotFound()) {
        *buf_len = 0;
        return LIGHTKV_ERR_NOT_FOUND;
    }
    if (!s.ok()) { set_error(s.ToString()); return LIGHTKV_ERR_IO; }
    if (value.size() > *buf_len) {
        *buf_len = value.size();  // 告知所需容量
        set_error("buffer too small");
        return LIGHTKV_ERR_INVALID;
    }
    if (buf && value.size() > 0) {
        std::memcpy(buf, value.data(), value.size());
    }
    *buf_len = value.size();
    return LIGHTKV_OK;
}

int lightkv_delete(lightkv_t* db, const char* key, size_t key_len) {
    if (!db || !db->db || !key) return LIGHTKV_ERR_INVALID;
    lightkv::WriteOptions wo;
    auto s = db->db->Delete(wo, lightkv::Slice(key, key_len));
    if (!s.ok()) { set_error(s.ToString()); return LIGHTKV_ERR_IO; }
    return LIGHTKV_OK;
}

int lightkv_exists(lightkv_t* db, const char* key, size_t key_len) {
    if (!db || !db->db || !key) return -LIGHTKV_ERR_INVALID;
    lightkv::ReadOptions ro;
    if (db->db->Exists(ro, lightkv::Slice(key, key_len))) return 1;
    return 0;
}

int lightkv_increment(lightkv_t* db, const char* key, size_t key_len,
                      int64_t delta, int64_t* new_value) {
    if (!db || !db->db || !key) return LIGHTKV_ERR_INVALID;
    lightkv::WriteOptions wo;
    int64_t out = 0;
    auto s = db->db->Increment(wo, lightkv::Slice(key, key_len), delta, &out);
    if (!s.ok()) { set_error(s.ToString()); return LIGHTKV_ERR_IO; }
    if (new_value) *new_value = out;
    return LIGHTKV_OK;
}

int lightkv_rename(lightkv_t* db, const char* src, size_t src_len,
                   const char* dst, size_t dst_len) {
    if (!db || !db->db || !src || !dst) return LIGHTKV_ERR_INVALID;
    lightkv::WriteOptions wo;
    auto s = db->db->Rename(wo, lightkv::Slice(src, src_len),
                            lightkv::Slice(dst, dst_len));
    if (!s.ok()) { set_error(s.ToString()); return LIGHTKV_ERR_IO; }
    return LIGHTKV_OK;
}

int lightkv_batch_write(lightkv_t* db, const lightkv_batch_op_t* ops, size_t count) {
    if (!db || !db->db) return LIGHTKV_ERR_INVALID;
    if (count == 0 || !ops) return LIGHTKV_OK;
    std::vector<lightkv::WALRecord::BatchOp> batch;
    batch.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        lightkv::WALRecord::BatchOp op;
        op.type = (ops[i].type == 1) ? lightkv::WALRecord::kTypeValue
                                      : lightkv::WALRecord::kTypeDeletion;
        op.key.assign(ops[i].key, ops[i].key_len);
        if (ops[i].type == 1) {
            op.value.assign(ops[i].value, ops[i].value_len);
        }
        batch.emplace_back(std::move(op));
    }
    lightkv::WriteOptions wo;
    auto s = db->db->BatchWrite(wo, batch);
    if (!s.ok()) { set_error(s.ToString()); return LIGHTKV_ERR_IO; }
    return LIGHTKV_OK;
}

size_t lightkv_scan(lightkv_t* db, const char* prefix, size_t prefix_len,
                    size_t limit, lightkv_scan_callback callback, void* user_data) {
    if (!db || !db->db || !callback) return 0;
    lightkv::ReadOptions ro;
    std::vector<std::pair<std::string, std::string>> results;
    int lim = (limit > 0 && limit < INT32_MAX) ? static_cast<int>(limit) : INT32_MAX;
    auto s = db->db->Scan(ro, lightkv::Slice(prefix, prefix_len), lim, &results);
    if (!s.ok()) { set_error(s.ToString()); return 0; }
    size_t emitted = 0;
    for (const auto& kv : results) {
        if (emitted >= limit) break;
        int rc = callback(kv.first.data(), kv.first.size(),
                          kv.second.data(), kv.second.size(), user_data);
        ++emitted;
        if (rc != 0) break;
    }
    return emitted;
}

#ifdef __cplusplus
}
#endif