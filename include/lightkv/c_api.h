#pragma once

#include <stddef.h>  // size_t
#include <stdint.h>   // int64_t

// ─── LightKV Embedded C API ───
//
// v2.0 嵌入式绑定（详见设计草案 3.2）
// 让 C 程序直接用 LightKV 作嵌入式 KV 引擎，无需网络层
//
// 用法：
//   lightkv_t db = lightkv_open("/path/to/data");
//   lightkv_put(db, "key", 3, "value", 5);
//   char buf[256]; size_t len;
//   lightkv_get(db, "key", 3, buf, &len);
//   lightkv_close(db);
//
// 所有字符串参数都是字节缓冲，长度显式传递（支持二进制安全）
// 错误用返回码表达：0 = 成功，非 0 = 错误码（lightkv_last_error 拿描述）

#ifdef __cplusplus
extern "C" {
#endif

// 不透明句柄
typedef struct lightkv_handle lightkv_t;

// 错误码
typedef enum {
    LIGHTKV_OK             = 0,
    LIGHTKV_ERR_INVALID    = 1,
    LIGHTKV_ERR_NOT_FOUND = 2,
    LIGHTKV_ERR_IO        = 3,
    LIGHTKV_ERR_CORRUPTION = 4,
    LIGHTKV_ERR_UNSUPPORTED = 5,
} lightkv_status_t;

// 打开/创建数据库
//   path: 数据目录路径（不存在会自动创建）
//   返回: 句柄，nullptr = 失败（lightkv_last_error 拿描述）
lightkv_t* lightkv_open(const char* path);

// 关闭数据库（释放所有资源）
void lightkv_close(lightkv_t* db);

// ─── 基础 KV 操作 ───

// 写入
//   db, key, key_len, value, value_len
//   返回: LIGHTKV_OK 成功，其他 = 错误
int lightkv_put(lightkv_t* db, const char* key, size_t key_len,
                 const char* value, size_t value_len);

// 读取
//   db, key, key_len, buf（调用方分配），buf_len（入参为容量，出参为实际长度）
//   返回: LIGHTKV_OK 成功，LIGHTKV_ERR_NOT_FOUND key 不存在
//   注意: 若 buf 容量不足，返回 LIGHTKV_ERR_INVALID，buf_len 设为所需长度
int lightkv_get(lightkv_t* db, const char* key, size_t key_len,
                char* buf, size_t* buf_len);

// 删除
int lightkv_delete(lightkv_t* db, const char* key, size_t key_len);

// 判断是否存在
//   返回: 1 = 存在，0 = 不存在，负值 = 错误
int lightkv_exists(lightkv_t* db, const char* key, size_t key_len);

// 原子自增
//   delta 可正可负
//   new_value 出参（可为 nullptr）
int lightkv_increment(lightkv_t* db, const char* key, size_t key_len,
                      int64_t delta, int64_t* new_value);

// 重命名
int lightkv_rename(lightkv_t* db, const char* src, size_t src_len,
                   const char* dst, size_t dst_len);

// ─── 批量操作 ───

// 原子批量写入 — 单次提交包含多个操作（详见设计草案 5）
//   ops: 操作数组，每项为 (type, key, key_len, value, value_len)
//   count: ops 数量
//   type: 1 = Put, 2 = Delete
typedef struct {
    int type;           // 1 = Put, 2 = Delete
    const char* key;
    size_t key_len;
    const char* value;
    size_t value_len;
} lightkv_batch_op_t;

int lightkv_batch_write(lightkv_t* db, const lightkv_batch_op_t* ops, size_t count);

// ─── Scan ───

// 扫描指定前缀的 key
//   prefix, prefix_len: 前缀
//   limit: 最多返回数量
//   callback: 每条 (key, value) 调用一次，返回 0 继续，非 0 停止
//   user_data: 透传给 callback 的上下文
//   返回: 已扫描的条数
typedef int (*lightkv_scan_callback)(const char* key, size_t key_len,
                                     const char* value, size_t value_len,
                                     void* user_data);

size_t lightkv_scan(lightkv_t* db, const char* prefix, size_t prefix_len,
                    size_t limit, lightkv_scan_callback callback, void* user_data);

// ─── 错误信息 ───

// 拿最后一次错误描述（线程本地）
const char* lightkv_last_error(void);

#ifdef __cplusplus
}
#endif