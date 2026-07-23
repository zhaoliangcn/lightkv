#pragma once

#include "slice.h"
#include <cstdint>
#include <string>
#include <vector>
#include <unordered_map>
#include <functional>
#include <mutex>
#include <atomic>

namespace lightkv {

// Watch 机制（详见设计草案 6）
//
// 两条路径并存但定位不同：
//   路径 A（Redis 兼容）— SUBSCRIBE __keyspace@0__:mykey，单向广播，无保活
//   路径 B（增强命令）— WATCH mykey，双向 RPC + revision 序号，支持断线重连补数据
//
// revision 复用 WAL 的 seq（无需独立序号空间），每次 Notify 用 db 的 last_seq
class WatchHub {
public:
    using NotifyFn = std::function<void(const std::string& pattern,
                                         const std::string& key,
                                         const std::string& event,
                                         uint64_t revision)>;

    // 订阅精确 key 或前缀（pattern 以 '*' 结尾视为前缀）
    // 返回 watcher_id，用于 UNWATCH
    uint64_t Subscribe(const std::string& pattern, NotifyFn cb);

    // 取消订阅（watcher_id 来自 Subscribe 返回值）
    void Unsubscribe(uint64_t watcher_id);

    // 通知所有匹配 pattern 的 watcher
    // 由 DBImpl::Put/Delete 触发，event 形如 "set" / "del" / "expire"
    void Notify(const std::string& key, const std::string& event, uint64_t revision);

    // 清空所有订阅（server 关闭时调用）
    void Clear();

    // 统计：当前活跃 watcher 数
    size_t WatcherCount() const;

private:
    struct Watcher {
        uint64_t id;
        bool is_prefix;       // pattern 结尾 '*' → 前缀匹配
        std::string pattern;  // is_prefix=false 时为完整 pattern（去掉末尾 *）
        NotifyFn callback;
    };

    mutable std::mutex mu_;
    std::vector<Watcher> watchers_;           // 线性表，订阅量通常很小
    std::atomic<uint64_t> next_id_{1};
};

} // namespace lightkv