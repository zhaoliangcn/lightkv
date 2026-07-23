#include "lightkv/watch.h"
#include <algorithm>
#include <cstring>

namespace lightkv {

uint64_t WatchHub::Subscribe(const std::string& pattern, NotifyFn cb) {
    Watcher w;
    w.id = next_id_.fetch_add(1, std::memory_order_relaxed);
    w.pattern = pattern;
    // 末尾 '*' → 前缀匹配；其他视为完整 key
    if (!pattern.empty() && pattern.back() == '*') {
        w.is_prefix = true;
        w.pattern = pattern.substr(0, pattern.size() - 1);
    } else {
        w.is_prefix = false;
    }
    w.callback = std::move(cb);

    std::lock_guard<std::mutex> lk(mu_);
    watchers_.push_back(std::move(w));
    return watchers_.back().id;
}

void WatchHub::Unsubscribe(uint64_t watcher_id) {
    std::lock_guard<std::mutex> lk(mu_);
    watchers_.erase(
        std::remove_if(watchers_.begin(), watchers_.end(),
                       [watcher_id](const Watcher& w) { return w.id == watcher_id; }),
        watchers_.end());
}

void WatchHub::Notify(const std::string& key, const std::string& event, uint64_t revision) {
    // 复制 watcher 刽表到本地，避免持锁调用回调（回调可能再 Subscribe → 死锁）
    std::vector<Watcher> snapshot;
    {
        std::lock_guard<std::mutex> lk(mu_);
        snapshot = watchers_;
    }
    for (const auto& w : snapshot) {
        bool match = w.is_prefix
                         ? (key.compare(0, w.pattern.size(), w.pattern) == 0)
                         : (key == w.pattern);
        if (match) {
            w.callback(w.is_prefix ? w.pattern + "*" : w.pattern,
                       key, event, revision);
        }
    }
}

void WatchHub::Clear() {
    std::lock_guard<std::mutex> lk(mu_);
    watchers_.clear();
}

size_t WatchHub::WatcherCount() const {
    std::lock_guard<std::mutex> lk(mu_);
    return watchers_.size();
}

} // namespace lightkv