#include "lightkv/zset_index.h"
#include "lightkv/db.h"
#include "lightkv/slice.h"

#include <algorithm>
#include <chrono>

namespace lightkv {

// ─── ZSetIndex ───

ZSetIndex::ZSetIndex()
    : arena_(new detail::Arena<ZSetNode, char>()),
      impl_(new detail::SkipList<ZSetNode, char>(arena_.get())) {}

ZSetIndex::~ZSetIndex() = default;

void ZSetIndex::Upsert(double score, const std::string& member) {
    auto it = reverse_.find(member);
    if (it != reverse_.end()) {
        // 已存在 → 删除旧节点（通过插入 deletion marker，SkipList 语义）
        ZSetNode old{it->second, member};
        impl_->InsertDeletion(old, 0);
    }
    ZSetNode node{score, member};
    impl_->Insert(node, char{}, 0);
    reverse_[member] = score;
}

bool ZSetIndex::Remove(const std::string& member) {
    auto it = reverse_.find(member);
    if (it == reverse_.end()) return false;
    ZSetNode node{it->second, member};
    impl_->InsertDeletion(node, 0);
    reverse_.erase(it);
    return true;
}

bool ZSetIndex::Score(const std::string& member, double* out) const {
    auto it = reverse_.find(member);
    if (it == reverse_.end()) return false;
    *out = it->second;
    return true;
}

int64_t ZSetIndex::Rank(const std::string& member) const {
    auto it = reverse_.find(member);
    if (it == reverse_.end()) return -1;
    ZSetNode target{it->second, member};
    int64_t rank = 0;
    auto iter = impl_->SeekToFirst();
    while (iter.Valid()) {
        if (iter.IsDeleted()) { iter.Next(); continue; }
        if (iter.key() == target) return rank;
        ++rank;
        iter.Next();
    }
    return -1;  // 理论上不会走到（reverse_ 与 impl_ 同步）
}

int64_t ZSetIndex::RevRank(const std::string& member) const {
    int64_t r = Rank(member);
    if (r < 0) return -1;
    int64_t n = static_cast<int64_t>(reverse_.size());
    return n - 1 - r;
}

void ZSetIndex::RangeByScore(double min, double max,
                              bool min_excl, bool max_excl,
                              std::vector<std::pair<double, std::string>>* out) const {
    // SeekGE 到 min，顺序遍历到 max
    ZSetNode seek_key{min, ""};  // 最小 member 字典序
    auto iter = impl_->SeekGE(seek_key);
    while (iter.Valid()) {
        if (iter.IsDeleted()) { iter.Next(); continue; }
        const ZSetNode& k = iter.key();
        bool pass_min = min_excl ? (k.score > min) : (k.score >= min);
        if (!pass_min) { iter.Next(); continue; }
        if (k.score > max) break;
        bool pass_max = max_excl ? (k.score < max) : (k.score <= max);
        if (!pass_max) { iter.Next(); continue; }
        out->emplace_back(k.score, k.member);
        iter.Next();
    }
}

void ZSetIndex::RangeByIndex(int64_t start, int64_t stop,
                              std::vector<std::pair<double, std::string>>* out) const {
    int64_t n = static_cast<int64_t>(reverse_.size());
    if (n == 0) return;
    // Redis 负数索引语义
    if (start < 0) start = n + start;
    if (stop < 0) stop = n + stop;
    if (start < 0) start = 0;
    if (stop >= n) stop = n - 1;
    if (start > stop || start >= n) return;

    int64_t idx = 0;
    auto iter = impl_->SeekToFirst();
    while (iter.Valid()) {
        if (iter.IsDeleted()) { iter.Next(); continue; }
        if (idx > stop) break;
        if (idx >= start) {
            const ZSetNode& k = iter.key();
            out->emplace_back(k.score, k.member);
        }
        ++idx;
        iter.Next();
    }
}

void ZSetIndex::RevRangeByIndex(int64_t start, int64_t stop,
                                 std::vector<std::pair<double, std::string>>* out) const {
    // 反向：先正向收集全部，再反转切片（简单实现，反向遍历需跳表反向指针）
    std::vector<std::pair<double, std::string>> all;
    RangeByIndex(0, -1, &all);
    int64_t n = static_cast<int64_t>(all.size());
    if (n == 0) return;
    if (start < 0) start = n + start;
    if (stop < 0) stop = n + stop;
    if (start < 0) start = 0;
    if (stop >= n) stop = n - 1;
    if (start > stop || start >= n) return;
    // 反向：第 start 个 = 正向第 n-1-start 个
    for (int64_t i = start; i <= stop; ++i) {
        out->emplace_back(all[n - 1 - i]);
    }
}

void ZSetIndex::RebuildFrom(const std::vector<std::pair<double, std::string>>& sorted_entries) {
    // 调用者应保证 sorted_entries 已按 (score, member) 升序
    for (const auto& e : sorted_entries) {
        ZSetNode node{e.first, e.second};
        impl_->Insert(node, char{}, 0);
        reverse_[e.second] = e.first;
    }
}

// ─── ZSetIndexHub ───

ZSetIndex* ZSetIndexHub::Get(const std::string& name) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = indices_.find(name);
    if (it == indices_.end()) return nullptr;
    return it->second.get();
}

void ZSetIndexHub::Put(const std::string& name, std::unique_ptr<ZSetIndex> idx) {
    std::lock_guard<std::mutex> lock(mutex_);
    indices_[name] = std::move(idx);
}

void ZSetIndexHub::Drop(const std::string& name) {
    std::lock_guard<std::mutex> lock(mutex_);
    indices_.erase(name);
}

void ZSetIndexHub::RebuildAll(DB* db) {
    // 扫描所有 zset score keys，按 name 分组重建
    // key 格式：\x05_zset_{name}:score:{padded_score}:{member}
    // 这里委托 db->Scan，前缀用 "\x05_zset_"
    // 由于 DB::Scan 的前缀匹配，扫到的所有 zset score keys 都会进来
    // 然后按 name 分组，每组排序后 RebuildFrom
    //
    // 注意：这是后台线程渐进式执行，不阻塞主线程
    // 实际接入 server.cpp 时调用此方法

    // 暂用 std::unordered_map 收集，server.cpp 接入时填充
    // 此处仅占位，避免环依赖 DB 的具体 Scan 实现
    (void)db;  // 参数使用占位，实际逻辑由 server.cpp 调入时完成
}

} // namespace lightkv