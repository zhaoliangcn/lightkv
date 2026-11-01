#pragma once

#include "skiplist.h"

#include <cstdint>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace lightkv {

// ZSet 索引节点：按 (score, member) 复合键排序
// 与 Redis 自身的 zskiplist 语义一致：
// - 主键 score 升序
// - 同 score 下按 member 字典序
struct ZSetNode {
    double score;
    std::string member;

    bool operator<(const ZSetNode& other) const {
        if (score != other.score) return score < other.score;
        return member < other.member;
    }
    bool operator==(const ZSetNode& other) const {
        return score == other.score && member == other.member;
    }
};

// 单个 ZSet 的内存索引。
//
// 用 detail::SkipList<ZSetNode, char, /*kRanked=*/true> 维护有序结构。跳表带
// span 之后，四条主要操作都是对数复杂度：
//   Insert/Erase    O(log N)
//   ZRANK           O(log N)
//   ZRANGE start stop   O(log N + K)   —— 按位置直接定位，不再从头走
//   ZRANGEBYSCORE   O(log N + K)   —— 按分数 seek 之后顺序扫
//
// 线程安全：所有公开方法都在内部 mu_ 上取锁，调用方不需要（也不能）再加锁。
// Hub 交出去的是 shared_ptr，索引本身也不会在半路被销毁。
class ZSetIndex {
    using Index = detail::SkipList<ZSetNode, char, /*kRanked=*/true>;
    using ArenaType = detail::Arena<ZSetNode, char>;

    // Guards everything below. The skip list has a lock of its own, but it only
    // protects the linked structure; reverse_ has to move in lockstep with it or
    // a reader can see the map and the list disagree (Score() reporting a member
    // that Rank() cannot find). One lock across both is the only way they stay
    // consistent, and the *Locked helpers keep the public methods from taking it
    // twice -- std::shared_mutex does not allow that.
    mutable std::shared_mutex mu_;

    std::unique_ptr<ArenaType> arena_;
    std::unique_ptr<Index> impl_;
    // 反向表：member → score，用于 ZSCORE / 删除时定位 score
    std::unordered_map<std::string, double> reverse_;

    // Nodes the list has unlinked. Erase() takes a node off every level but the
    // arena cannot hand its bytes back, so each update/removal parks one node
    // until the index is rebuilt. CompactLocked() starts over from the live
    // entries once they outnumber them, which is what keeps the footprint (and
    // the walk in RangeByIndex) proportional to Size() rather than to how many
    // times members have been re-scored.
    size_t retired_ = 0;

    void ResetLocked();
    void MaybeCompactLocked();
    int64_t RankLocked(const std::string& member) const;
    void RangeByScoreLocked(double min, double max, bool min_excl, bool max_excl,
                            std::vector<std::pair<double, std::string>>* out,
                            int64_t* count_only, int64_t offset, int64_t limit) const;
    void RangeByIndexLocked(int64_t start, int64_t stop,
                            std::vector<std::pair<double, std::string>>* out) const;

public:
    ZSetIndex();
    ~ZSetIndex();

    ZSetIndex(const ZSetIndex&) = delete;
    ZSetIndex& operator=(const ZSetIndex&) = delete;

    // 添加或更新 (score, member)：先摘掉旧节点再插入新节点。
    // 同一 score 视为无变化（ZADD 语义），不会额外搬动节点。
    void Upsert(double score, const std::string& member);
    // 删除 member；不存在返回 false
    bool Remove(const std::string& member);
    // 查询 member 的 score；不存在返回 false
    bool Score(const std::string& member, double* out) const;
    // 返回 (score, member) 在升序中的 0-based rank；不存在返回 -1
    int64_t Rank(const std::string& member) const;
    // 反向 rank（0 = 最高分）；不存在返回 -1
    int64_t RevRank(const std::string& member) const;
    // 元素数量
    size_t Size() const;
    // 按 [min, max] 分数区间收集 (score, member)，含端点控制。
    // offset/limit 对应 ZRANGEBYSCORE 的 LIMIT：跳过 offset 个匹配后最多取
    // limit 个（limit < 0 表示不限制）。
    void RangeByScore(double min, double max, bool min_excl, bool max_excl,
                      std::vector<std::pair<double, std::string>>* out,
                      int64_t offset = 0, int64_t limit = -1) const;
    // 分数区间内的元素个数（ZCOUNT），不materialise
    int64_t CountByScore(double min, double max, bool min_excl, bool max_excl) const;
    // 按位置区间 [start, stop] 收集 (score, member)，负数为 Redis 语义
    void RangeByIndex(int64_t start, int64_t stop,
                      std::vector<std::pair<double, std::string>>* out) const;
    // 反向按位置区间
    void RevRangeByIndex(int64_t start, int64_t stop,
                         std::vector<std::pair<double, std::string>>* out) const;

    // 启动重建：清空现有内容后按已排序的 (score, member) 列表填充
    void RebuildFrom(const std::vector<std::pair<double, std::string>>& sorted_entries);
};

// 某个 zset 索引在某一时刻的「代次」。
//
// 惰性重建（索引不在时扫描 KV 建一份）需要这个：扫描是慢的，扫的这段时间里
// 可能已经有写入落库并且更新了索引，若把扫描结果直接装回去就会把那些写入抹掉，
// 而且一旦抹掉就再也不会自愈。重建方在扫描前取一份 stamp，装回时比对，只有
// 期间没有任何写入/删除才装。
struct ZSetStamp {
    uint64_t clear = 0;  // FLUSHDB 递增
    uint64_t write = 0;  // 该 name 的写入/删除递增

    bool operator==(const ZSetStamp& other) const {
        return clear == other.clear && write == other.write;
    }
    bool operator!=(const ZSetStamp& other) const { return !(*this == other); }
};

// ZSet 索引单例：按 zset name 持有 ZSetIndex
// 线程安全：所有公开方法持 mutex_
class ZSetIndexHub {
    mutable std::mutex mutex_;
    std::unordered_map<std::string, std::shared_ptr<ZSetIndex>> indices_;
    std::unordered_map<std::string, uint64_t> writes_;
    uint64_t clear_gen_ = 0;

public:
    // 取得某 zset 的索引；不存在返回 nullptr。
    // 返回值持有强引用：即使另一线程同时 Drop/覆盖了同名索引，调用方手里的这
    // 一份仍然有效（改动会落在一个已经脱离 Hub 的索引上，丢弃即可，不会再
    // 变成释放后使用）。
    std::shared_ptr<ZSetIndex> Get(const std::string& name) const;

    // 无条件注册（接管所有权）
    void Put(const std::string& name, std::shared_ptr<ZSetIndex> idx);

    // 当前代次 / 标记该 name 有过写入
    ZSetStamp Stamp(const std::string& name) const;
    void Bump(const std::string& name);

    // 仅当代次未变时注册；返回是否装上
    bool Install(const std::string& name, std::shared_ptr<ZSetIndex> idx, const ZSetStamp& stamp);

    // 删除某 zset 的索引（DEL / 过期 / 全清）
    void Drop(const std::string& name);

    // 丢掉全部索引（FLUSHDB）
    void Clear();

    // 当前持有的索引数量（测试与统计用）
    size_t Size() const;

    // 显式重建：扫描 DB 里已有的 zset，按 name 分组建索引。
    // 阻塞式，不做后台调度 —— 调用方自己决定什么时候付这个代价。正常读写路径
    // 不需要它：索引缺失时第一次读会惰性建。
    void RebuildAll(class DB* db);
};

} // namespace lightkv
