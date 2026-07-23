#pragma once

#include "skiplist.h"
#include <string>
#include <unordered_map>
#include <memory>
#include <mutex>
#include <vector>
#include <cstdint>

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

// 单个 ZSet 的内存索引
// 用 detail::SkipList<ZSetNode, char> 维护有序结构，支持：
// - O(log N) 插入/删除
// - O(K)     ZRANGE 按位置遍历
// - O(log N) ZRANK 按 (score, member) 定位
// - O(log N + K) ZRANGEBYSCORE 按分数区间扫描
class ZSetIndex {
    using Index = detail::SkipList<ZSetNode, char>;
    using ArenaType = detail::Arena<ZSetNode, char>;
    std::unique_ptr<ArenaType> arena_;
    std::unique_ptr<Index> impl_;
    // 反向表：member → score，用于 ZSCORE / 删除时定位 score
    std::unordered_map<std::string, double> reverse_;

public:
    ZSetIndex();
    ~ZSetIndex();

    // 添加或更新 (score, member)；若 member 已存在则先删旧后插新
    void Upsert(double score, const std::string& member);
    // 删除 member；若不存在则无副作用，返回是否删除
    bool Remove(const std::string& member);
    // 查询 member 的 score；不存在返回 false
    bool Score(const std::string& member, double* out) const;
    // 返回 (score, member) 在升序中的 0-based rank；不存在返回 -1
    int64_t Rank(const std::string& member) const;
    // 反向 rank（0 = 最高分）；不存在返回 -1
    int64_t RevRank(const std::string& member) const;
    // 元素数量
    size_t Size() const { return reverse_.size(); }
    // 按 [min, max] 分数区间收集 (score, member)，含端点控制
    void RangeByScore(double min, double max,
                      bool min_excl, bool max_excl,
                      std::vector<std::pair<double, std::string>>* out) const;
    // 按位置区间 [start, stop] 收集 (score, member)，stop 为 -1 表示末尾
    void RangeByIndex(int64_t start, int64_t stop,
                      std::vector<std::pair<double, std::string>>* out) const;
    // 反向按位置区间
    void RevRangeByIndex(int64_t start, int64_t stop,
                         std::vector<std::pair<double, std::string>>* out) const;

    // 启动重建：从已排序的 (score, member) 列表填充
    void RebuildFrom(const std::vector<std::pair<double, std::string>>& sorted_entries);
};

// ZSet 索引单例：按 zset name 持有 ZSetIndex
// 线程安全：所有公开方法持 mutex_
class ZSetIndexHub {
    std::mutex mutex_;
    std::unordered_map<std::string, std::unique_ptr<ZSetIndex>> indices_;

public:
    // 取得某 zset 的索引；若不存在返回 nullptr
    // 调用者持有返回值期间索引保持存活（Hub 持所有权）
    ZSetIndex* Get(const std::string& name);

    // 注册某 zset 的索引（接管所有权）
    void Put(const std::string& name, std::unique_ptr<ZSetIndex> idx);

    // 删除某 zset 的索引（对应 ZREM 全清或 DEL key）
    void Drop(const std::string& name);

    // 启动时后台批量重建：扫描所有 zset 前缀，填充索引
    // 委托 DB 的 Scan，Hub 不直接依赖 DB
    void RebuildAll(class DB* db);
};

} // namespace lightkv