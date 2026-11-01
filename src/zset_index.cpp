#include "lightkv/zset_index.h"

#include "lightkv/db.h"
#include "lightkv/slice.h"
#include "server_internal.h"

#include <algorithm>
#include <cstdlib>

namespace lightkv {

// ─── ZSetIndex ───

ZSetIndex::ZSetIndex()
    : arena_(new ArenaType()),
      impl_(new Index(arena_.get())) {}

ZSetIndex::~ZSetIndex() = default;

void ZSetIndex::ResetLocked() {
    // Destroy the list before the arena it allocates from.
    impl_.reset();
    arena_.reset();
    arena_.reset(new ArenaType());
    impl_.reset(new Index(arena_.get()));
    reverse_.clear();
    retired_ = 0;
}

void ZSetIndex::MaybeCompactLocked() {
    // Erase() unlinks but cannot free, so the retired nodes pile up in the old
    // arena. Rebuild from the live entries once they outnumber the live ones;
    // the floor keeps a small zset from rebuilding on every other update.
    if (retired_ <= 64 || retired_ <= reverse_.size()) return;

    std::vector<std::pair<double, std::string>> live;
    live.reserve(reverse_.size());
    for (auto iter = impl_->SeekToFirst(); iter.Valid(); iter.Next()) {
        live.emplace_back(iter.key().score, iter.key().member);
    }

    ResetLocked();
    try {
        for (const auto& e : live) {
            impl_->Insert(ZSetNode{e.first, e.second}, char{}, 0);
            reverse_.emplace(e.second, e.first);
        }
    } catch (...) {
        // Half a rebuild is worse than none: drop everything so the next read
        // rebuilds from the database instead of serving a partial index.
        ResetLocked();
        throw;
    }
}

void ZSetIndex::Upsert(double score, const std::string& member) {
    std::unique_lock<std::shared_mutex> lock(mu_);
    auto it = reverse_.find(member);
    if (it != reverse_.end()) {
        if (it->second == score) return;  // same score: nothing to move
        impl_->Erase(ZSetNode{it->second, member});
        ++retired_;
        it->second = score;
    } else {
        reverse_.emplace(member, score);
    }

    try {
        impl_->Insert(ZSetNode{score, member}, char{}, 0);
    } catch (...) {
        // The list did not take the entry, so the map must not claim one:
        // otherwise Score() reports a score for a member no range query returns.
        reverse_.erase(member);
        throw;
    }
    MaybeCompactLocked();
}

bool ZSetIndex::Remove(const std::string& member) {
    std::unique_lock<std::shared_mutex> lock(mu_);
    auto it = reverse_.find(member);
    if (it == reverse_.end()) return false;
    impl_->Erase(ZSetNode{it->second, member});
    reverse_.erase(it);
    ++retired_;
    MaybeCompactLocked();
    return true;
}

bool ZSetIndex::Score(const std::string& member, double* out) const {
    std::shared_lock<std::shared_mutex> lock(mu_);
    auto it = reverse_.find(member);
    if (it == reverse_.end()) return false;
    *out = it->second;
    return true;
}

int64_t ZSetIndex::RankLocked(const std::string& member) const {
    auto it = reverse_.find(member);
    if (it == reverse_.end()) return -1;
    return impl_->RankOf(ZSetNode{it->second, member});
}

int64_t ZSetIndex::Rank(const std::string& member) const {
    std::shared_lock<std::shared_mutex> lock(mu_);
    return RankLocked(member);
}

int64_t ZSetIndex::RevRank(const std::string& member) const {
    std::shared_lock<std::shared_mutex> lock(mu_);
    int64_t r = RankLocked(member);
    if (r < 0) return -1;
    return static_cast<int64_t>(reverse_.size()) - 1 - r;
}

size_t ZSetIndex::Size() const {
    std::shared_lock<std::shared_mutex> lock(mu_);
    return reverse_.size();
}

void ZSetIndex::RangeByScoreLocked(double min, double max, bool min_excl, bool max_excl,
                                   std::vector<std::pair<double, std::string>>* out,
                                   int64_t* count_only, int64_t offset, int64_t limit) const {
    // Seek to the first entry at or after min; the "" member sorts before every
    // real member, so this lands on the start of the min-score group.
    auto iter = impl_->SeekGE(ZSetNode{min, ""});
    int64_t count = 0;
    while (iter.Valid()) {
        const ZSetNode& k = iter.key();
        if (k.score > max) break;
        const bool pass_min = min_excl ? (k.score > min) : (k.score >= min);
        const bool pass_max = max_excl ? (k.score < max) : (k.score <= max);
        if (pass_min && pass_max) {
            if (count >= offset && (limit < 0 || count - offset < limit)) {
                if (out) out->emplace_back(k.score, k.member);
            }
            ++count;
            if (limit >= 0 && count - offset >= limit) break;
        }
        iter.Next();
    }
    if (count_only) *count_only = count;
}

void ZSetIndex::RangeByScore(double min, double max, bool min_excl, bool max_excl,
                             std::vector<std::pair<double, std::string>>* out,
                             int64_t offset, int64_t limit) const {
    std::shared_lock<std::shared_mutex> lock(mu_);
    RangeByScoreLocked(min, max, min_excl, max_excl, out, nullptr, offset, limit);
}

int64_t ZSetIndex::CountByScore(double min, double max, bool min_excl, bool max_excl) const {
    std::shared_lock<std::shared_mutex> lock(mu_);
    int64_t count = 0;
    RangeByScoreLocked(min, max, min_excl, max_excl, nullptr, &count, 0, -1);
    return count;
}

void ZSetIndex::RangeByIndexLocked(int64_t start, int64_t stop,
                                   std::vector<std::pair<double, std::string>>* out) const {
    const int64_t n = static_cast<int64_t>(reverse_.size());
    if (n == 0) return;
    // Redis 负数索引语义
    if (start < 0) start = n + start;
    if (stop < 0) stop = n + stop;
    if (start < 0) start = 0;
    if (stop >= n) stop = n - 1;
    if (start > stop || start >= n) return;

    // Jump straight to `start` instead of walking the whole set: this is what
    // makes ZRANGE 0 9 on a million-member leaderboard cost ten nodes, not a
    // million.
    auto iter = impl_->SeekToRank(static_cast<uint64_t>(start));
    for (int64_t i = start; i <= stop && iter.Valid(); ++i) {
        out->emplace_back(iter.key().score, iter.key().member);
        iter.Next();
    }
}

void ZSetIndex::RangeByIndex(int64_t start, int64_t stop,
                             std::vector<std::pair<double, std::string>>* out) const {
    std::shared_lock<std::shared_mutex> lock(mu_);
    RangeByIndexLocked(start, stop, out);
}

void ZSetIndex::RevRangeByIndex(int64_t start, int64_t stop,
                                std::vector<std::pair<double, std::string>>* out) const {
    std::shared_lock<std::shared_mutex> lock(mu_);
    const int64_t n = static_cast<int64_t>(reverse_.size());
    if (n == 0) return;
    if (start < 0) start = n + start;
    if (stop < 0) stop = n + stop;
    if (start < 0) start = 0;
    if (stop >= n) stop = n - 1;
    if (start > stop || start >= n) return;

    // Reverse index i is forward index n-1-i, so a descending window is the
    // forward window [n-1-stop, n-1-start] emitted backwards. Collecting just
    // that window keeps this O(log N + K) too, instead of materialising the set.
    std::vector<std::pair<double, std::string>> window;
    window.reserve(static_cast<size_t>(stop - start + 1));
    RangeByIndexLocked(n - 1 - stop, n - 1 - start, &window);
    for (auto it = window.rbegin(); it != window.rend(); ++it) {
        out->emplace_back(*it);
    }
}

void ZSetIndex::RebuildFrom(const std::vector<std::pair<double, std::string>>& sorted_entries) {
    std::unique_lock<std::shared_mutex> lock(mu_);
    // Start from empty. Appending to whatever was there before would keep every
    // stale entry alive and hand out ranks that no longer exist.
    ResetLocked();
    try {
        for (const auto& e : sorted_entries) {
            // A member appears once in a sorted set. If the caller feeds a
            // duplicate, the first (lowest-scored, given the sort) copy wins so
            // that reverse_ and the list still describe the same set.
            if (!reverse_.emplace(e.second, e.first).second) continue;
            impl_->Insert(ZSetNode{e.first, e.second}, char{}, 0);
        }
    } catch (...) {
        ResetLocked();
        throw;
    }
}

// ─── ZSetIndexHub ───

std::shared_ptr<ZSetIndex> ZSetIndexHub::Get(const std::string& name) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = indices_.find(name);
    if (it == indices_.end()) return nullptr;
    return it->second;
}

void ZSetIndexHub::Put(const std::string& name, std::shared_ptr<ZSetIndex> idx) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (idx) {
        indices_[name] = std::move(idx);
    } else {
        indices_.erase(name);
    }
}

ZSetStamp ZSetIndexHub::Stamp(const std::string& name) const {
    std::lock_guard<std::mutex> lock(mutex_);
    ZSetStamp stamp;
    stamp.clear = clear_gen_;
    auto it = writes_.find(name);
    stamp.write = (it == writes_.end()) ? 0 : it->second;
    return stamp;
}

void ZSetIndexHub::Bump(const std::string& name) {
    std::lock_guard<std::mutex> lock(mutex_);
    ++writes_[name];
}

bool ZSetIndexHub::Install(const std::string& name, std::shared_ptr<ZSetIndex> idx,
                           const ZSetStamp& stamp) {
    std::lock_guard<std::mutex> lock(mutex_);
    ZSetStamp now;
    now.clear = clear_gen_;
    auto it = writes_.find(name);
    now.write = (it == writes_.end()) ? 0 : it->second;
    if (now != stamp) return false;
    indices_[name] = std::move(idx);
    return true;
}

void ZSetIndexHub::Drop(const std::string& name) {
    std::lock_guard<std::mutex> lock(mutex_);
    indices_.erase(name);
    // A rebuild that started before the drop must not put the index back.
    ++writes_[name];
}

void ZSetIndexHub::Clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    indices_.clear();
    writes_.clear();
    // Names arriving from here on are distinguishable from any scan that was
    // already in flight, even ones for names this hub had never seen.
    ++clear_gen_;
}

size_t ZSetIndexHub::Size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return indices_.size();
}

namespace {

// "\x05_zset_<name>:score:<sign><20 chars>:<member>"
//
// Parsed from the right: a name is allowed to contain ':' and even ":score:",
// but the score field is fixed width, so the last ':' delimits the member and
// the 21 characters before it are the score.
bool ParseScoreKey(const std::string& key, const std::string& prefix,
                   std::string* name, double* score, std::string* member) {
    constexpr size_t kScoreLen = 21;  // sign + %020.4f
    if (key.size() <= prefix.size() || key.compare(0, prefix.size(), prefix) != 0) return false;
    const size_t member_sep = key.rfind(':');
    if (member_sep == std::string::npos) return false;
    // prefix + name + ":score:" + score + ":" + member
    const size_t min_len = prefix.size() + 1 + 7 + kScoreLen + 1;
    if (key.size() < min_len) return false;
    const size_t score_begin = member_sep - kScoreLen;
    if (score_begin < prefix.size() + 7) return false;
    if (key.compare(score_begin - 7, 7, ":score:") != 0) return false;
    const size_t name_len = score_begin - 7 - prefix.size();
    if (name_len == 0) return false;
    char* end = nullptr;
    const std::string score_str = key.substr(score_begin, kScoreLen);
    const double v = std::strtod(score_str.c_str(), &end);
    if (end == score_str.c_str()) return false;
    *name = key.substr(prefix.size(), name_len);
    *score = v;
    *member = key.substr(member_sep + 1);
    return true;
}

}  // namespace

void ZSetIndexHub::RebuildAll(DB* db) {
    if (!db) return;
    const std::string prefix(internal::ZSET_PREFIX);

    ReadOptions ro;
    std::vector<std::pair<std::string, std::string>> rows;
    // Everything under the zset prefix -- score keys, member keys and the meta
    // counter. ParseScoreKey() keeps only the score keys, which are the set.
    Status s = db->Scan(ro, Slice(prefix.data(), prefix.size()), 1000000, &rows);
    if (!s.ok()) return;

    std::unordered_map<std::string, std::vector<std::pair<double, std::string>>> groups;
    for (const auto& kv : rows) {
        std::string name, member;
        double score = 0;
        if (!ParseScoreKey(kv.first, prefix, &name, &score, &member)) continue;
        groups[name].emplace_back(score, member);
    }

    for (auto& group : groups) {
        // Take the stamp before touching the data so a write that lands while we
        // build keeps its value instead of being overwritten by this snapshot.
        const ZSetStamp stamp = Stamp(group.first);
        auto& entries = group.second;
        std::sort(entries.begin(), entries.end(),
                  [](const std::pair<double, std::string>& a,
                     const std::pair<double, std::string>& b) {
                      if (a.first != b.first) return a.first < b.first;
                      return a.second < b.second;
                  });
        auto idx = std::make_shared<ZSetIndex>();
        idx->RebuildFrom(entries);
        Install(group.first, std::move(idx), stamp);
    }
}

} // namespace lightkv
