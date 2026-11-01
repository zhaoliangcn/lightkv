// ZSetIndex: the in-memory sorted index behind ZRANGE / ZRANK / ZCOUNT.
//
// The index keeps two structures in sync (a ranked skip list and a member ->
// score map), so the test drives it against a std::map model and compares every
// query shape it exposes. The model is ordered exactly the way Redis orders a
// sorted set: by score, then by member.
#include "lightkv/zset_index.h"

#include <cassert>
#include <cmath>
#include <cstdio>
#include <map>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

using namespace lightkv;

namespace {

struct Model {
    std::map<std::pair<double, std::string>, char> rows;
    std::unordered_map<std::string, double> score;

    void Upsert(double s, const std::string& m) {
        auto it = score.find(m);
        if (it != score.end()) {
            if (it->second == s) return;
            rows.erase({it->second, m});
            it->second = s;
        } else {
            score.emplace(m, s);
        }
        rows.emplace(std::make_pair(s, m), 0);
    }

    void Remove(const std::string& m) {
        auto it = score.find(m);
        if (it == score.end()) return;
        rows.erase({it->second, m});
        score.erase(it);
    }

    size_t Size() const { return rows.size(); }

    int64_t Rank(const std::string& m) const {
        auto it = score.find(m);
        if (it == score.end()) return -1;
        int64_t rank = 0;
        for (const auto& r : rows) {
            if (r.first.second == m) return rank;
            ++rank;
        }
        return -1;
    }
};

std::vector<std::pair<double, std::string>> Snapshot(const ZSetIndex& idx) {
    std::vector<std::pair<double, std::string>> out;
    idx.RangeByIndex(0, -1, &out);
    return out;
}

std::vector<std::pair<double, std::string>> ModelRows(const Model& model) {
    std::vector<std::pair<double, std::string>> out;
    out.reserve(model.rows.size());
    for (const auto& r : model.rows) out.emplace_back(r.first.first, r.first.second);
    return out;
}

void CheckEqual(const std::vector<std::pair<double, std::string>>& got,
                const std::vector<std::pair<double, std::string>>& want,
                const char* what) {
    if (got.size() != want.size()) {
        std::printf("FAIL %s: size %zu != %zu\n", what, got.size(), want.size());
        std::fflush(stdout);
        assert(false);
    }
    for (size_t i = 0; i < got.size(); ++i) {
        if (got[i].first != want[i].first || got[i].second != want[i].second) {
            std::printf("FAIL %s: [%zu] (%g,%s) != (%g,%s)\n", what, i, got[i].first,
                        got[i].second.c_str(), want[i].first, want[i].second.c_str());
            std::fflush(stdout);
            assert(false);
        }
    }
}

// Redis-style window clipping, duplicated here on purpose: the test must not
// reuse the code it is checking.
void Clip(int64_t n, int64_t* start, int64_t* stop, bool* empty) {
    if (*start < 0) *start = n + *start;
    if (*stop < 0) *stop = n + *stop;
    if (*start < 0) *start = 0;
    if (*stop >= n) *stop = n - 1;
    *empty = (n == 0 || *start > *stop || *start >= n);
}

void TestAgainstModel() {
    ZSetIndex idx;
    Model model;
    std::mt19937 rng(20261101);

    for (int step = 0; step < 20000; ++step) {
        const std::string member = "m" + std::to_string(rng() % 300);
        const double score = static_cast<double>(rng() % 60);
        if (rng() % 4 == 0) {
            const bool removed = idx.Remove(member);
            const bool expected = model.score.find(member) != model.score.end();
            assert(removed == expected);
            model.Remove(member);
        } else {
            idx.Upsert(score, member);
            model.Upsert(score, member);
        }

        if (step % 500 != 0) continue;

        assert(idx.Size() == model.Size());
        CheckEqual(Snapshot(idx), ModelRows(model), "full range");

        // Ranks, including members that are not there.
        for (int probe = 0; probe < 20; ++probe) {
            const std::string m = "m" + std::to_string(rng() % 320);
            assert(idx.Rank(m) == model.Rank(m));
            const int64_t r = model.Rank(m);
            assert(idx.RevRank(m) == (r < 0 ? -1 : static_cast<int64_t>(model.Size()) - 1 - r));
        }

        // Position windows, negative indices included.
        for (int probe = 0; probe < 20; ++probe) {
            const int64_t n = static_cast<int64_t>(model.Size());
            int64_t start = static_cast<int64_t>(rng() % 40) - 20;
            int64_t stop = start + static_cast<int64_t>(rng() % 20) - 5;
            int64_t cs = start, cp = stop;
            bool empty = false;
            Clip(n, &cs, &cp, &empty);
            auto want = ModelRows(model);
            std::vector<std::pair<double, std::string>> expected;
            if (!empty) {
                for (int64_t i = cs; i <= cp; ++i) expected.push_back(want[static_cast<size_t>(i)]);
            }
            std::vector<std::pair<double, std::string>> got;
            idx.RangeByIndex(start, stop, &got);
            CheckEqual(got, expected, "RangeByIndex");

            // Reverse window over the same clipping.
            std::vector<std::pair<double, std::string>> rev_expected;
            if (!empty) {
                for (int64_t i = cs; i <= cp; ++i) {
                    rev_expected.push_back(want[static_cast<size_t>(n - 1 - i)]);
                }
            }
            std::vector<std::pair<double, std::string>> rev_got;
            idx.RevRangeByIndex(start, stop, &rev_got);
            CheckEqual(rev_got, rev_expected, "RevRangeByIndex");
        }

        // Score ranges with both endpoints inclusive/exclusive.
        for (int probe = 0; probe < 10; ++probe) {
            const double lo = static_cast<double>(rng() % 60);
            const double hi = lo + static_cast<double>(rng() % 20);
            const bool lo_excl = (rng() % 2) == 0;
            const bool hi_excl = (rng() % 2) == 0;
            std::vector<std::pair<double, std::string>> expected;
            int64_t count = 0;
            for (const auto& row : ModelRows(model)) {
                const bool in_lo = lo_excl ? row.first > lo : row.first >= lo;
                const bool in_hi = hi_excl ? row.first < hi : row.first <= hi;
                if (in_lo && in_hi) {
                    expected.push_back(row);
                    ++count;
                }
            }
            std::vector<std::pair<double, std::string>> got;
            idx.RangeByScore(lo, hi, lo_excl, hi_excl, &got);
            CheckEqual(got, expected, "RangeByScore");
            assert(idx.CountByScore(lo, hi, lo_excl, hi_excl) == count);
        }
    }
    std::printf("  PASS: 20000 random upserts/removes agree with the model\n");
}

void TestChurnCompacts() {
    // Re-scoring one member repeatedly is the case that used to leak a node per
    // update. The index is expected to keep exactly one entry and to keep
    // answering from it no matter how much churn has gone through.
    ZSetIndex idx;
    for (int i = 0; i < 5000; ++i) {
        idx.Upsert(static_cast<double>(i % 97), "only");
        assert(idx.Size() == 1);
        if (i % 250 == 0) {
            auto rows = Snapshot(idx);
            assert(rows.size() == 1);
            assert(rows[0].second == "only");
            assert(idx.Rank("only") == 0);
        }
    }
    double score = -1;
    assert(idx.Score("only", &score));
    assert(score == static_cast<double>(4999 % 97));
    assert(idx.Remove("only"));
    assert(idx.Size() == 0);
    assert(Snapshot(idx).empty());
    assert(idx.Rank("only") == -1);
    std::printf("  PASS: 5000 re-scores keep a single live entry\n");
}

void TestRebuildClearsAndDedupes() {
    ZSetIndex idx;
    idx.Upsert(1.0, "stale_a");
    idx.Upsert(2.0, "stale_b");

    std::vector<std::pair<double, std::string>> entries = {
        {1.0, "a"}, {2.0, "b"}, {3.0, "c"},
        {9.0, "b"},  // duplicate member: must not create two entries
    };
    idx.RebuildFrom(entries);

    assert(idx.Size() == 3);
    std::vector<std::pair<double, std::string>> want = {{1.0, "a"}, {2.0, "b"}, {3.0, "c"}};
    CheckEqual(Snapshot(idx), want, "rebuild");

    // Rebuilding again from the same input must be idempotent.
    idx.RebuildFrom(entries);
    assert(idx.Size() == 3);
    CheckEqual(Snapshot(idx), want, "rebuild twice");

    double score = 0;
    assert(idx.Score("b", &score) && score == 2.0);
    assert(idx.Rank("c") == 2);
    std::printf("  PASS: RebuildFrom clears old state and collapses duplicates\n");
}

void TestConcurrentReaders() {
    ZSetIndex idx;
    constexpr int kThreads = 4;
    constexpr int kOps = 1500;

    std::vector<std::thread> writers;
    for (int t = 0; t < kThreads; ++t) {
        writers.emplace_back([&idx, t] {
            for (int i = 0; i < kOps; ++i) {
                const std::string m = "t" + std::to_string(t) + "_" + std::to_string(i % 100);
                idx.Upsert(static_cast<double>(i % 40), m);
                if (i % 9 == 0) idx.Remove(m);
            }
        });
    }

    std::vector<std::thread> readers;
    for (int r = 0; r < 2; ++r) {
        readers.emplace_back([&idx] {
            for (int i = 0; i < 3000; ++i) {
                std::vector<std::pair<double, std::string>> rows;
                idx.RangeByIndex(0, 19, &rows);
                idx.RevRangeByIndex(0, 9, &rows);
                idx.RangeByScore(10, 20, false, true, &rows);
                idx.CountByScore(0, 40, false, false);
                const size_t n = idx.Size();
                if (n > 0) idx.Rank("t0_0");
                double score = 0;
                idx.Score("t1_1", &score);
            }
        });
    }

    for (auto& t : writers) t.join();
    for (auto& r : readers) r.join();

    // Every writer's last action for a member decides whether it is in the set;
    // rebuild the expectation from the same rule.
    Model model;
    for (int t = 0; t < kThreads; ++t) {
        for (int i = 0; i < kOps; ++i) {
            const std::string m = "t" + std::to_string(t) + "_" + std::to_string(i % 100);
            model.Upsert(static_cast<double>(i % 40), m);
            if (i % 9 == 0) model.Remove(m);
        }
    }
    assert(idx.Size() == model.Size());
    CheckEqual(Snapshot(idx), ModelRows(model), "concurrent final state");
    std::printf("  PASS: 4 writers + 2 readers, %zu members left\n", idx.Size());
}

void TestHubLifetimeAndStamps() {
    ZSetIndexHub hub;
    assert(hub.Get("z") == nullptr);

    auto idx = std::make_shared<ZSetIndex>();
    idx->Upsert(1.0, "a");
    hub.Put("z", idx);
    assert(hub.Size() == 1);

    // A reader that took the pointer keeps a live index even if the key is
    // dropped underneath it: that is what makes Drop-on-DEL safe.
    auto held = hub.Get("z");
    assert(held != nullptr);
    hub.Drop("z");
    assert(hub.Get("z") == nullptr);
    held->Upsert(2.0, "b");
    assert(held->Size() == 2);

    // A rebuild that started before a write must not be installed.
    const ZSetStamp stale = hub.Stamp("w");
    hub.Bump("w");
    auto rebuilt = std::make_shared<ZSetIndex>();
    rebuilt->Upsert(1.0, "x");
    assert(!hub.Install("w", rebuilt, stale));
    assert(hub.Get("w") == nullptr);
    assert(hub.Install("w", rebuilt, hub.Stamp("w")));
    assert(hub.Get("w") != nullptr);

    // FLUSHDB invalidates every in-flight rebuild, including names the hub has
    // never seen.
    const ZSetStamp before_flush = hub.Stamp("fresh");
    hub.Clear();
    assert(hub.Size() == 0);
    auto after_flush = std::make_shared<ZSetIndex>();
    assert(!hub.Install("fresh", after_flush, before_flush));
    std::printf("  PASS: hub keeps indices alive and rejects stale installs\n");
}

}  // namespace

int main() {
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    std::printf("[ZSetIndex Test]\n");
    TestAgainstModel();
    TestChurnCompacts();
    TestRebuildClearsAndDedupes();
    TestConcurrentReaders();
    TestHubLifetimeAndStamps();
    std::printf("[ZSetIndex Test] all checks passed\n");
    return 0;
}
