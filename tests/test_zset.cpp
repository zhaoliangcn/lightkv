#include "lightkv/db.h"
#include "lightkv/server.h"
#include "lightkv/client.h"
#include <iostream>
#include <thread>
#include <chrono>
#include <string>
#include <cmath>

using namespace lightkv;

static int passed = 0;
static int failed = 0;

#define TEST(name, cond) do { \
    if (cond) { passed++; std::cout << "  PASS: " << name << std::endl; } \
    else { failed++; std::cout << "  FAIL: " << name << std::endl; } \
} while(0)

int main() {
    std::string db_path = "/tmp/lightkv_zset_test";
    system(("rm -rf " + db_path).c_str());

    Options opts;
    opts.db_path = db_path;
    DB* db = nullptr;
    auto s = DB::Open(opts, &db);
    if (!s.ok()) { std::cerr << "Failed to open DB: " << s.ToString() << std::endl; return 1; }

    ServerOptions srv_opts;
    srv_opts.tcp_port = 16383;
    srv_opts.http_port = 18083;
    Server server(db, srv_opts);
    std::thread server_thread([&server]() { server.Run(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    Client c;
    if (!c.Connect("127.0.0.1", 16383)) {
        std::cerr << "Failed to connect" << std::endl;
        return 1;
    }
    std::cout << "[ZSet Test] Connected to server\n" << std::endl;

    // ═══════════════════════════════════════════════
    // 1. ZADD / ZSCORE Tests
    // ═══════════════════════════════════════════════
    std::cout << "=== ZADD/ZSCORE Tests ===" << std::endl;

    c.Delete("myzset");

    // ZADD multiple members
    int64_t added = c.ZAdd("myzset", {{1.0, "one"}, {2.0, "two"}, {3.0, "three"}});
    TEST("ZADD adds 3 members", added == 3);

    // ZSCORE
    auto score = c.ZScore("myzset", "two");
    TEST("ZSCORE returns score for existing member", score.has_value());
    if (score) {
        double val = std::stod(*score);
        TEST("ZSCORE returns correct score", val >= 1.99 && val <= 2.01);
    }

    // ZSCORE for non-existing member
    auto ns = c.ZScore("myzset", "nonexist");
    TEST("ZSCORE returns nullopt for non-existing member", !ns.has_value());

    // ZADD update score (should return 0)
    added = c.ZAdd("myzset", {{2.5, "two"}});
    TEST("ZADD update returns 0 (no new member)", added == 0);
    score = c.ZScore("myzset", "two");
    TEST("ZSCORE returns updated score", score.has_value());
    if (score) {
        double val = std::stod(*score);
        TEST("ZSCORE returns updated value", val >= 2.49 && val <= 2.51);
    }

    // ZADD same score (should return 0)
    added = c.ZAdd("myzset", {{2.5, "two"}});
    TEST("ZADD same score returns 0", added == 0);

    // ═══════════════════════════════════════════════
    // 2. ZCARD / ZREM Tests
    // ═══════════════════════════════════════════════
    std::cout << "\n=== ZCARD/ZREM Tests ===" << std::endl;

    c.Delete("myzset2");
    c.ZAdd("myzset2", {{1.0, "a"}, {2.0, "b"}, {3.0, "c"}, {4.0, "d"}});

    int64_t card = c.ZCard("myzset2");
    TEST("ZCARD returns 4", card == 4);

    int64_t removed = c.ZRem("myzset2", {"b", "d"});
    TEST("ZREM removes 2 members", removed == 2);

    card = c.ZCard("myzset2");
    TEST("ZCARD returns 2 after ZREM", card == 2);

    removed = c.ZRem("myzset2", {"nonexist"});
    TEST("ZREM non-existing returns 0", removed == 0);

    removed = c.ZRem("myzset2", {"a", "c"});
    TEST("ZREM all returns 2", removed == 2);
    card = c.ZCard("myzset2");
    TEST("ZCARD returns 0 after removing all", card == 0);

    // ═══════════════════════════════════════════════
    // 3. ZRANGE / ZREVRANGE Tests
    // ═══════════════════════════════════════════════
    std::cout << "\n=== ZRANGE/ZREVRANGE Tests ===" << std::endl;

    c.Delete("myzset3");
    c.ZAdd("myzset3", {{1.0, "one"}, {2.0, "two"}, {3.0, "three"}, {4.0, "four"}, {5.0, "five"}});

    auto range = c.ZRange("myzset3", 0, -1);
    TEST("ZRANGE 0 -1 returns 5 members", range.size() == 5);
    if (range.size() >= 5) {
        TEST("ZRANGE[0] is one", range[0] == "one");
        TEST("ZRANGE[2] is three", range[2] == "three");
        TEST("ZRANGE[4] is five", range[4] == "five");
    }

    // ZRANGE with scores
    auto ws = c.ZRangeWithScores("myzset3", 0, 2);
    TEST("ZRANGE with scores returns 3", ws.size() == 3);
    if (ws.size() >= 2) {
        TEST("ZRANGE with scores[0].member is one", ws[0].first == "one");
        TEST("ZRANGE with scores[1].score ~ 2.0", ws[1].second >= 1.99 && ws[1].second <= 2.01);
    }

    // ZRANGE partial
    range = c.ZRange("myzset3", 1, 3);
    TEST("ZRANGE 1 3 returns 3 members", range.size() == 3);
    if (range.size() >= 3) {
        TEST("ZRANGE 1 3 [0] is two", range[0] == "two");
        TEST("ZRANGE 1 3 [2] is four", range[2] == "four");
    }

    // ZREVRANGE
    range = c.ZRevRange("myzset3", 0, 2);
    TEST("ZREVRANGE 0 2 returns 3 members", range.size() == 3);
    if (range.size() >= 3) {
        TEST("ZREVRANGE[0] is five", range[0] == "five");
        TEST("ZREVRANGE[1] is four", range[1] == "four");
        TEST("ZREVRANGE[2] is three", range[2] == "three");
    }

    // ZREVRANGE with scores
    range = c.ZRevRange("myzset3", 0, 1, true);
    TEST("ZREVRANGE with scores returns 4 items", range.size() == 4);

    // Negative indices
    range = c.ZRange("myzset3", -2, -1);
    TEST("ZRANGE -2 -1 returns 2 members", range.size() == 2);
    if (range.size() >= 2) {
        TEST("ZRANGE -2 -1 [0] is four", range[0] == "four");
        TEST("ZRANGE -2 -1 [1] is five", range[1] == "five");
    }

    // Empty zset
    c.Delete("emptyzset");
    range = c.ZRange("emptyzset", 0, -1);
    TEST("ZRANGE on empty returns empty", range.empty());

    // ═══════════════════════════════════════════════
    // 4. ZCOUNT / ZRANGEBYSCORE Tests
    // ═══════════════════════════════════════════════
    std::cout << "\n=== ZCOUNT/ZRANGEBYSCORE Tests ===" << std::endl;

    c.Delete("myzset4");
    c.ZAdd("myzset4", {{1.0, "a"}, {2.0, "b"}, {3.0, "c"}, {4.0, "d"}, {5.0, "e"}});

    int64_t cnt = c.ZCount("myzset4", "2", "4");
    TEST("ZCOUNT 2 4 returns 3", cnt == 3);

    cnt = c.ZCount("myzset4", "(2", "(4");
    TEST("ZCOUNT (2 (4 returns 1", cnt == 1);

    cnt = c.ZCount("myzset4", "-inf", "+inf");
    TEST("ZCOUNT -inf +inf returns 5", cnt == 5);

    auto rbs = c.ZRangeByScore("myzset4", "2", "4");
    TEST("ZRANGEBYSCORE 2 4 returns 3", rbs.size() == 3);
    if (rbs.size() >= 3) {
        TEST("ZRANGEBYSCORE[0] is b", rbs[0] == "b");
        TEST("ZRANGEBYSCORE[2] is d", rbs[2] == "d");
    }

    rbs = c.ZRangeByScore("myzset4", "1", "5", 1, 2);
    TEST("ZRANGEBYSCORE with LIMIT returns 2", rbs.size() == 2);
    if (rbs.size() >= 2) {
        TEST("ZRANGEBYSCORE LIMIT[0] is b", rbs[0] == "b");
        TEST("ZRANGEBYSCORE LIMIT[1] is c", rbs[1] == "c");
    }

    rbs = c.ZRangeByScore("myzset4", "2", "4", 0, -1, true);
    TEST("ZRANGEBYSCORE with WITHSCORES returns 6 items", rbs.size() == 6);

    // ═══════════════════════════════════════════════
    // 5. ZRANK / ZREVRANK Tests
    // ═══════════════════════════════════════════════
    std::cout << "\n=== ZRANK/ZREVRANK Tests ===" << std::endl;

    c.Delete("rankzset");
    c.ZAdd("rankzset", {{1.0, "one"}, {2.0, "two"}, {3.0, "three"}, {4.0, "four"}, {5.0, "five"}});

    auto rank = c.ZRank("rankzset", "one");
    TEST("ZRANK returns rank for existing member", rank.has_value());
    if (rank) TEST("ZRANK of one == 0", *rank == 0);
    rank = c.ZRank("rankzset", "three");
    if (rank) TEST("ZRANK of three == 2", *rank == 2);
    rank = c.ZRank("rankzset", "five");
    if (rank) TEST("ZRANK of five == 4", *rank == 4);

    auto revrank = c.ZRevRank("rankzset", "five");
    TEST("ZREVRANK returns rank for existing member", revrank.has_value());
    if (revrank) TEST("ZREVRANK of five == 0", *revrank == 0);
    revrank = c.ZRevRank("rankzset", "three");
    if (revrank) TEST("ZREVRANK of three == 2", *revrank == 2);
    revrank = c.ZRevRank("rankzset", "one");
    if (revrank) TEST("ZREVRANK of one == 4", *revrank == 4);

    // ZRANK/ZREVRANK on non-existing member
    rank = c.ZRank("rankzset", "nonexist");
    TEST("ZRANK returns nullopt for non-existing member", !rank.has_value());
    revrank = c.ZRevRank("rankzset", "nonexist");
    TEST("ZREVRANK returns nullopt for non-existing member", !revrank.has_value());

    // ZRANK/ZREVRANK on empty zset
    c.Delete("emptyrank");
    rank = c.ZRank("emptyrank", "x");
    TEST("ZRANK on empty zset returns nullopt", !rank.has_value());
    revrank = c.ZRevRank("emptyrank", "x");
    TEST("ZREVRANK on empty zset returns nullopt", !revrank.has_value());

    // ZRANK/ZREVRANK on same-score members (tie broken by member name)
    c.Delete("tietest");
    c.ZAdd("tietest", {{1.0, "c"}, {1.0, "a"}, {1.0, "b"}});
    rank = c.ZRank("tietest", "a");
    if (rank) TEST("ZRANK tie [0] is a", *rank == 0);
    rank = c.ZRank("tietest", "b");
    if (rank) TEST("ZRANK tie [1] is b", *rank == 1);
    rank = c.ZRank("tietest", "c");
    if (rank) TEST("ZRANK tie [2] is c", *rank == 2);

    // ═══════════════════════════════════════════════
    // 6. TYPE Test
    // ═══════════════════════════════════════════════
    std::cout << "\n=== TYPE Test ===" << std::endl;

    c.Delete("typezset");
    c.ZAdd("typezset", {{1.0, "a"}});

    auto t = c.Type("typezset");
    TEST("TYPE returns zset", t == "zset");

    // ═══════════════════════════════════════════════
    // 6. Pipeline Test
    // ═══════════════════════════════════════════════
    std::cout << "\n=== Pipeline Test ===" << std::endl;

    c.Delete("pipe_zset");

    c.Pipeline();
    c.Queue({"ZADD", "pipe_zset", "1", "a", "2", "b", "3", "c"});
    c.Queue({"ZCARD", "pipe_zset"});
    c.Queue({"ZRANGE", "pipe_zset", "0", "-1"});
    c.Queue({"ZSCORE", "pipe_zset", "b"});
    auto results = c.ExecPipeline();
    TEST("Pipeline zset ops returns 4 results", results.size() == 4);

    // ═══════════════════════════════════════════════
    // 7. Negative Score Test
    // ═══════════════════════════════════════════════
    std::cout << "\n=== Negative Score Test ===" << std::endl;

    c.Delete("negzset");
    added = c.ZAdd("negzset", {{-5.0, "neg5"}, {-1.0, "neg1"}, {0.0, "zero"}, {3.0, "pos3"}});
    TEST("ZADD with negative scores adds 4", added == 4);

    range = c.ZRange("negzset", 0, -1);
    TEST("ZRANGE with negative scores returns 4", range.size() == 4);
    if (range.size() >= 4) {
        TEST("ZRANGE[0] is neg5", range[0] == "neg5");
        TEST("ZRANGE[2] is zero", range[2] == "zero");
        TEST("ZRANGE[3] is pos3", range[3] == "pos3");
    }

    // ═══════════════════════════════════════════════
    // 8. Same Score Different Members Test
    // ═══════════════════════════════════════════════
    std::cout << "\n=== Same Score Different Members Test ===" << std::endl;

    c.Delete("samescore");
    c.ZAdd("samescore", {{1.0, "c"}, {1.0, "a"}, {1.0, "b"}});
    range = c.ZRange("samescore", 0, -1);
    TEST("ZRANGE same score returns 3 members", range.size() == 3);
    if (range.size() >= 3) {
        // Should be sorted by member name when scores are equal
        TEST("ZRANGE same score [0] is a", range[0] == "a");
        TEST("ZRANGE same score [1] is b", range[1] == "b");
        TEST("ZRANGE same score [2] is c", range[2] == "c");
    }

    // ═══════════════════════════════════════════════
    // Summary
    // ═══════════════════════════════════════════════
    std::cout << "\n[ZSet Test] Summary: " << passed << " passed, " << failed << " failed" << std::endl;

    c.Quit();
    server.Stop();
    server_thread.join();
    delete db;
    system(("rm -rf " + db_path).c_str());

    return (failed == 0) ? 0 : 1;
}
