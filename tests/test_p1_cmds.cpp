#include "lightkv/db.h"
#include "lightkv/server.h"
#include "lightkv/client.h"
#include <iostream>
#include <thread>
#include <chrono>
#include <cassert>
#include <string>
#include <cstdlib>
#include <set>

using namespace lightkv;

static int total_tests = 0;
static int passed_tests = 0;

#define TEST(name, expr) do { \
    total_tests++; \
    bool _ok = (expr); \
    if (_ok) { passed_tests++; std::cout << "  [PASS] " << name << std::endl; } \
    else { std::cout << "  [FAIL] " << name << " (line " << __LINE__ << ")" << std::endl; } \
} while(0)

int main() {
    std::string db_path = "/tmp/lightkv_p1_test";
    system(("rm -rf " + db_path).c_str());

    Options opts;
    opts.db_path = db_path;
    DB* db = nullptr;
    auto s = DB::Open(opts, &db);
    if (!s.ok()) { std::cerr << "Failed to open DB: " << s.ToString() << std::endl; return 1; }

    ServerOptions srv_opts;
    srv_opts.tcp_port = 16380;
    srv_opts.http_port = 18081;
    Server server(db, srv_opts);
    std::thread server_thread([&server]() { server.Run(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    Client c;
    if (!c.Connect("127.0.0.1", 16380)) {
        std::cerr << "Failed to connect" << std::endl;
        return 1;
    }
    std::cout << "[P1 Test] Connected to server\n" << std::endl;

    // ═══════════════════════════════════════════════
    // 1. Hash Commands
    // ═══════════════════════════════════════════════
    std::cout << "=== 1. Hash Commands ===" << std::endl;

    // HSET / HGET
    auto hset_r = c.HSet("user:1", {{"name", "Alice"}, {"age", "30"}, {"city", "Beijing"}});
    TEST("HSET 3 new fields returns 3", hset_r == 3);

    auto hget_name = c.HGet("user:1", "name");
    TEST("HGET name = Alice", hget_name.has_value() && *hget_name == "Alice");
    auto hget_age = c.HGet("user:1", "age");
    TEST("HGET age = 30", hget_age.has_value() && *hget_age == "30");
    auto hget_miss = c.HGet("user:1", "missing");
    TEST("HGET missing field returns nullopt", !hget_miss.has_value());

    // HSET existing field (should return 0)
    auto hset_existing = c.HSet("user:1", {{"name", "Bob"}});
    TEST("HSET existing field returns 0", hset_existing == 0);
    auto hget_new_name = c.HGet("user:1", "name");
    TEST("HGET name updated to Bob", hget_new_name.has_value() && *hget_new_name == "Bob");

    // HMSET / HMGET
    TEST("HMSET", c.HMSet("user:2", {{"x", "1"}, {"y", "2"}, {"z", "3"}}));
    auto hmget = c.HMGet("user:2", {"x", "y", "z", "missing"});
    TEST("HMGET returns 4 values", hmget.size() == 4);
    TEST("HMGET x=1", hmget[0].has_value() && *hmget[0] == "1");
    TEST("HMGET y=2", hmget[1].has_value() && *hmget[1] == "2");
    TEST("HMGET z=3", hmget[2].has_value() && *hmget[2] == "3");
    TEST("HMGET missing=nil", !hmget[3].has_value());

    // HDEL
    auto hdel_r = c.HDel("user:1", {"city", "missing"});
    TEST("HDEL 1 existing + 1 missing = 1", hdel_r == 1);
    auto hget_city = c.HGet("user:1", "city");
    TEST("HGET deleted field returns nullopt", !hget_city.has_value());

    // HEXISTS
    TEST("HEXISTS name = true", c.HExists("user:1", "name"));
    TEST("HEXISTS city = false", !c.HExists("user:1", "city"));

    // HGETALL (after HDEL city)
    auto hgetall = c.HGetAll("user:1");
    TEST("HGETALL returns 2 fields (after HDEL city)", hgetall.size() == 2);
    // HGetAll returns vector of pairs, search for name=Bob
    bool found_name = false, found_age = false;
    for (auto& p : hgetall) {
        if (p.first == "name" && p.second == "Bob") found_name = true;
        if (p.first == "age" && p.second == "30") found_age = true;
    }
    TEST("HGETALL name=Bob", found_name);
    TEST("HGETALL age=30", found_age);

    // HLEN
    auto hlen = c.HLen("user:1");
    TEST("HLEN = 2", hlen == 2);

    // HKEYS
    auto hkeys = c.HKeys("user:2");
    TEST("HKEYS returns 3 keys", hkeys.size() == 3);

    // HVALS
    auto hvals = c.HVals("user:2");
    TEST("HVALS returns 3 values", hvals.size() == 3);

    // HINCRBY
    auto hincr = c.HIncrBy("counter:1", "views", 5);
    TEST("HINCRBY +5 = 5", hincr == 5);
    auto hincr2 = c.HIncrBy("counter:1", "views", 3);
    TEST("HINCRBY +3 = 8", hincr2 == 8);
    auto hincr_neg = c.HIncrBy("counter:1", "views", -2);
    TEST("HINCRBY -2 = 6", hincr_neg == 6);

    // HSTRLEN
    auto hstrlen = c.HStrLen("user:1", "name");
    TEST("HSTRLEN name = 3", hstrlen == 3);
    auto hstrlen_miss = c.HStrLen("user:1", "missing");
    TEST("HSTRLEN missing = 0", hstrlen_miss == 0);

    // Hash type detection
    TEST("TYPE hash = hash", c.Type("user:1") == "hash");

    // ═══════════════════════════════════════════════
    // 2. List Commands
    // ═══════════════════════════════════════════════
    std::cout << "\n=== 2. List Commands ===" << std::endl;

    // LPUSH
    auto lpush_r = c.LPush("mylist", {"a", "b", "c"});
    TEST("LPUSH 3 elements returns 3", lpush_r == 3);

    // LLEN
    auto llen = c.LLen("mylist");
    TEST("LLEN = 3", llen == 3);

    // LINDEX
    auto lidx0 = c.LIndex("mylist", 0);
    TEST("LINDEX 0 = c", lidx0.has_value() && *lidx0 == "c");
    auto lidx1 = c.LIndex("mylist", 1);
    TEST("LINDEX 1 = b", lidx1.has_value() && *lidx1 == "b");
    auto lidx2 = c.LIndex("mylist", 2);
    TEST("LINDEX 2 = a", lidx2.has_value() && *lidx2 == "a");
    auto lidx_neg = c.LIndex("mylist", -1);
    TEST("LINDEX -1 = a", lidx_neg.has_value() && *lidx_neg == "a");
    auto lidx_oob = c.LIndex("mylist", 100);
    TEST("LINDEX out of bounds = nullopt", !lidx_oob.has_value());

    // LRANGE
    auto lrange_all = c.LRange("mylist", 0, -1);
    TEST("LRANGE 0 -1 returns 3", lrange_all.size() == 3);
    TEST("LRANGE 0 -1 [0]=c", lrange_all[0] == "c");
    TEST("LRANGE 0 -1 [2]=a", lrange_all[2] == "a");
    auto lrange_sub = c.LRange("mylist", 0, 1);
    TEST("LRANGE 0 1 returns 2", lrange_sub.size() == 2);
    TEST("LRANGE 0 1 [0]=c", lrange_sub[0] == "c");
    TEST("LRANGE 0 1 [1]=b", lrange_sub[1] == "b");

    // RPush
    auto rpush_r = c.RPush("mylist", {"x", "y"});
    TEST("RPUSH 2 elements returns 5", rpush_r == 5);
    auto lrange_after_rpush = c.LRange("mylist", 0, -1);
    TEST("LRANGE after RPUSH last=y", lrange_after_rpush.back() == "y");

    // LPOP
    auto lpop = c.LPop("mylist");
    TEST("LPOP returns c", lpop.has_value() && *lpop == "c");
    auto llen_after_lpop = c.LLen("mylist");
    TEST("LLEN after LPOP = 4", llen_after_lpop == 4);

    // RPOP
    auto rpop = c.RPop("mylist");
    TEST("RPOP returns y", rpop.has_value() && *rpop == "y");
    auto llen_after_rpop = c.LLen("mylist");
    TEST("LLEN after RPOP = 3", llen_after_rpop == 3);

    // LSET
    TEST("LSET index 0 = NEW", c.LSet("mylist", 0, "NEW"));
    auto lidx_after_set = c.LIndex("mylist", 0);
    TEST("LINDEX 0 after LSET = NEW", lidx_after_set.has_value() && *lidx_after_set == "NEW");

    // LTRIM
    TEST("LTRIM 0 1", c.LTrim("mylist", 0, 1));
    auto llen_after_trim = c.LLen("mylist");
    TEST("LLEN after LTRIM = 2", llen_after_trim == 2);
    auto lrange_after_trim = c.LRange("mylist", 0, -1);
    TEST("LRANGE after LTRIM has 2 elements", lrange_after_trim.size() == 2);

    // LREM
    c.LPush("remlist", {"a", "b", "a", "c", "a"});
    auto lrem_r = c.LRem("remlist", 2, "a");
    TEST("LREM 2 of 'a' returns 2", lrem_r == 2);
    auto llen_after_rem = c.LLen("remlist");
    TEST("LLEN after LREM = 3", llen_after_rem == 3);

    // List type detection
    TEST("TYPE list = list", c.Type("mylist") == "list");

    // ═══════════════════════════════════════════════
    // 3. Set Commands
    // ═══════════════════════════════════════════════
    std::cout << "\n=== 3. Set Commands ===" << std::endl;

    // SADD
    auto sadd_r = c.SAdd("myset", {"a", "b", "c"});
    TEST("SADD 3 new members = 3", sadd_r == 3);

    // SADD duplicate
    auto sadd_dup = c.SAdd("myset", {"b", "d"});
    TEST("SADD 1 dup + 1 new = 1", sadd_dup == 1);

    // SCARD
    auto scard = c.SCard("myset");
    TEST("SCARD = 4", scard == 4);

    // SISMEMBER
    TEST("SISMEMBER a = true", c.SIsMember("myset", "a"));
    TEST("SISMEMBER b = true", c.SIsMember("myset", "b"));
    TEST("SISMEMBER z = false", !c.SIsMember("myset", "z"));

    // SMEMBERS
    auto smembers = c.SMembers("myset");
    TEST("SMEMBERS returns 4", smembers.size() == 4);
    std::set<std::string> member_set(smembers.begin(), smembers.end());
    TEST("SMEMBERS contains a", member_set.count("a"));
    TEST("SMEMBERS contains b", member_set.count("b"));
    TEST("SMEMBERS contains c", member_set.count("c"));
    TEST("SMEMBERS contains d", member_set.count("d"));

    // SREM
    auto srem_r = c.SRem("myset", {"a", "z"});
    TEST("SREM 1 existing + 1 missing = 1", srem_r == 1);
    auto scard_after_rem = c.SCard("myset");
    TEST("SCARD after SREM = 3", scard_after_rem == 3);
    TEST("SISMEMBER a after SREM = false", !c.SIsMember("myset", "a"));

    // SPOP
    auto spop = c.SPop("myset");
    TEST("SPOP returns a member", spop.has_value());
    auto scard_after_pop = c.SCard("myset");
    TEST("SCARD after SPOP = 2", scard_after_pop == 2);

    // SRANDMEMBER
    auto srandmember = c.SRandMember("myset");
    TEST("SRANDMEMBER returns a member", srandmember.has_value());
    auto scard_after_rand = c.SCard("myset");
    TEST("SCARD after SRANDMEMBER unchanged = 2", scard_after_rand == 2);

    // SMOVE
    c.SAdd("srcset", {"x", "y"});
    c.SAdd("dstset", {"z"});
    auto smove_r = c.SMove("srcset", "dstset", "x");
    TEST("SMOVE x from src to dst = true", smove_r);
    TEST("SISMEMBER srcset x = false", !c.SIsMember("srcset", "x"));
    TEST("SISMEMBER dstset x = true", c.SIsMember("dstset", "x"));
    auto smove_fail = c.SMove("srcset", "dstset", "nonexist");
    TEST("SMOVE non-existent member = false", !smove_fail);

    // Set type detection
    TEST("TYPE set = set", c.Type("myset") == "set");

    // ═══════════════════════════════════════════════
    // 4. Cross-type KEYS filtering
    // ═══════════════════════════════════════════════
    std::cout << "\n=== 4. Cross-type KEYS filtering ===" << std::endl;
    // Add a regular key so KEYS has something to return
    c.Set("p1_regular_key", "test_value");
    auto all_keys = c.Keys("*");
    TEST("KEYS * returns user-facing keys", all_keys.size() > 0);
    // Verify no internal keys are returned
    bool has_internal = false;
    for (auto& k : all_keys) {
        if (k.find("__meta__") != std::string::npos) has_internal = true;
    }
    TEST("KEYS does not return internal keys", !has_internal);

    // ═══════════════════════════════════════════════
    // 5. Edge Cases
    // ═══════════════════════════════════════════════
    std::cout << "\n=== 5. Edge Cases ===" << std::endl;

    // Empty hash/list/set
    TEST("HLEN non-existent = 0", c.HLen("nohash") == 0);
    TEST("LLEN non-existent = 0", c.LLen("nolist") == 0);
    TEST("SCARD non-existent = 0", c.SCard("noset") == 0);

    // LPOP/RPOP on empty list
    auto lpop_empty = c.LPop("nolist");
    TEST("LPOP non-existent = nullopt", !lpop_empty.has_value());
    auto rpop_empty = c.RPop("nolist");
    TEST("RPOP non-existent = nullopt", !rpop_empty.has_value());

    // SPOP on empty set
    auto spop_empty = c.SPop("noset");
    TEST("SPOP non-existent = nullopt", !spop_empty.has_value());

    // HINCRBY on non-numeric value
    c.HSet("badcounter", {{"val", "notanumber"}});
    auto hincr_bad = c.HIncrBy("badcounter", "val", 1);
    TEST("HINCRBY on non-numeric returns 0", hincr_bad == 0);

    // LSET out of bounds
    auto lset_oob = c.LSet("mylist", 999, "fail");
    TEST("LSET out of bounds fails", !lset_oob);

    // LREM count=0 (remove all)
    c.LPush("remall", {"a", "a", "a"});
    auto lrem_all = c.LRem("remall", 0, "a");
    TEST("LREM count=0 removes all 3", lrem_all == 3);
    TEST("LLEN after LREM all = 0", c.LLen("remall") == 0);

    // ═══════════════════════════════════════════════
    // 6. Pipeline with P1 commands
    // ═══════════════════════════════════════════════
    std::cout << "\n=== 6. Pipeline with P1 commands ===" << std::endl;
    c.Pipeline();
    c.Queue({"HSET", "pipehash", "f1", "v1"});
    c.Queue({"HGET", "pipehash", "f1"});
    c.Queue({"LPUSH", "pipelist", "a", "b"});
    c.Queue({"LLEN", "pipelist"});
    c.Queue({"SADD", "pipeset", "x", "y"});
    c.Queue({"SCARD", "pipeset"});
    auto pipe_resp = c.ExecPipeline();
    TEST("Pipeline 6 P1 commands returns 6 results", pipe_resp.size() == 6);

    // ═══════════════════════════════════════════════
    // Summary
    // ═══════════════════════════════════════════════
    c.Disconnect();
    server.Stop();
    server_thread.join();
    delete db;

    std::cout << "\n═══════════════════════════════════" << std::endl;
    std::cout << "P1 Test Results: " << passed_tests << "/" << total_tests << " passed" << std::endl;
    std::cout << "═══════════════════════════════════" << std::endl;

    return (passed_tests == total_tests) ? 0 : 1;
}
