#include "lightkv/db.h"
#include "lightkv/server.h"
#include "lightkv/client.h"
#include <iostream>
#include <thread>
#include <chrono>
#include <cassert>
#include <string>
#include <cstdlib>
#include <cmath>

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
    std::string db_path = "/tmp/lightkv_p0_test";
    system(("rm -rf " + db_path).c_str());

    Options opts;
    opts.db_path = db_path;
    DB* db = nullptr;
    auto s = DB::Open(opts, &db);
    if (!s.ok()) { std::cerr << "Failed to open DB: " << s.ToString() << std::endl; return 1; }

    ServerOptions srv_opts;
    srv_opts.tcp_port = 16379;
    srv_opts.http_port = 18080;
    Server server(db, srv_opts);
    std::thread server_thread([&server]() { server.Run(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    Client c;
    if (!c.Connect("127.0.0.1", 16379)) {
        std::cerr << "Failed to connect" << std::endl;
        return 1;
    }
    std::cout << "[P0 Test] Connected to server\n" << std::endl;

    // ═══════════════════════════════════════════════
    // 1. Basic Operations (existing)
    // ═══════════════════════════════════════════════
    std::cout << "=== 1. Basic Operations ===" << std::endl;
    TEST("PING", c.Ping());
    TEST("SET", c.Set("k1", "v1"));
    auto g1 = c.Get("k1");
    TEST("GET", g1.has_value() && *g1 == "v1");
    auto g2 = c.Get("nonexist");
    TEST("GET non-existent", !g2.has_value());
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // ═══════════════════════════════════════════════
    // 2. String Extension — INCR / DECR / INCRBY / DECRBY
    // ═══════════════════════════════════════════════
    std::cout << "\n=== 2. String Extension: INCR/DECR/INCRBY/DECRBY ===" << std::endl;
    // Use separate keys for each test to avoid interference
    auto r1 = c.Incr("cnt_a");
    TEST("INCR on new key returns 1", r1.has_value() && *r1 == 1);
    auto r2 = c.Incr("cnt_a");
    TEST("INCR existing returns 2", r2.has_value() && *r2 == 2);
    auto r3 = c.Incr("cnt_a");
    TEST("INCR returns 3", r3.has_value() && *r3 == 3);

    auto r4 = c.Decr("dcnt_a");
    TEST("DECR on new key returns -1", r4.has_value() && *r4 == -1);
    auto r5 = c.Decr("dcnt_a");
    TEST("DECR returns -2", r5.has_value() && *r5 == -2);

    // INCRBY each on a fresh key
    c.Set("iby", "0");
    auto r6 = c.IncrBy("iby", 5);
    TEST("INCRBY +5", r6.has_value() && *r6 == 5);
    auto r7 = c.IncrBy("iby", 3);
    TEST("INCRBY +3 = 8", r7.has_value() && *r7 == 8);

    c.Set("dby", "10");
    auto r8 = c.DecrBy("dby", 3);
    TEST("DECRBY -3 from 10", r8.has_value() && *r8 == 7);
    auto r9 = c.DecrBy("dby", 5);
    TEST("DECRBY -5 = 2", r9.has_value() && *r9 == 2);

    // Verify via GET
    auto cv = c.Get("cnt_a");
    TEST("GET cnt_a = 3", cv.has_value() && *cv == "3");

    // ═══════════════════════════════════════════════
    // 3. String Extension — INCRBYFLOAT
    // ═══════════════════════════════════════════════
    std::cout << "\n=== 3. String Extension: INCRBYFLOAT ===" << std::endl;
    auto fval = c.IncrByFloat("fkey", 3.14);
    TEST("INCRBYFLOAT returns string", fval.has_value());
    auto fget = c.Get("fkey");
    TEST("INCRBYFLOAT stored", fget.has_value());

    // ═══════════════════════════════════════════════
    // 4. String Extension — MSET / MGET
    // ═══════════════════════════════════════════════
    std::cout << "\n=== 4. String Extension: MSET/MGET ===" << std::endl;
    std::vector<std::pair<std::string, std::string>> kvs = {{"ma", "1"}, {"mb", "2"}, {"mc", "3"}};
    TEST("MSET", c.MSet(kvs));

    auto mget = c.MGet({"ma", "mb", "mc", "missing"});
    TEST("MGET count = 4", mget.size() == 4);
    TEST("MGET ma=1", mget.size() > 0 && mget[0].has_value() && *mget[0] == "1");
    TEST("MGET mb=2", mget.size() > 1 && mget[1].has_value() && *mget[1] == "2");
    TEST("MGET mc=3", mget.size() > 2 && mget[2].has_value() && *mget[2] == "3");
    TEST("MGET missing=nil", mget.size() > 3 && !mget[3].has_value());

    // ═══════════════════════════════════════════════
    // 5. String Extension — SETEX / SETNX / GETSET / APPEND / STRLEN
    // ═══════════════════════════════════════════════
    std::cout << "\n=== 5. String Extension: SETEX/SETNX/GETSET/APPEND/STRLEN ===" << std::endl;
    TEST("SETEX", c.SetEx("exkey", 100, "expires"));
    auto exval = c.Get("exkey");
    TEST("SETEX value correct", exval.has_value() && *exval == "expires");
    auto ttl_ex = c.Ttl("exkey");
    TEST("SETEX has TTL > 0", ttl_ex > 0 && ttl_ex <= 100);

    TEST("SETNX new key", c.SetNx("nxnew", "first"));
    TEST("SETNX existing key returns 0", !c.SetNx("nxnew", "second"));
    auto nxval = c.Get("nxnew");
    TEST("SETNX value unchanged", nxval.has_value() && *nxval == "first");

    c.Set("gs2", "oldval");
    auto gs_old = c.GetSet("gs2", "newval");
    TEST("GETSET returns old value", gs_old.has_value() && *gs_old == "oldval");
    auto gs_new = c.Get("gs2");
    TEST("GETSET sets new value", gs_new.has_value() && *gs_new == "newval");

    c.Set("ap", "hello");
    auto ap_len = c.Append("ap", " world");
    TEST("APPEND returns length 11", ap_len == 11);
    auto apval = c.Get("ap");
    TEST("APPEND result", apval.has_value() && *apval == "hello world");

    TEST("STRLEN = 11", c.StrLen("ap") == 11);
    TEST("STRLEN non-existent = 0", c.StrLen("noexist") == 0);

    c.Set("gr", "hello world");
    auto gr_val = c.Get("gr");
    TEST("GETRANGE setup ok", gr_val.has_value());

    // ═══════════════════════════════════════════════
    // 6. General Commands — EXISTS / TYPE
    // ═══════════════════════════════════════════════
    std::cout << "\n=== 6. General Commands: EXISTS/TYPE ===" << std::endl;
    TEST("EXISTS existing = 1", c.Exists({"k1"}) == 1);
    TEST("EXISTS multi (2 of 3)", c.Exists({"k1", "ma", "noexist"}) == 2);
    TEST("EXISTS non-existent = 0", c.Exists({"noexist"}) == 0);
    TEST("TYPE string", c.Type("k1") == "string");
    TEST("TYPE none", c.Type("notexist") == "none");

    // ═══════════════════════════════════════════════
    // 7. General Commands — EXPIRE / TTL / PTTL / PERSIST
    // ═══════════════════════════════════════════════
    std::cout << "\n=== 7. General Commands: EXPIRE/TTL/PTTL/PERSIST ===" << std::endl;
    TEST("EXPIRE returns 1", c.Expire("k1", 100));
    auto ttl = c.Ttl("k1");
    TEST("TTL > 0", ttl > 0 && ttl <= 100);
    auto pttl = c.Pttl("k1");
    TEST("PTTL > 0 (ms)", pttl > 0 && pttl <= 100000);
    TEST("PERSIST returns 1", c.Persist("k1"));
    TEST("TTL after PERSIST = -2", c.Ttl("k1") == -2);
    // PERSIST on key without TTL should return 0
    auto persist_no_ttl = c.Persist("k1");
    TEST("PERSIST on no-ttl key returns 0", persist_no_ttl == false);

    // ═══════════════════════════════════════════════
    // 8. General Commands — RENAME / RENAMENX
    // ═══════════════════════════════════════════════
    std::cout << "\n=== 8. General Commands: RENAME/RENAMENX ===" << std::endl;
    c.Set("old", "value");
    TEST("RENAME", c.Rename("old", "new"));
    auto newval = c.Get("new");
    TEST("RENAME new key has value", newval.has_value() && *newval == "value");
    auto oldval = c.Get("old");
    TEST("RENAME old key deleted", !oldval.has_value());

    c.Set("target", "existing");
    auto rnm_res = c.RenameNx("new", "target");
    TEST("RENAMENX on existing target returns 0", !rnm_res);
    auto rnm_res2 = c.RenameNx("new", "dest");
    TEST("RENAMENX on new target returns 1", rnm_res2 == true);
    auto destval = c.Get("dest");
    TEST("RENAMENX dest has value", destval.has_value() && *destval == "value");

    // ═══════════════════════════════════════════════
    // 9. General Commands — KEYS
    // ═══════════════════════════════════════════════
    std::cout << "\n=== 9. General Commands: KEYS ===" << std::endl;
    c.Set("akey", "a");
    c.Set("bkey", "b");
    auto all_keys = c.Keys("*");
    TEST("KEYS * returns results", all_keys.size() > 1);
    bool found_k1 = false;
    for (auto& k : all_keys) {
        if (k == "k1") found_k1 = true;
    }
    TEST("KEYS * contains 'k1'", found_k1);

    // ═══════════════════════════════════════════════
    // 10. DEL multi-key
    // ═══════════════════════════════════════════════
    std::cout << "\n=== 10. DEL multi-key ===" << std::endl;
    c.Pipeline();
    c.Queue({"DEL", "ma", "mb", "mc"});
    auto del_resp = c.ExecPipeline();
    TEST("DEL multi-key returns response", !del_resp.empty());
    auto del_check = c.Exists({"ma", "mb", "mc"});
    TEST("DEL actually deleted", del_check == 0);

    // ═══════════════════════════════════════════════
    // 11. Edge Cases
    // ═══════════════════════════════════════════════
    std::cout << "\n=== 11. Edge Cases ===" << std::endl;

    auto large_r = c.IncrBy("large", 999999999);
    TEST("INCRBY large value", large_r.has_value());

    // Empty string value
    TEST("SET empty string", c.Set("empty", ""));
    auto empty_get = c.Get("empty");
    TEST("GET empty string", empty_get.has_value() && *empty_get == "");

    // Key with special characters
    TEST("SET key with spaces", c.Set("key with spaces", "val"));
    auto sp_get = c.Get("key with spaces");
    TEST("GET key with spaces", sp_get.has_value() && *sp_get == "val");

    // Very long key (1000 chars)
    std::string long_key(1000, 'x');
    TEST("SET long key", c.Set(long_key, "longval"));
    auto lk_get = c.Get(long_key);
    TEST("GET long key", lk_get.has_value() && *lk_get == "longval");

    // ═══════════════════════════════════════════════
    // 12. Pipeline with new commands
    // ═══════════════════════════════════════════════
    std::cout << "\n=== 12. Pipeline ===" << std::endl;
    c.Pipeline();
    c.Queue({"INCR", "pcnt"});
    c.Queue({"INCR", "pcnt"});
    c.Queue({"GET", "pcnt"});
    c.Queue({"EXISTS", "pcnt"});
    c.Queue({"TYPE", "pcnt"});
    c.Queue({"STRLEN", "pcnt"});
    auto pipe_resp = c.ExecPipeline();
    TEST("Pipeline 6 commands returns 6 results", pipe_resp.size() == 6);

    // ═══════════════════════════════════════════════
    // 13. DECRBY with zero and negative
    // ═══════════════════════════════════════════════
    std::cout << "\n=== 13. DECRBY edge cases ===" << std::endl;
    c.Set("zero_test", "5");
    auto dec0 = c.DecrBy("zero_test", 0);
    TEST("DECRBY 0", dec0.has_value() && *dec0 == 5);
    auto dec_neg = c.DecrBy("zero_test", -2);
    TEST("DECRBY -2 (same as +2)", dec_neg.has_value() && *dec_neg == 7);

    // ═══════════════════════════════════════════════
    // Summary
    // ═══════════════════════════════════════════════
    c.Disconnect();
    server.Stop();
    server_thread.join();
    delete db;

    std::cout << "\n═══════════════════════════════════" << std::endl;
    std::cout << "P0 Test Results: " << passed_tests << "/" << total_tests << " passed" << std::endl;
    std::cout << "═══════════════════════════════════" << std::endl;

    return (passed_tests == total_tests) ? 0 : 1;
}
