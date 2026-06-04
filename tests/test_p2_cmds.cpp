#include "lightkv/db.h"
#include "lightkv/server.h"
#include "lightkv/client.h"
#include <iostream>
#include <thread>
#include <chrono>
#include <string>
#include <cmath>
#include <set>

using namespace lightkv;

static int passed = 0;
static int failed = 0;

#define TEST(name, cond) do { \
    if (cond) { passed++; std::cout << "  PASS: " << name << std::endl; } \
    else { failed++; std::cout << "  FAIL: " << name << std::endl; } \
} while(0)

int main() {
    std::string db_path = "/tmp/lightkv_p2_test";
    system(("rm -rf " + db_path).c_str());

    Options opts;
    opts.db_path = db_path;
    DB* db = nullptr;
    auto s = DB::Open(opts, &db);
    if (!s.ok()) { std::cerr << "Failed to open DB: " << s.ToString() << std::endl; return 1; }

    ServerOptions srv_opts;
    srv_opts.tcp_port = 16382;
    srv_opts.http_port = 18082;
    Server server(db, srv_opts);
    std::thread server_thread([&server]() { server.Run(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    Client c;
    if (!c.Connect("127.0.0.1", 16382)) {
        std::cerr << "Failed to connect" << std::endl;
        return 1;
    }
    std::cout << "[P2 Test] Connected to server\n" << std::endl;

    // ═══════════════════════════════════════════════
    // 1. Bitmap Tests
    // ═══════════════════════════════════════════════
    std::cout << "=== Bitmap Tests ===" << std::endl;

    // SETBIT/GETBIT basic
    auto old0 = c.SetBit("mybitmap", 0, 1);
    TEST("SETBIT offset 0 to 1 returns old bit 0", old0 == 0);

    auto bit0 = c.GetBit("mybitmap", 0);
    TEST("GETBIT offset 0 returns 1", bit0 == 1);

    auto old1 = c.SetBit("mybitmap", 0, 0);
    TEST("SETBIT offset 0 to 0 returns old bit 1", old1 == 1);

    auto bit0_after = c.GetBit("mybitmap", 0);
    TEST("GETBIT offset 0 after clear returns 0", bit0_after == 0);

    // SETBIT at large offset
    auto old_far = c.SetBit("mybitmap", 100, 1);
    TEST("SETBIT offset 100 to 1 returns old bit 0", old_far == 0);

    auto bit_far = c.GetBit("mybitmap", 100);
    TEST("GETBIT offset 100 returns 1", bit_far == 1);

    // GETBIT on non-existent key
    c.Delete("nonexist_bit");
    auto bit_ne = c.GetBit("nonexist_bit", 50);
    TEST("GETBIT on non-existent key returns 0", bit_ne == 0);

    // BITCOUNT
    c.Delete("bitstring");
    c.Set("bitstring", "foobar"); // f=01100110, o=01101111, o=01101111, b=01100010, a=01100001, r=01110010
    // Total set bits: 4+6+6+3+3+5 = 27? Let me count properly:
    // f=0x66=01100110 (4), o=0x6F=01101111 (6), o=0x6F (6), b=0x62=01100010 (3), a=0x61=01100001 (3), r=0x72=01110010 (4)
    // Total: 4+6+6+3+3+4 = 26
    auto bc = c.BitCount("bitstring");
    TEST("BITCOUNT 'foobar' returns 26", bc == 26);

    // BITCOUNT with range
    auto bc_range = c.BitCount("bitstring", 0, 0);
    TEST("BITCOUNT 'foobar' 0 0 returns bits in first byte (f=4)", bc_range == 4);

    // BITPOS
    c.Delete("bitpos_test");
    c.Set("bitpos_test", std::string("\xff\xf0\x00", 3));
    auto bp0 = c.BitPos("bitpos_test", 0);
    TEST("BITPOS 0 in 0xff 0xf0 0x00 returns 12", bp0 == 12);

    auto bp1 = c.BitPos("bitpos_test", 1);
    TEST("BITPOS 1 in 0xff 0xf0 0x00 returns 0", bp1 == 0);

    // BITPOS on empty string looking for 1
    c.Delete("empty_bitpos");
    auto bp_empty1 = c.BitPos("empty_bitpos", 1);
    TEST("BITPOS 1 on empty string returns -1", bp_empty1 == -1);

    std::cout << "\n=== HyperLogLog Tests ===" << std::endl;

    // PFADD basic
    auto pf1 = c.PfAdd("myhll", {"a", "b", "c", "d", "e"});
    TEST("PFADD 5 new elements returns 1", pf1 == 1);

    auto pf2 = c.PfAdd("myhll", {"a", "b"});
    TEST("PFADD existing elements returns 0", pf2 == 0);

    // PFCOUNT
    auto cnt = c.PfCount({"myhll"});
    TEST("PFCOUNT returns ~5 (within 10% error)", cnt >= 4 && cnt <= 6);

    // PFMERGE
    c.Delete("myhll2");
    c.PfAdd("myhll2", {"f", "g", "h"});
    auto merge_ok = c.PfMerge("hllmerge", {"myhll", "myhll2"});
    TEST("PFMERGE returns OK", merge_ok);

    auto merged_cnt = c.PfCount({"hllmerge"});
    TEST("PFCOUNT merged returns ~8 (within 10% error)", merged_cnt >= 6 && merged_cnt <= 10);

    // PFCOUNT multiple keys
    auto multi_cnt = c.PfCount({"myhll", "myhll2"});
    TEST("PFCOUNT multiple keys returns ~8", multi_cnt >= 6 && multi_cnt <= 10);

    // Large cardinality test
    c.Delete("large_hll");
    for (int i = 0; i < 1000; ++i) {
        c.PfAdd("large_hll", {"elem_" + std::to_string(i)});
    }
    auto large_cnt = c.PfCount({"large_hll"});
    // HLL standard error is ~0.81%, so 1000 ± 100 is very generous
    TEST("PFCOUNT 1000 elements within 10% error", large_cnt >= 900 && large_cnt <= 1100);

    std::cout << "\n=== Cross-Type Tests ===" << std::endl;

    // Bitmap on existing string key
    c.Delete("cross_type");
    c.Set("cross_type", "hello");
    auto cross_bit = c.SetBit("cross_type", 0, 1);
    TEST("SETBIT on string key works", cross_bit == 0);

    std::cout << "\n=== Pipeline Tests ===" << std::endl;

    c.Delete("pipe_bitmap");
    c.Pipeline();
    c.Queue({"SETBIT", "pipe_bitmap", "0", "1"});
    c.Queue({"SETBIT", "pipe_bitmap", "10", "1"});
    c.Queue({"SETBIT", "pipe_bitmap", "20", "1"});
    c.Queue({"BITCOUNT", "pipe_bitmap"});
    auto results = c.ExecPipeline();
    TEST("Pipeline bitmap ops returns 4 results", results.size() == 4);

    c.Delete("pipe_hll");
    c.Pipeline();
    c.Queue({"PFADD", "pipe_hll", "x", "y", "z"});
    c.Queue({"PFCOUNT", "pipe_hll"});
    results = c.ExecPipeline();
    TEST("Pipeline hll ops returns 2 results", results.size() == 2);

    std::cout << "\n=== Summary ===" << std::endl;
    std::cout << "Passed: " << passed << std::endl;
    std::cout << "Failed: " << failed << std::endl;

    c.Quit();
    server.Stop();
    server_thread.join();
    delete db;

    return failed > 0 ? 1 : 0;
}
