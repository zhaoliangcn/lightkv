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
    std::string db_path = "/tmp/lightkv_geo_test";
    system(("rm -rf " + db_path).c_str());

    Options opts;
    opts.db_path = db_path;
    DB* db = nullptr;
    auto s = DB::Open(opts, &db);
    if (!s.ok()) { std::cerr << "Failed to open DB: " << s.ToString() << std::endl; return 1; }

    ServerOptions srv_opts;
    srv_opts.tcp_port = 16384;
    srv_opts.http_port = 18084;
    Server server(db, srv_opts);
    std::thread server_thread([&server]() { server.Run(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    Client c;
    if (!c.Connect("127.0.0.1", 16384)) {
        std::cerr << "Failed to connect" << std::endl;
        return 1;
    }
    std::cout << "[Geo Test] Connected to server\n" << std::endl;

    // ═══════════════════════════════════════════════
    // 1. GEOADD / GEOPOS Tests
    // ═══════════════════════════════════════════════
    std::cout << "=== GEOADD/GEOPOS Tests ===" << std::endl;

    c.Delete("cities");

    // GEOADD multiple locations
    int64_t added = c.GeoAdd("cities", {{116.40, 39.90, "Beijing"}, {121.47, 31.23, "Shanghai"}});
    TEST("GEOADD adds 2 locations", added == 2);

    // GEOADD with same location (should return 0)
    added = c.GeoAdd("cities", {{116.40, 39.90, "Beijing"}});
    TEST("GEOADD same location returns 0", added == 0);

    // GEOADD with updated location
    added = c.GeoAdd("cities", {{116.41, 39.91, "Beijing"}});
    TEST("GEOADD updated location returns 0", added == 0);

    // GEOPOS
    auto positions = c.GeoPos("cities", {"Beijing", "Shanghai", "NonExistent"});
    TEST("GEOPOS returns 3 positions", positions.size() == 3);

    if (positions.size() >= 2) {
        TEST("GEOPOS Beijing longitude ~116.41", std::abs(positions[0].first - 116.41) < 0.1);
        TEST("GEOPOS Beijing latitude ~39.91", std::abs(positions[0].second - 39.91) < 0.1);
        TEST("GEOPOS Shanghai longitude ~121.47", std::abs(positions[1].first - 121.47) < 0.1);
        TEST("GEOPOS Shanghai latitude ~31.23", std::abs(positions[1].second - 31.23) < 0.1);
    }

    // ═══════════════════════════════════════════════
    // 2. GEODIST Tests
    // ═══════════════════════════════════════════════
    std::cout << "\n=== GEODIST Tests ===" << std::endl;

    // Test GEODIST in meters
    auto dist_m = c.GeoDist("cities", "Beijing", "Shanghai", "m");
    TEST("GEODIST returns distance in meters", dist_m.has_value());
    if (dist_m) {
        // Beijing to Shanghai is approximately 1,068,000 meters
        TEST("GEODIST meters ~1,068,000", *dist_m > 1000000 && *dist_m < 1200000);
    }

    // Test GEODIST in kilometers
    auto dist_km = c.GeoDist("cities", "Beijing", "Shanghai", "km");
    TEST("GEODIST returns distance in km", dist_km.has_value());
    if (dist_km) {
        TEST("GEODIST km ~1068", *dist_km > 1000 && *dist_km < 1200);
    }

    // Test GEODIST with non-existent member
    auto dist_none = c.GeoDist("cities", "Beijing", "NonExistent");
    TEST("GEODIST non-existent returns nullopt", !dist_none.has_value());

    // Test GEODIST default unit (meters)
    auto dist_default = c.GeoDist("cities", "Beijing", "Shanghai");
    TEST("GEODIST default unit is meters", dist_default.has_value());
    if (dist_default && dist_m) {
        TEST("GEODIST default equals meters", std::abs(*dist_default - *dist_m) < 1.0);
    }

    // ═══════════════════════════════════════════════
    // 3. Edge Cases
    // ═══════════════════════════════════════════════
    std::cout << "\n=== Edge Cases ===" << std::endl;

    // Test GEOADD with invalid longitude
    c.Delete("invalid");
    added = c.GeoAdd("invalid", {{200.0, 39.90, "Invalid"}});
    TEST("GEOADD invalid longitude returns error", added == 0);

    // Test GEOADD with invalid latitude
    added = c.GeoAdd("invalid", {{116.40, 100.0, "Invalid"}});
    TEST("GEOADD invalid latitude returns error", added == 0);

    // Test GEOPOS on non-existent key
    auto pos_empty = c.GeoPos("nonexistent", {"Beijing"});
    TEST("GEOPOS on non-existent key returns empty", pos_empty.size() == 1);

    // ═══════════════════════════════════════════════
    // Summary
    // ═══════════════════════════════════════════════
    c.Quit();
    server.Stop();
    server_thread.join();
    delete db;

    std::cout << "\n=== Summary ===" << std::endl;
    std::cout << "Passed: " << passed << ", Failed: " << failed << std::endl;
    return failed == 0 ? 0 : 1;
}
