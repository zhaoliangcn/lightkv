#include "test_paths.h"
#include "lightkv/db.h"
#include "lightkv/server.h"
#include "lightkv/client.h"
#include <iostream>
#include <thread>
#include <chrono>
#include <cassert>
#include <cstdlib>
#include <set>
#include <string>
#include <vector>

using namespace lightkv;

int main() {
    // Unbuffered stdout so progress is visible even if the process aborts
    setvbuf(stdout, nullptr, _IONBF, 0);

    std::string db_path = LIGHTKV_TEST_TMP "/lightkv_server_test";
    
    // Clean up previous test data
    lightkv_remove_tree(db_path);
    
    // Open database
    Options opts;
    opts.db_path = db_path;
    DB* db = nullptr;
    auto s = DB::Open(opts, &db);
    if (!s.ok()) {
        std::cerr << "Failed to open DB: " << s.ToString() << std::endl;
        return 1;
    }
    std::cout << "[Test] Database opened" << std::endl;

    // Start server in background thread
    ServerOptions srv_opts;
    srv_opts.tcp_port = 16385;
    srv_opts.http_port = 18085;
    
    Server server(db, srv_opts);
    std::thread server_thread([&server]() {
        server.Run();
    });

    // Give server time to start
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // Test Client SDK
    Client client;
    
    // Test 1: Connect
    std::cout << "[Test] Connecting to server..." << std::endl;
    if (!client.Connect("127.0.0.1", 16385)) {
        std::cerr << "[FAIL] Connect failed: " << client.last_error() << std::endl;
        return 1;
    }
    std::cout << "[Test] Connected" << std::endl;

    // Test 2: Ping
    std::cout << "[Test] PING..." << std::endl;
    if (!client.Ping()) {
        std::cerr << "[FAIL] Ping failed: " << client.last_error() << std::endl;
        return 1;
    }
    std::cout << "[Test] PONG received" << std::endl;

    // Test 3: Set/Get
    std::cout << "[Test] SET/GET..." << std::endl;
    if (!client.Set("hello", "world")) {
        std::cerr << "[FAIL] SET hello failed" << std::endl;
        return 1;
    }
    auto val = client.Get("hello");
    if (!val.has_value()) {
        std::cerr << "[FAIL] GET hello returned nil (expected 'world')" << std::endl;
        return 1;
    }
    if (*val != "world") {
        std::cerr << "[FAIL] GET hello = '" << *val << "' (expected 'world')" << std::endl;
        return 1;
    }
    std::cout << "[Test] GET hello = " << *val << std::endl;

    // Test 4: Get non-existent key
    std::cout << "[Test] GET non-existent..." << std::endl;
    auto nil = client.Get("nonexistent");
    assert(!nil.has_value());
    std::cout << "[Test] GET nonexistent = nil" << std::endl;

    // Test 5: Delete
    std::cout << "[Test] DELETE..." << std::endl;
    assert(client.Delete("hello"));
    nil = client.Get("hello");
    assert(!nil.has_value());
    std::cout << "[Test] DELETE hello succeeded" << std::endl;

    // Test 6: Set multiple keys
    std::cout << "[Test] SET multiple..." << std::endl;
    client.Set("a", "1");
    client.Set("b", "2");
    client.Set("c", "3");
    client.Set("d", "4");
    std::cout << "[Test] Set 4 keys" << std::endl;

    // Test 7: DeleteRange
    std::cout << "[Test] DELRANGE..." << std::endl;
    assert(client.DeleteRange("a", "c"));
    auto b = client.Get("b");
    assert(!b.has_value());
    auto d = client.Get("d");
    assert(d.has_value() && *d == "4");
    std::cout << "[Test] DELRANGE a-c succeeded, d still exists" << std::endl;

    // Test 8: SCAN cursor semantics
    //
    // The cursor used to be an index into an array that was re-scanned on every
    // call, so any write between two pages silently shifted it. It is now the
    // last key returned, hex-encoded.
    std::cout << "[Test] SCAN..." << std::endl;
    {
        // SendCommand takes argv, so split the pseudo-command on spaces.
        auto send = [&client](const std::string& raw) {
            std::vector<std::string> argv;
            size_t pos = 0;
            while (pos < raw.size()) {
                size_t sp = raw.find(' ', pos);
                if (sp == std::string::npos) sp = raw.size();
                argv.push_back(raw.substr(pos, sp - pos));
                pos = sp + 1;
            }
            return client.SendCommand(argv);
        };
        for (int i = 0; i < 50; ++i) {
            client.Set("scan_" + std::to_string(i), "v");
        }
        // A key that is not a number at all: the cursor must round-trip it.
        client.Set("zzz_last_key", "v");

        // SCAN replies with *2\r\n<cursor bulk>\r\n<keys array>\r\n. Walk it properly:
        // the first bulk string is the cursor, NOT a key.
        auto scan_parse = [](const std::string& raw, std::string* cursor,
                             std::vector<std::string>* keys) {
            size_t p = 4;  // skip "*2\r\n"
            size_t dollar = raw.find("$", p);
            if (dollar == std::string::npos) return false;
            size_t len_end = raw.find("\r\n", dollar);
            size_t len = static_cast<size_t>(
                std::atoi(raw.substr(dollar + 1, len_end - dollar - 1).c_str()));
            *cursor = raw.substr(len_end + 2, len);
            p = len_end + 2 + len + 2;
            size_t star = raw.find("*", p);
            if (star == std::string::npos) return false;
            size_t star_end = raw.find("\r\n", star);
            size_t n = static_cast<size_t>(
                std::atoi(raw.substr(star + 1, star_end - star - 1).c_str()));
            p = star_end + 2;
            for (size_t i = 0; i < n; ++i) {
                dollar = raw.find("$", p);
                if (dollar == std::string::npos) return false;
                len_end = raw.find("\r\n", dollar);
                len = static_cast<size_t>(
                    std::atoi(raw.substr(dollar + 1, len_end - dollar - 1).c_str()));
                keys->push_back(raw.substr(len_end + 2, len));
                p = len_end + 2 + len + 2;
            }
            return true;
        };

        // Walk the whole key space with COUNT 7 and collect what we see.
        std::set<std::string> seen;
        std::string cursor = "0";
        int rounds = 0;
        do {
            std::string raw = send("SCAN " + cursor + " COUNT 7");
            if (raw.rfind("-ERR", 0) == 0) {
                std::cerr << "  SCAN failed: " << raw << std::endl;
                assert(false);
            }
            std::vector<std::string> keys;
            if (!scan_parse(raw, &cursor, &keys)) {
                std::cerr << "  unparseable SCAN reply: " << raw << std::endl;
                assert(false);
            }
            for (const auto& k : keys) {
                if (!seen.insert(k).second) {
                    std::cerr << "  SCAN returned '" << k << "' twice" << std::endl;
                    assert(false);
                }
            }
            if (++rounds > 200) {
                std::cerr << "  SCAN did not terminate" << std::endl;
                assert(false);
            }
        } while (cursor != "0");

        // c and d survive DELRANGE a-c (the range is [a, c)).
        const size_t expected = 50 + 1 + 2;
        if (seen.size() != expected) {
            std::cerr << "  SCAN saw " << seen.size() << " keys, expected "
                      << expected << std::endl;
            for (const auto& k : seen) {
                if (k.rfind("scan_", 0) != 0 && k != "zzz_last_key") {
                    std::cerr << "    extra: '" << k << "'" << std::endl;
                }
            }
        }
        assert(seen.size() == expected);
        assert(seen.count("scan_0") == 1);
        assert(seen.count("scan_49") == 1);
        assert(seen.count("zzz_last_key") == 1);
        assert(seen.count("c") == 1);
        assert(seen.count("d") == 1);
        // Keys deleted before the scan must not reappear.
        assert(seen.count("a") == 0);
        assert(seen.count("b") == 0);

        // A pattern that matches nothing must still terminate rather than
        // handing back a cursor that never advances.
        std::string none = send("SCAN 0 MATCH nomatch* COUNT 7");
        assert(none.find("*2\r\n") != std::string::npos);
        std::string c2;
        std::vector<std::string> none_keys;
        assert(scan_parse(none, &c2, &none_keys));
        if (c2 != "0") {
            std::cerr << "  MATCH matching nothing returned cursor " << c2 << std::endl;
        }
        assert(c2 == "0");

        // MATCH prefix filtering: "scan_1*" matches scan_1 and scan_10..scan_19 only.
        std::string pref = send("SCAN 0 MATCH scan_1* COUNT 100");
        assert(pref.find("scan_10") != std::string::npos);
        assert(pref.find("scan_19") != std::string::npos);
        assert(pref.find("scan_2") == std::string::npos);
        assert(pref.find("scan_49") == std::string::npos);
        assert(pref.find("zzz_last_key") == std::string::npos);
        {
            std::string pc;
            std::vector<std::string> pk;
            assert(scan_parse(pref, &pc, &pk));
            std::set<std::string> want;
            want.insert("scan_1");
            for (int i = 10; i <= 19; ++i) want.insert("scan_" + std::to_string(i));
            std::set<std::string> got(pk.begin(), pk.end());
            if (got != want) {
                std::cerr << "  MATCH scan_1* returned " << got.size()
                          << " keys, expected " << want.size() << std::endl;
            }
            assert(got == want);
        }

        // A malformed cursor is rejected rather than silently restarting.
        assert(send("SCAN zzzz").rfind("-ERR", 0) == 0);

        for (int i = 0; i < 50; ++i) client.Del("scan_" + std::to_string(i));
        client.Del("zzz_last_key");
        std::cout << "[Test] SCAN walked " << seen.size() << " keys once each"
                  << std::endl;
    }

    // Test 9: FLUSHDB
    //
    // FLUSHDB / FLUSHALL were listed as replicated writes but had no handler at
    // all, so clients got "unknown command" for a command the cluster path
    // advertised as supported.
    std::cout << "[Test] FLUSHDB..." << std::endl;
    client.Set("flush_me", "1");
    client.Set("flush_me2", "2");
    assert(client.Get("flush_me").has_value());
    client.FlushDB();
    if (client.Get("flush_me").has_value() || client.Get("flush_me2").has_value()) {
        std::cerr << "  keys survived FLUSHDB" << std::endl;
        assert(false);
    }
    // The server must still be usable afterwards.
    client.Set("after_flush", "3");
    auto after = client.Get("after_flush");
    assert(after.has_value() && *after == "3");
    client.Del("after_flush");
    std::cout << "[Test] FLUSHDB removed all keys, server still serving" << std::endl;

    // Test 9: DBSIZE
    //
    // Used to return a hardcoded 0 behind a "// Approximate by scanning"
    // comment describing a scan that was never written.
    std::cout << "[Test] DBSIZE..." << std::endl;
    {
        client.FlushDB();
        auto size_of = [&client]() -> long long {
            std::string raw = client.SendCommand({"DBSIZE"});
            if (raw.rfind(":") != 0) return -1;
            return std::atoll(raw.c_str() + 1);
        };
        if (size_of() != 0) {
            std::cerr << "  empty DB reported DBSIZE " << size_of() << std::endl;
        }
        assert(size_of() == 0);

        for (int i = 0; i < 25; ++i) client.Set("sz_" + std::to_string(i), "v");
        if (size_of() != 25) {
            std::cerr << "  25 keys reported DBSIZE " << size_of() << std::endl;
        }
        assert(size_of() == 25);

        // Internal metadata keys must not be counted.
        //
        // NOTE: a set lives under a "__set_" prefixed blob, and that prefixed
        // key is itself classified internal -- so DBSIZE counts zero for it.
        // Redis would count 1 (the set is one key there). This is a real
        // divergence from Redis, recorded in docs/缺陷修复方案.md, not something
        // this test papers over.
        client.SendCommand({"SADD", "dbsz_set", "a", "b"});
        long long with_set = size_of();
        if (with_set != 25) {
            std::cerr << "  DBSIZE changed unexpectedly after SADD: " << with_set
                      << std::endl;
        }
        assert(with_set == 25);

        for (int i = 0; i < 25; ++i) client.Del("sz_" + std::to_string(i));
        client.SendCommand({"DEL", "dbsz_set"});
        assert(size_of() == 0);
        std::cout << "[Test] DBSIZE matches the live key count" << std::endl;
    }

    // Test 10: SRANDMEMBER is actually random
    //
    // It used to return elems[n/2] behind a "Deterministic" comment, making it
    // a fixed lookup rather than a sample.
    std::cout << "[Test] SRANDMEMBER..." << std::endl;
    {
        client.SendCommand({"SADD", "rnd", "a", "b", "c", "d", "e", "f", "g", "h"});
        std::set<std::string> drawn;
        for (int i = 0; i < 60; ++i) {
            std::string r = client.SendCommand({"SRANDMEMBER", "rnd"});
            if (r.rfind("$") != 0) {
                std::cerr << "  SRANDMEMBER failed: " << r << std::endl;
                assert(false);
            }
            size_t nl = r.find("\r\n");
            size_t len = static_cast<size_t>(std::atoi(r.substr(1, nl - 1).c_str()));
            drawn.insert(r.substr(nl + 2, len));
        }
        // With 8 members and 60 draws, a fixed "middle element" would yield 1
        // distinct value. Require a decent spread instead.
        if (drawn.size() < 4) {
            std::cerr << "  SRANDMEMBER produced only " << drawn.size()
                      << " distinct members in 60 draws" << std::endl;
        }
        assert(drawn.size() >= 4);
        for (const auto& m : drawn) {
            assert(m.size() == 1 && m[0] >= 'a' && m[0] <= 'h');
        }
        client.SendCommand({"DEL", "rnd"});
        std::cout << "[Test] SRANDMEMBER produced " << drawn.size()
                  << " distinct members in 60 draws" << std::endl;
    }

    // Test 11: Stats
    std::cout << "[Test] STATS..." << std::endl;
    auto stats = client.Stats();
    assert(!stats.empty());
    for (auto& [k, v] : stats) {
        std::cout << "  " << k << " = " << v << std::endl;
    }

    // Test 12: Pub/Sub actually delivers
    //
    // SUBSCRIBE / PSUBSCRIBE / WATCH all hardcoded fd = -1, so
    // connections_.find(-1) always failed and no message could ever reach a
    // client -- while PUBLISH still reported a non-zero receiver count.
    std::cout << "[Test] Pub/Sub..." << std::endl;
    {
        constexpr const char* kHost = "127.0.0.1";
        constexpr uint16_t kPort = 16385;

        // The pushed message arrives ahead of the PING reply, so the first read
        // on this connection returns the message itself.
        auto recv_push = [](lightkv::Client& c) -> std::string {
            return c.SendCommand({"PING"});
        };

        lightkv::Client sub;
        lightkv::Client pub;
        if (!sub.Connect(kHost, kPort) || !pub.Connect(kHost, kPort)) {
            std::cerr << "  could not open the two pub/sub connections" << std::endl;
            assert(false);
        }

        std::string r = sub.SendCommand({"SUBSCRIBE", "news"});
        if (r.rfind("*3") != 0) {
            std::cerr << "  SUBSCRIBE failed: " << r << std::endl;
            assert(false);
        }

        // PUBLISH reports how many subscribers actually received it.
        r = pub.SendCommand({"PUBLISH", "news", "hello"});
        if (r != ":1\r\n") {
            std::cerr << "  PUBLISH reported '" << r << "', expected :1 (one receiver)"
                      << std::endl;
        }
        assert(r == ":1\r\n");

        // Nobody is subscribed to this channel.
        assert(pub.SendCommand({"PUBLISH", "nosuch", "x"}) == ":0\r\n");

        // The pushed message is delivered asynchronously: PUBLISH only queues it and
        // the event loop flushes it on its next turn. Poll rather than assuming
        // it is already on the wire (which is also how Redis behaves).
        std::string pushed;
        for (int i = 0; i < 100; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
            pushed = recv_push(sub);
            if (pushed.find("message") != std::string::npos) break;
        }
        if (pushed.find("message") == std::string::npos ||
            pushed.find("news") == std::string::npos ||
            pushed.find("hello") == std::string::npos) {
            std::cerr << "  subscriber did not receive the message: " << pushed
                      << std::endl;
        }
        assert(pushed.find("message") != std::string::npos);
        assert(pushed.find("news") != std::string::npos);
        assert(pushed.find("hello") != std::string::npos);

        // A second client unsubscribing must not disturb the first client's
        // subscription -- UNSUBSCRIBE used to call WatchHub::Clear(), wiping
        // every other client's watchers.
        lightkv::Client sub2;
        assert(sub2.Connect(kHost, kPort));
        sub2.SendCommand({"SUBSCRIBE", "other"});
        sub2.SendCommand({"UNSUBSCRIBE"});
        assert(pub.SendCommand({"PUBLISH", "news", "still-here"}) == ":1\r\n");

        // After sub unsubscribes, nobody receives it any more.
        sub.SendCommand({"UNSUBSCRIBE"});
        assert(pub.SendCommand({"PUBLISH", "news", "gone"}) == ":0\r\n");

        sub.Disconnect();
        sub2.Disconnect();
        pub.Disconnect();
        std::cout << "[Test] Pub/Sub delivered and unsubscribed per connection"
                  << std::endl;
    }

    // Test 13: Quit
    std::cout << "[Test] QUIT..." << std::endl;
    client.Quit();
    assert(!client.IsConnected());
    std::cout << "[Test] Disconnected" << std::endl;

    // Stop server
    server.Stop();
    server_thread.join();
    delete db;

    std::cout << "\n[All tests passed!]" << std::endl;
    return 0;
}
