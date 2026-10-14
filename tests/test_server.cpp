#include "test_paths.h"
#include "lightkv/db.h"
#include "lightkv/server.h"
#include "lightkv/client.h"
#include "lightkv/encoding.h"
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

    // Test 13: Replication must not lie
    //
    // REPLICAOF host port used to set is_slave_/readonly and then
    // `return resp_ok()` with a `// TODO: Connect to new master`. An operator
    // would read that as "replicating", INFO would claim role:slave, and the
    // node would silently never receive a write.
    std::cout << "[Test] Replication..." << std::endl;
    {
        // PSYNC forces a full RDB dump. generate_rdb_snapshot() used to page the
        // dump with "Scan(limit=100) in a loop", but DB::Scan has no cursor --
        // every call restarts at the first key. With >= 100 keys it therefore
        // re-read the same first 100 forever, appending each batch again until
        // the process ran out of memory, on the event loop thread.
        // So make sure the database is comfortably past that threshold.
        for (int i = 0; i < 250; ++i) {
            client.Set("rdb_" + std::to_string(i), std::string(32, 'v'));
        }
        long long size = 0;
        {
            std::string sz = client.SendCommand({"DBSIZE"});
            if (sz.rfind(":") == 0) size = std::atoll(sz.c_str() + 1);
        }
        if (size < 100) {
            std::cerr << "  need >= 100 keys to exercise RDB paging, have " << size
                      << std::endl;
        }
        assert(size >= 100);

        lightkv::Client rc;
        assert(rc.Connect("127.0.0.1", 16385));
        std::string r = rc.SendCommand({"REPLICAOF", "127.0.0.1", "16399"});
        if (r.rfind("-ERR", 0) != 0) {
            std::cerr << "  REPLICAOF reported success without a slave client: " << r
                      << std::endl;
        }
        assert(r.rfind("-ERR", 0) == 0);

        // PSYNC / REPLCONF are the master-side half and do work.
        assert(rc.SendCommand({"REPLCONF", "listening-port", "16399"}).rfind("+OK", 0) == 0);
        assert(rc.SendCommand({"REPLCONF", "capa", "psync2"}).rfind("+OK", 0) == 0);
        // A non-numeric ACK offset is rejected rather than silently dropped.
        assert(rc.SendCommand({"REPLCONF", "ack", "notanumber"}).rfind("-ERR", 0) == 0);

        // PSYNC with "?" forces FULLRESYNC. The reply is a bulk string; the
        // important part is that it does NOT hang or crash -- generate_rdb_snapshot
        // used to loop forever once the database held >= 100 keys.
        std::string ps = rc.SendCommand({"PSYNC", "?", "-1"});
        if (ps.rfind("$", 0) != 0) {
            std::cerr << "  PSYNC failed: " << ps << std::endl;
        }
        assert(ps.rfind("$", 0) == 0);

        // The master follows the reply with a full dump. Drain it, or the next
        // command's reply read lands in the middle of the RDB payload. This
        // used to be a no-op because Windows never actually sent the dump.
        std::string rdb_head = rc.ReadLineRaw();
        if (rdb_head.empty() || rdb_head[0] != '$') {
            std::cerr << "  expected an RDB bulk header after PSYNC, got: "
                      << rdb_head << std::endl;
        }
        assert(!rdb_head.empty() && rdb_head[0] == '$');
        long long rdb_size = std::stoll(rdb_head.substr(1));
        if (rdb_size <= 6) {
            std::cerr << "  RDB payload implausibly small: " << rdb_size << std::endl;
        }
        assert(rdb_size > 6);
        std::string rdb_body = rc.ReadRaw(static_cast<size_t>(rdb_size) + 2);
        if (rdb_body.size() != static_cast<size_t>(rdb_size) + 2) {
            std::cerr << "  short RDB read: got " << rdb_body.size() << " of "
                      << (rdb_size + 2) << " bytes" << std::endl;
        }
        assert(rdb_body.size() == static_cast<size_t>(rdb_size) + 2);
        assert(rdb_body.compare(0, 6, "LKVDB0") == 0);
        assert(rdb_body.compare(rdb_body.size() - 2, 2, "\r\n") == 0);

        // The trailer must be a real checksum, not the placeholder 0 the master
        // used to write.
        uint32_t trailer = 0;
        for (int i = 0; i < 4; ++i) {
            trailer |= static_cast<uint32_t>(
                           static_cast<uint8_t>(rdb_body[rdb_size - 4 + i]))
                       << (8 * i);
        }
        if (trailer == 0) {
            std::cerr << "  RDB trailer is the placeholder 0, not a checksum"
                      << std::endl;
        }
        assert(trailer != 0);
        // The checksum covers everything ahead of the trailer itself.
        const std::string payload =
            rdb_body.substr(0, static_cast<size_t>(rdb_size) - 4);
        if (Crc32c(payload.data(), payload.size()) != trailer) {
            std::cerr << "  RDB trailer does not match the payload checksum"
                      << std::endl;
        }
        assert(Crc32c(payload.data(), payload.size()) == trailer);

        // INFO replication should now report a real slave count.
        std::string info = rc.SendCommand({"INFO", "replication"});
        if (info.find("connected_slaves:") == std::string::npos) {
            std::cerr << "  INFO replication missing connected_slaves" << std::endl;
        }
        assert(info.find("connected_slaves:") != std::string::npos);

        rc.Disconnect();
        for (int i = 0; i < 250; ++i) client.Del("rdb_" + std::to_string(i));
        std::cout << "[Test] Replication refuses to pretend" << std::endl;
    }

    // Test 14: CONFIG must not answer "unset" for "unsupported"
    //
    // CONFIG GET returned an empty array for every parameter except
    // requirepass. An empty array is a valid RESP answer meaning "this parameter
    // has no value", so a client reading maxmemory or appendonly would believe
    // the setting is unset rather than that the server does not implement it.
    std::cout << "[Test] CONFIG..." << std::endl;
    {
        lightkv::Client cc;
        assert(cc.Connect("127.0.0.1", 16385));
        std::string r = cc.SendCommand({"CONFIG", "GET", "requirepass"});
        if (r.rfind("*2", 0) != 0) {
            std::cerr << "  CONFIG GET requirepass failed: " << r << std::endl;
        }
        assert(r.rfind("*2", 0) == 0);

        r = cc.SendCommand({"CONFIG", "GET", "maxmemory"});
        if (r.rfind("-ERR", 0) != 0) {
            std::cerr << "  CONFIG GET maxmemory answered '" << r
                      << "' instead of reporting it is unsupported" << std::endl;
        }
        assert(r.rfind("-ERR", 0) == 0);

        // SET already reported unsupported parameters honestly; keep both sides
        // consistent.
        assert(cc.SendCommand({"CONFIG", "SET", "maxmemory", "100mb"})
                   .rfind("-ERR", 0) == 0);
        assert(cc.SendCommand({"CONFIG", "NOSUCH"}).rfind("-ERR", 0) == 0);

        cc.Disconnect();
        std::cout << "[Test] CONFIG distinguishes unsupported from unset"
                  << std::endl;
    }

    // ── Master/replica end-to-end ──────────────────────────────────
    //
    // Two real servers on real ports: one acts as the master, the other issues
    // REPLICAOF against it. This is the first test in the repo that actually
    // exercises the replication link -- AddSlave previously had no caller, so
    // nothing was ever registered as a slave and feed_replication() returned on
    // its first line.
    {
        const std::string m_db = LIGHTKV_TEST_TMP "/lightkv_repl_master";
        const std::string r_db = LIGHTKV_TEST_TMP "/lightkv_repl_replica";
const std::string e_db = LIGHTKV_TEST_TMP "/lightkv_repl_empty";
        lightkv_remove_tree(m_db);
        lightkv_remove_tree(r_db);

        Options m_opts;
        m_opts.db_path = m_db;
        DB* master_db = nullptr;
        if (!DB::Open(m_opts, &master_db).ok()) {
            std::cerr << "  cannot open the master DB" << std::endl;
            assert(false);
        }

        ServerOptions m_srv;
        m_srv.tcp_port = 16390;
        m_srv.http_port = 18090;
        Server master(master_db, m_srv);
        std::thread m_thread([&master]() { master.Run(); });

        Options r_opts;
        r_opts.db_path = r_db;
        DB* replica_db = nullptr;
        if (!DB::Open(r_opts, &replica_db).ok()) {
            std::cerr << "  cannot open the replica DB" << std::endl;
            assert(false);
        }

        ServerOptions r_srv;
        r_srv.tcp_port = 16391;
        r_srv.http_port = 18091;
        Server replica(replica_db, r_srv);
        std::thread r_thread([&replica]() { replica.Run(); });

        std::this_thread::sleep_for(std::chrono::milliseconds(400));

        Client mc;
        Client rc;
        assert(mc.Connect("127.0.0.1", 16390));
        assert(rc.Connect("127.0.0.1", 16391));

        // Seed the master BEFORE the replica attaches, so the replica has to
        // pick the state up through the full snapshot.
        for (int i = 0; i < 20; ++i) {
            assert(mc.SendCommand({"SET", "seed_" + std::to_string(i), "v" + std::to_string(i)}) == "+OK\r\n");
        }

        // A non-idempotent write, so a replayed backlog is visible. The master
        // used to resend its whole backlog on top of the snapshot the replica
        // had just loaded; with SET that is invisible, with INCR it is data loss.
        for (int i = 0; i < 7; ++i) {
            mc.SendCommand({"INCR", "counter"});
        }
        assert(mc.SendCommand({"GET", "counter"}) == "$1\r\n7\r\n");

        std::string r = rc.SendCommand({"REPLICAOF", "127.0.0.1", "16390"});
        if (r.rfind("+OK", 0) != 0) {
            std::cerr << "  REPLICAOF failed: " << r << std::endl;
        }
        assert(r.rfind("+OK", 0) == 0);

        // Let the link finish loading the snapshot.
        bool loaded = false;
        std::string got;
        for (int i = 0; i < 100 && !loaded; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            got = rc.SendCommand({"GET", "seed_7"});
            loaded = (got.find("v7") != std::string::npos);
        }
        if (!loaded) {
            std::cerr << "  replica never received the master's initial state; "
                         "GET seed_7 returned: "
                      << got << std::endl;
        }
        assert(loaded);
        // Exact form: "v7" is two bytes.
        assert(got == "$2\r\nv7\r\n");

        // The snapshot already contained counter=7. If the master also replays
        // its backlog from offset 0 on top of it, this reads 14.
        bool counter_ok = false;
        std::string cgot;
        for (int i = 0; i < 100 && !counter_ok; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            cgot = rc.SendCommand({"GET", "counter"});
            counter_ok = (cgot == "$1\r\n7\r\n");
        }
        if (!counter_ok) {
            std::cerr << "  replica counter is " << cgot
                      << "; a non-idempotent write was replayed (want $1\\r\\n7)"
                      << std::endl;
        }
        assert(counter_ok);

        // Every seeded key must be there, with the exact RESP form.
        for (int i = 0; i < 20; ++i) {
            const std::string want = "v" + std::to_string(i);
            std::string got = rc.SendCommand({"GET", "seed_" + std::to_string(i)});
            const std::string expect = "$" + std::to_string(want.size()) + "\r\n" + want + "\r\n";
            if (got != expect) {
                std::cerr << "  replica key seed_" << i << " is '" << got
                          << "', expected '" << expect << "'" << std::endl;
            }
            assert(got == expect);
        }

        // Incremental: writes after the attach must stream across.
        assert(mc.SendCommand({"SET", "after_attach", "live"}) == "+OK\r\n");
        bool streamed = false;
        for (int i = 0; i < 100 && !streamed; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            streamed = (rc.SendCommand({"GET", "after_attach"}) == "$4\r\nlive\r\n");
        }
        if (!streamed) {
            std::cerr << "  replica never received an incremental write" << std::endl;
        }
        assert(streamed);

        // A replica must refuse client writes.
        r = rc.SendCommand({"SET", "should_fail", "x"});
        if (r.rfind("-READONLY", 0) != 0 && r.rfind("-ERR", 0) != 0) {
            std::cerr << "  replica accepted a client write: " << r << std::endl;
        }
        assert(r.rfind("-READONLY", 0) == 0 || r.rfind("-ERR", 0) == 0);

        // Partial resync, driven by hand: play a slave that reconnects and asks
        // to resume from the master's current offset. The master must not send
        // its backlog from byte zero -- which is what it used to do on every
        // fresh connection, including right after a full sync.
        std::string repl_line;
        {
            const std::string mi = mc.SendCommand({"INFO", "replication"});
            const size_t at = mi.find("repl_id:");
            const size_t eol = mi.find("\r\n", at);
            assert(at != std::string::npos && eol != std::string::npos);
            repl_line = mi.substr(at + 8, eol - at - 8);
        }
        Client fake;
        assert(fake.Connect("127.0.0.1", 16390));
        assert(fake.SendCommand({"REPLCONF", "listening-port", "16399"}).rfind("+OK", 0) == 0);
        assert(fake.SendCommand({"REPLCONF", "capa", "psync2"}).rfind("+OK", 0) == 0);
        // Resume from wherever the master is right now, so nothing old is owed.
        std::string cur = mc.SendCommand({"INFO", "replication"});
        {
            const size_t at = cur.find("repl_offset:");
            const size_t eol = cur.find("\r\n", at);
            assert(at != std::string::npos && eol != std::string::npos);
            cur = cur.substr(at + 12, eol - at - 12);
        }
        std::string ps2 = fake.SendCommand({"PSYNC", repl_line, cur});
        if (ps2.rfind("$", 0) != 0) {
            std::cerr << "  PSYNC <id> <offset> was rejected: " << ps2 << std::endl;
        }
        assert(ps2.rfind("$", 0) == 0);
        {
            const size_t nl = ps2.find("\r\n");
            const long len = std::stol(ps2.substr(1, nl - 1));
            const std::string body = ps2.substr(nl + 2, static_cast<size_t>(len));
            // A partial resync replies with the bare repl_id -- no offset, and
            // above all no RDB dump behind it.
            if (body != repl_line) {
                std::cerr << "  expected a bare repl_id back, got: " << body
                          << std::endl;
            }
            assert(body == repl_line);
        }

        // Everything written from here on must arrive at the fake slave. The
        // first frame has to be exactly this write: if the master resent its
        // backlog from zero, the first frame would be some ancient seed SET.
        assert(mc.SendCommand({"SET", "after_continue", "tail"}) == "+OK\r\n");

        // Read one complete RESP array so the assertion sees the arguments and
        // not just the "*3" header.
        auto read_array = [](Client& c) -> std::string {
            std::string out = c.ReadLineRaw();
            if (out.empty() || out[0] != '*') return out;
            const long n = std::stol(out.substr(1));
            for (long i = 0; i < n; ++i) {
                out += "\n" + c.ReadLineRaw();
                const size_t nl = out.rfind("\n");
                const std::string hdr = out.substr(nl + 1);
                if (!hdr.empty() && hdr[0] == '$') {
                    out += c.ReadRaw(static_cast<size_t>(std::stol(hdr.substr(1))) + 2);
                }
            }
            return out;
        };

        std::string frame;
        for (int i = 0; i < 100 && frame.empty(); ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            frame = read_array(fake);
        }
        if (frame.rfind("*3", 0) != 0 || frame.find("after_continue") == std::string::npos) {
            std::string shown;
            for (char ch : frame) {
                shown += (ch == '\r') ? '~' : (ch == '\n' ? '|' : ch);
            }
            std::cerr << "  master replayed its backlog after a partial resync;"
                      << " first frame: " << shown << std::endl;
        }
        assert(frame.rfind("*3", 0) == 0);
        assert(frame.find("after_continue") != std::string::npos);
        fake.Disconnect();

        // INFO must report the link honestly.
        std::string info = rc.SendCommand({"INFO", "replication"});
        if (info.find("master_link_status:up") == std::string::npos) {
            std::fprintf(stderr, "  INFO did not report the link as up: %s\n",
                         info.c_str());
        }
        assert(info.find("master_link_status:up") != std::string::npos);
        std::string minfo = mc.SendCommand({"INFO", "replication"});
        if (minfo.find("connected_slaves:1") == std::string::npos) {
            std::fprintf(stderr, "  master did not count the replica: %s\n",
                         minfo.c_str());
        }
        assert(minfo.find("connected_slaves:1") != std::string::npos);

        // Promotion stops replication and re-opens the node for writes.
        assert(rc.SendCommand({"REPLICAOF", "NO", "ONE"}).rfind("+OK", 0) == 0);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        assert(rc.SendCommand({"SET", "after_promote", "ok"}) == "+OK\r\n");

        // A REPLICAOF pointing at nothing must fail loudly rather than report
        // success it cannot honour.
        assert(rc.SendCommand({"REPLICAOF", "127.0.0.1", "16999"}).rfind("-ERR", 0) == 0);

        // An empty master is still a full image: the replica must end up empty rather
        // than keep keys the new master has never heard of.
        Options e_opts;
        e_opts.db_path = e_db;
        DB* empty_db = nullptr;
        assert(DB::Open(e_opts, &empty_db).ok());

        ServerOptions e_srv;
        e_srv.tcp_port = 16392;
        e_srv.http_port = 18092;
        Server empty_master(empty_db, e_srv);
        std::thread e_thread([&empty_master]() { empty_master.Run(); });
        std::this_thread::sleep_for(std::chrono::milliseconds(300));

        assert(rc.SendCommand({"REPLICAOF", "127.0.0.1", "16392"}).rfind("+OK", 0) == 0);
        bool emptied = false;
        for (int i = 0; i < 100 && !emptied; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            emptied = (rc.SendCommand({"GET", "seed_7"}) == "$-1\r\n");
        }
        if (!emptied) {
            std::cerr << "  an empty master did not clear the replica" << std::endl;
        }
        assert(emptied);

        empty_master.Stop();
        e_thread.join();
        delete empty_db;
        lightkv_remove_tree(e_db);

        mc.Disconnect();
        rc.Disconnect();
        replica.Stop();
        r_thread.join();
        master.Stop();
        m_thread.join();
        delete replica_db;
        delete master_db;
        lightkv_remove_tree(m_db);
        lightkv_remove_tree(r_db);
        std::cout << "[Test] master/replica replication end-to-end" << std::endl;
    }

    // Test 15: Quit
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
