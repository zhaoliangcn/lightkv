#include "lightkv/db.h"
#include "lightkv/server.h"
#include "lightkv/client.h"
#include <iostream>
#include <thread>
#include <chrono>
#include <string>
#include <cstdlib>

using namespace lightkv;

static int total_tests = 0;
static int passed_tests = 0;

#define TEST(name, expr) do { \
    total_tests++; \
    bool _ok = (expr); \
    if (_ok) { passed_tests++; std::cout << "  [PASS] " << name << std::endl; } \
    else { std::cout << "  [FAIL] " << name << " (line " << __LINE__ << ")" << std::endl; } \
} while(0)

#define CHECK_CONNECT(client, host, port) do { \
    if (!(client).Connect((host), (port))) { \
        std::cerr << "  FATAL: Connect failed: " << (client).GetLastError() << std::endl; \
        return 1; \
    } \
} while(0)

// Helper: start server with auth enabled
static void run_auth_server(DB* db, uint16_t tcp_port, const std::string& password) {
    ServerOptions srv_opts;
    srv_opts.tcp_port = tcp_port;
    srv_opts.http_port = 0;
    srv_opts.enable_http = false;
    srv_opts.requirepass = password;
    Server server(db, srv_opts);
    server.Run();
}

// Wait for server to be ready
static bool wait_for_server(const std::string& host, uint16_t port, int max_retries = 20) {
    for (int i = 0; i < max_retries; i++) {
        Client probe;
        if (probe.Connect(host, port)) {
            probe.Disconnect();
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return false;
}

int main() {
    std::string db_path = "/tmp/lightkv_auth_test";
    system(("rm -rf " + db_path).c_str());

    Options opts;
    opts.db_path = db_path;
    DB* db = nullptr;
    auto s = DB::Open(opts, &db);
    if (!s.ok()) {
        std::cerr << "Failed to open DB: " << s.ToString() << std::endl;
        return 1;
    }

    const std::string password = "mysecret123";
    const uint16_t port = 36379;

    // Start server with auth in background thread
    std::thread server_thread([db, port, password]() {
        run_auth_server(db, port, password);
    });

    if (!wait_for_server("127.0.0.1", port)) {
        std::cerr << "Server failed to start" << std::endl;
        return 1;
    }

    std::cout << "\n=== AUTH Tests ===\n" << std::endl;

    // Test 1: Connect without auth, commands should fail with NOAUTH
    {
        std::cout << "[Test] No auth -> commands fail" << std::endl;
        Client client;
        CHECK_CONNECT(client, "127.0.0.1", port);
        // PING should still work (allowed without auth)
        TEST("PING without auth", client.Ping());
        // GET should fail with NOAUTH
        auto val = client.Get("testkey");
        TEST("GET without auth returns NOAUTH error", !val.has_value());
        // SET should fail with NOAUTH
        bool set_ok = client.Set("testkey", "value");
        TEST("SET without auth fails", !set_ok);
        client.Disconnect();
    }

    // Test 2: Connect with wrong password
    {
        std::cout << "[Test] Wrong password -> AUTH fails" << std::endl;
        Client client;
        CHECK_CONNECT(client, "127.0.0.1", port);
        bool auth_ok = client.Auth("wrongpassword");
        TEST("AUTH with wrong password fails", !auth_ok);
        // After failed auth, commands should still fail
        auto val = client.Get("testkey");
        TEST("GET after wrong auth returns NOAUTH error", !val.has_value());
        client.Disconnect();
    }

    // Test 3: Connect with correct password
    {
        std::cout << "[Test] Correct password -> AUTH succeeds" << std::endl;
        Client client;
        CHECK_CONNECT(client, "127.0.0.1", port);
        bool auth_ok = client.Auth(password);
        TEST("AUTH with correct password succeeds", auth_ok);
        // After auth, commands should work
        TEST("SET after auth", client.Set("auth_test_key", "auth_test_value"));
        auto val = client.Get("auth_test_key");
        TEST("GET after auth returns correct value", val.has_value() && *val == "auth_test_value");
        TEST("DELETE after auth", client.Delete("auth_test_key"));
        client.Disconnect();
    }

    // Test 4: CONFIG GET requirepass
    {
        std::cout << "[Test] CONFIG GET requirepass" << std::endl;
        Client client;
        CHECK_CONNECT(client, "127.0.0.1", port);
        TEST("AUTH before CONFIG", client.Auth(password));
        auto result = client.ConfigGet("requirepass");
        TEST("CONFIG GET requirepass returns value", !result.empty());
        client.Disconnect();
    }

    // Test 5: CONFIG SET requirepass
    {
        std::cout << "[Test] CONFIG SET requirepass" << std::endl;
        Client client;
        CHECK_CONNECT(client, "127.0.0.1", port);
        TEST("AUTH before CONFIG SET", client.Auth(password));
        bool set_ok = client.ConfigSet("requirepass", "newpass456");
        TEST("CONFIG SET requirepass succeeds", set_ok);
        if (set_ok) {
            // Now try auth with new password
            Client client2;
            CHECK_CONNECT(client2, "127.0.0.1", port);
            bool auth_ok = client2.Auth("newpass456");
            TEST("AUTH with new password succeeds", auth_ok);
            TEST("SET with new password auth", client2.Set("newpass_test", "value"));
            client2.Disconnect();
        }
        client.Disconnect();
    }

    // Test 6: Server without password -> AUTH should error
    {
        std::cout << "[Test] Server without password -> AUTH errors" << std::endl;
        std::string db_path2 = "/tmp/lightkv_auth_test_noauth";
        system(("rm -rf " + db_path2).c_str());
        Options opts2;
        opts2.db_path = db_path2;
        DB* db2 = nullptr;
        DB::Open(opts2, &db2);

        const uint16_t port2 = 36380;
        std::thread server_thread2([db2, port2]() {
            ServerOptions srv_opts;
            srv_opts.tcp_port = port2;
            srv_opts.http_port = 0;
            srv_opts.enable_http = false;
            Server server(db2, srv_opts);
            server.Run();
        });

        if (!wait_for_server("127.0.0.1", port2)) {
            std::cerr << "  Server2 failed to start" << std::endl;
            db2->~DB();
            server_thread2.detach();
            return 1;
        }

        Client client;
        CHECK_CONNECT(client, "127.0.0.1", port2);
        // Without password, commands should work
        TEST("SET without password", client.Set("noauth_key", "value"));
        auto val = client.Get("noauth_key");
        TEST("GET without password", val.has_value() && *val == "value");
        // AUTH should error
        bool auth_err = client.Auth("anypass");
        TEST("AUTH when no password set returns error", !auth_err);
        client.Disconnect();

        db2->~DB();
        server_thread2.detach();
    }

    std::cout << "\n=== Results: " << passed_tests << "/" << total_tests << " passed ===" << std::endl;

    delete db;

    return (passed_tests == total_tests) ? 0 : 1;
}
