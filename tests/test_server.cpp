#include "lightkv/db.h"
#include "lightkv/server.h"
#include "lightkv/client.h"
#include <iostream>
#include <thread>
#include <chrono>
#include <cassert>

using namespace lightkv;

int main() {
    std::string db_path = "/tmp/lightkv_server_test";
    
    // Clean up previous test data
    system(("rm -rf " + db_path).c_str());
    
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
    assert(client.Connect("127.0.0.1", 16385));
    std::cout << "[Test] Connected" << std::endl;

    // Test 2: Ping
    std::cout << "[Test] PING..." << std::endl;
    assert(client.Ping());
    std::cout << "[Test] PONG received" << std::endl;

    // Test 3: Set/Get
    std::cout << "[Test] SET/GET..." << std::endl;
    assert(client.Set("hello", "world"));
    auto val = client.Get("hello");
    assert(val.has_value() && *val == "world");
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

    // Test 8: Stats
    std::cout << "[Test] STATS..." << std::endl;
    auto stats = client.Stats();
    assert(!stats.empty());
    for (auto& [k, v] : stats) {
        std::cout << "  " << k << " = " << v << std::endl;
    }

    // Test 9: Quit
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
