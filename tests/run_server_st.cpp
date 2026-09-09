#include "test_paths.h"
#include "lightkv/db.h"
#include "lightkv/server.h"
#include <iostream>
#include <thread>
#include <chrono>

int main() {
    std::string db_path = LIGHTKV_TEST_TMP "/lightkv_bench_st";
    lightkv_remove_tree(db_path);

    lightkv::Options opts;
    opts.db_path = db_path;
    lightkv::DB* db = nullptr;
    auto s = lightkv::DB::Open(opts, &db);
    if (!s.ok()) {
        std::cerr << "Failed to open DB: " << s.ToString() << std::endl;
        return 1;
    }

    lightkv::ServerOptions srv_opts;
    srv_opts.tcp_port = 16379;
    srv_opts.http_port = 18080;
    srv_opts.requirepass = "benchpass123";
    srv_opts.worker_threads = 0;  // Single-threaded

    lightkv::Server server(db, srv_opts);
    std::thread t([&server]() { server.Run(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    std::cout << "[Single-threaded server started]" << std::endl;
    t.join();

    delete db;
    return 0;
}
