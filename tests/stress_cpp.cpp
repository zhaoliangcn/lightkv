#include <iostream>
#include <chrono>
#include <string>
#include <vector>
#include <thread>
#include <atomic>
#include <iomanip>
#include <numeric>
#include "lightkv/client.h"

struct StressResult {
    std::string name;
    int total_ops;
    int success_ops;
    int failed_ops;
    double duration_ms;
    double ops_per_sec;
    double p50_ms;
    double p99_ms;
};

void worker_thread(int thread_id, int ops_per_thread,
                   std::atomic<int>& total_success,
                   std::atomic<int>& total_failed,
                   std::vector<double>& latencies,
                   std::mutex& lat_mutex) {
    lightkv::Client client;
    if (!client.Connect("127.0.0.1", 16379)) {
        total_failed += ops_per_thread;
        return;
    }

    for (int i = 0; i < ops_per_thread; i++) {
        std::string key = "stress_" + std::to_string(thread_id) + "_" + std::to_string(i);
        std::string value = "value_" + std::to_string(i);

        auto start = std::chrono::high_resolution_clock::now();

        // SET
        bool ok = client.Set(key, value);
        auto mid = std::chrono::high_resolution_clock::now();

        // GET
        auto val = client.Get(key);

        // DELETE
        client.Delete(key);

        auto end = std::chrono::high_resolution_clock::now();

        double ms = std::chrono::duration<double, std::milli>(end - start).count();
        if (ok) {
            total_success += 3; // SET + GET + DELETE
        } else {
            total_failed += 3;
        }

        std::lock_guard<std::mutex> lock(lat_mutex);
        latencies.push_back(ms);
    }

    client.Quit();
}

int main(int argc, char* argv[]) {
    int num_threads = 10;
    int ops_per_thread = 1000;

    if (argc > 1) num_threads = std::stoi(argv[1]);
    if (argc > 2) ops_per_thread = std::stoi(argv[2]);

    int total_ops = num_threads * ops_per_thread * 3; // SET + GET + DELETE per iteration

    std::cout << "[C++ Stress Test]" << std::endl;
    std::cout << "  Threads: " << num_threads << std::endl;
    std::cout << "  Ops/thread: " << ops_per_thread << " (SET+GET+DELETE)" << std::endl;
    std::cout << "  Total ops: " << total_ops << std::endl;

    std::atomic<int> success{0};
    std::atomic<int> failed{0};
    std::vector<double> latencies;
    std::mutex lat_mutex;

    std::vector<std::thread> threads;
    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < num_threads; i++) {
        threads.emplace_back(worker_thread, i, ops_per_thread,
                            std::ref(success), std::ref(failed),
                            std::ref(latencies), std::ref(lat_mutex));
    }

    for (auto& t : threads) {
        t.join();
    }

    auto end = std::chrono::high_resolution_clock::now();
    double duration_ms = std::chrono::duration<double, std::milli>(end - start).count();

    // Calculate percentiles
    if (latencies.empty()) {
        std::cout << "\n  Results: No successful operations" << std::endl;
        std::cout << "    Make sure the server is running on 127.0.0.1:16379" << std::endl;
        return 1;
    }

    std::sort(latencies.begin(), latencies.end());
    double p50 = latencies[latencies.size() * 50 / 100];
    double p99 = latencies[latencies.size() * 99 / 100];
    double avg = std::accumulate(latencies.begin(), latencies.end(), 0.0) / latencies.size();

    double ops_sec = success / (duration_ms / 1000.0);

    std::cout << "\n  Results:" << std::endl;
    std::cout << "    Duration:     " << std::fixed << std::setprecision(1) << duration_ms << " ms" << std::endl;
    std::cout << "    Success:      " << success << " ops" << std::endl;
    std::cout << "    Failed:       " << failed << " ops" << std::endl;
    std::cout << "    Throughput:   " << std::fixed << std::setprecision(0) << ops_sec << " ops/sec" << std::endl;
    std::cout << "    Avg Latency:  " << std::fixed << std::setprecision(2) << avg << " ms" << std::endl;
    std::cout << "    P50 Latency:  " << std::fixed << std::setprecision(2) << p50 << " ms" << std::endl;
    std::cout << "    P99 Latency:  " << std::fixed << std::setprecision(2) << p99 << " ms" << std::endl;

    std::cout << "\n[C++ Stress Test Complete]" << std::endl;
    return 0;
}
