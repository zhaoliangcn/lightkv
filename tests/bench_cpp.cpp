#include <iostream>
#include <chrono>
#include <string>
#include <vector>
#include <iomanip>
#include "lightkv/client.h"

struct BenchmarkResult {
    std::string name;
    int ops;
    double duration_ms;
    double ops_per_sec;
};

void run_benchmark(const std::string& label, int count, auto&& fn) {
    auto start = std::chrono::high_resolution_clock::now();
    fn();
    auto end = std::chrono::high_resolution_clock::now();
    double ms = std::chrono::duration<double, std::milli>(end - start).count();
    double ops = count / (ms / 1000.0);
    std::cout << "  " << std::left << std::setw(25) << label
              << std::right << std::setw(10) << count << " ops  |  "
              << std::fixed << std::setprecision(1) << std::setw(10) << ms << " ms  |  "
              << std::fixed << std::setprecision(0) << std::setw(10) << ops << " ops/sec"
              << std::endl;
}

int main() {
    lightkv::Client client;
    if (!client.Connect("127.0.0.1", 16379)) {
        std::cerr << "Failed to connect: " << client.GetLastError() << std::endl;
        return 1;
    }
    std::cout << "Connected to LightKV server" << std::endl;

    const int N = 10000;

    std::cout << "\n[C++ Client Benchmark]" << std::endl;
    std::cout << "  " << std::left << std::setw(25) << "Operation"
              << std::right << std::setw(10) << "Count" << "  |  "
              << std::setw(10) << "Time(ms)" << "  |  "
              << std::setw(10) << "Ops/sec" << std::endl;
    std::cout << "  " << std::string(75, '-') << std::endl;

    // SET benchmark
    run_benchmark("SET", N, [&]() {
        for (int i = 0; i < N; i++) {
            client.Set("cpp_key_" + std::to_string(i), "value_" + std::to_string(i));
        }
    });

    // GET benchmark
    run_benchmark("GET", N, [&]() {
        for (int i = 0; i < N; i++) {
            client.Get("cpp_key_" + std::to_string(i));
        }
    });

    // DELETE benchmark
    run_benchmark("DELETE", N, [&]() {
        for (int i = 0; i < N; i++) {
            client.Delete("cpp_key_" + std::to_string(i));
        }
    });

    // Mixed workload
    run_benchmark("MIXED (SET+GET+DEL)", N, [&]() {
        for (int i = 0; i < N; i++) {
            std::string k = "cpp_mixed_" + std::to_string(i);
            client.Set(k, "v");
            client.Get(k);
            client.Delete(k);
        }
    });

    client.Quit();
    std::cout << "\n[C++ Benchmark Complete]" << std::endl;
    return 0;
}
