#include <iostream>
#include <chrono>
#include <string>
#include <iomanip>
#include "lightkv/client.h"

void run_benchmark(const std::string& label, int count, auto&& fn) {
    auto start = std::chrono::high_resolution_clock::now();
    fn();
    auto end = std::chrono::high_resolution_clock::now();
    double ms = std::chrono::duration<double, std::milli>(end - start).count();
    double ops = count / (ms / 1000.0);
    std::cout << "  " << std::left << std::setw(25) << label
              << std::right << std::setw(10) << count << " ops  |  "
              << std::fixed << std::setprecision(1) << std::setw(10) << ms << " ms  |  "
              << std::fixed << std::setprecision(0) << std::setw(12) << ops << " ops/sec"
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
    const int BATCH = 100;

    std::cout << "\n[C++ Pipeline Benchmark]" << std::endl;
    std::cout << "  " << std::left << std::setw(25) << "Operation"
              << std::right << std::setw(10) << "Count" << "  |  "
              << std::setw(10) << "Time(ms)" << "  |  "
              << std::setw(12) << "Ops/sec" << std::endl;
    std::cout << "  " << std::string(80, '-') << std::endl;

    // Regular SET (baseline)
    run_benchmark("SET (regular)", N, [&]() {
        for (int i = 0; i < N; i++) {
            client.Set("cpp_pipe_" + std::to_string(i), "value_" + std::to_string(i));
        }
    });

    // Pipeline SET
    run_benchmark("SET (pipeline)", N, [&]() {
        for (int i = 0; i < N; i += BATCH) {
            client.Pipeline();
            int end = std::min(i + BATCH, N);
            for (int j = i; j < end; j++) {
                client.Queue({"SET", "cpp_pipe_" + std::to_string(j), "value_" + std::to_string(j)});
            }
            client.ExecPipeline();
        }
    });

    // Regular GET (baseline)
    run_benchmark("GET (regular)", N, [&]() {
        for (int i = 0; i < N; i++) {
            client.Get("cpp_pipe_" + std::to_string(i));
        }
    });

    // Pipeline GET
    run_benchmark("GET (pipeline)", N, [&]() {
        for (int i = 0; i < N; i += BATCH) {
            client.Pipeline();
            int end = std::min(i + BATCH, N);
            for (int j = i; j < end; j++) {
                client.Queue({"GET", "cpp_pipe_" + std::to_string(j)});
            }
            client.ExecPipeline();
        }
    });

    // Regular DELETE (baseline)
    run_benchmark("DELETE (regular)", N, [&]() {
        for (int i = 0; i < N; i++) {
            client.Delete("cpp_pipe_" + std::to_string(i));
        }
    });

    // Pipeline DELETE
    run_benchmark("DELETE (pipeline)", N, [&]() {
        for (int i = 0; i < N; i += BATCH) {
            client.Pipeline();
            int end = std::min(i + BATCH, N);
            for (int j = i; j < end; j++) {
                client.Queue({"DEL", "cpp_pipe_" + std::to_string(j)});
            }
            client.ExecPipeline();
        }
    });

    client.Quit();
    std::cout << "\n[C++ Pipeline Benchmark Complete]" << std::endl;
    return 0;
}
