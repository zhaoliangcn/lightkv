#include "lightkv/db.h"
#include <chrono>
#include <iostream>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <random>
#include <iomanip>

struct BenchmarkResult {
    std::string name;
    size_t operations;
    double elapsed_ms;
    double ops_per_sec;
    double avg_latency_us;
    double p50_latency_us;
    double p99_latency_us;
};

class Timer {
public:
    Timer() : start_(Clock::now()) {}
    double ElapsedMs() const {
        return std::chrono::duration<double, std::milli>(Clock::now() - start_).count();
    }
    double ElapsedUs() const {
        return std::chrono::duration<double, std::micro>(Clock::now() - start_).count();
    }
private:
    using Clock = std::chrono::high_resolution_clock;
    Clock::time_point start_;
};

class LatencyRecorder {
public:
    void Record(double us) { latencies_.push_back(us); }
    double P50() const { return Percentile(50); }
    double P99() const { return Percentile(99); }
    double Avg() const {
        double sum = 0;
        for (double v : latencies_) sum += v;
        return latencies_.empty() ? 0 : sum / latencies_.size();
    }
private:
    std::vector<double> latencies_;
    double Percentile(int p) const {
        if (latencies_.empty()) return 0;
        auto sorted = latencies_;
        std::sort(sorted.begin(), sorted.end());
        size_t idx = static_cast<size_t>(sorted.size() * p / 100.0);
        if (idx >= sorted.size()) idx = sorted.size() - 1;
        return sorted[idx];
    }
};

void PrintResult(const BenchmarkResult& res) {
    std::cout << "| " << std::left << std::setw(28) << res.name
              << "| " << std::right << std::setw(10) << res.operations
              << " | " << std::setw(10) << std::fixed << std::setprecision(2) << res.elapsed_ms
              << " | " << std::setw(12) << std::fixed << std::setprecision(0) << res.ops_per_sec
              << " | " << std::setw(12) << std::fixed << std::setprecision(1) << res.avg_latency_us
              << " | " << std::setw(10) << std::fixed << std::setprecision(1) << res.p50_latency_us
              << " | " << std::setw(10) << std::fixed << std::setprecision(1) << res.p99_latency_us
              << " |" << std::endl;
}

void PrintHeader() {
    std::cout << "| " << std::left << std::setw(28) << "Test Name"
              << "| " << std::right << std::setw(10) << "Ops"
              << " | " << std::setw(10) << "Time(ms)"
              << " | " << std::setw(12) << "QPS"
              << " | " << std::setw(12) << "Avg Lat(us)"
              << " | " << std::setw(10) << "P50(us)"
              << " | " << std::setw(10) << "P99(us)"
              << " |" << std::endl;
    std::cout << "|" << std::string(30, '-')
              << "|" << std::string(12, '-')
              << "|" << std::string(12, '-')
              << "|" << std::string(14, '-')
              << "|" << std::string(14, '-')
              << "|" << std::string(12, '-')
              << "|" << std::string(12, '-')
              << "|" << std::endl;
}

BenchmarkResult RunSequentialWrite(lightkv::DB* db, size_t n, int key_len, int value_len) {
    LatencyRecorder rec;
    Timer t;
    for (size_t i = 0; i < n; ++i) {
        char key_buf[64], val_buf[8192];
        snprintf(key_buf, sizeof(key_buf), "%0*d", key_len, static_cast<int>(i));
        memset(val_buf, 'v', value_len);
        val_buf[value_len] = '\0';
        Timer op_t;
        auto s = db->Put(lightkv::WriteOptions(), key_buf, lightkv::Slice(val_buf, value_len));
        rec.Record(op_t.ElapsedUs());
        if (!s.ok()) { std::cerr << "Error: " << s.ToString() << std::endl; }
    }
    double elapsed = t.ElapsedMs();
    return {"SeqWrite(k" + std::to_string(key_len) + "v" + std::to_string(value_len) + ")",
            n, elapsed, n / elapsed * 1000.0, rec.Avg(), rec.P50(), rec.P99()};
}

BenchmarkResult RunRandomWrite(lightkv::DB* db, size_t n, int key_len, int value_len) {
    LatencyRecorder rec;
    std::mt19937 rng(42);
    Timer t;
    for (size_t i = 0; i < n; ++i) {
        char key_buf[64], val_buf[8192];
        snprintf(key_buf, sizeof(key_buf), "%0*d", key_len, static_cast<int>(rng() % (n * 10)));
        memset(val_buf, 'v', value_len);
        val_buf[value_len] = '\0';
        Timer op_t;
        auto s = db->Put(lightkv::WriteOptions(), key_buf, lightkv::Slice(val_buf, value_len));
        rec.Record(op_t.ElapsedUs());
        if (!s.ok()) { std::cerr << "Error: " << s.ToString() << std::endl; }
    }
    double elapsed = t.ElapsedMs();
    return {"RandomWrite(k" + std::to_string(key_len) + "v" + std::to_string(value_len) + ")",
            n, elapsed, n / elapsed * 1000.0, rec.Avg(), rec.P50(), rec.P99()};
}

BenchmarkResult RunSequentialRead(lightkv::DB* db, size_t n, int key_len) {
    LatencyRecorder rec;
    Timer t;
    std::string val;
    for (size_t i = 0; i < n; ++i) {
        char key_buf[64];
        snprintf(key_buf, sizeof(key_buf), "%0*d", key_len, static_cast<int>(i));
        Timer op_t;
        auto s = db->Get(lightkv::ReadOptions(), key_buf, &val);
        rec.Record(op_t.ElapsedUs());
        if (!s.ok() && !s.IsNotFound()) { std::cerr << "Error: " << s.ToString() << std::endl; }
    }
    double elapsed = t.ElapsedMs();
    return {"SeqRead(k" + std::to_string(key_len) + ")",
            n, elapsed, n / elapsed * 1000.0, rec.Avg(), rec.P50(), rec.P99()};
}

BenchmarkResult RunRandomRead(lightkv::DB* db, size_t n, int key_len, int key_space) {
    LatencyRecorder rec;
    std::mt19937 rng(42);
    std::string val;
    Timer t;
    for (size_t i = 0; i < n; ++i) {
        char key_buf[64];
        snprintf(key_buf, sizeof(key_buf), "%0*d", key_len, static_cast<int>(rng() % key_space));
        Timer op_t;
        auto s = db->Get(lightkv::ReadOptions(), key_buf, &val);
        rec.Record(op_t.ElapsedUs());
        if (!s.ok() && !s.IsNotFound()) { std::cerr << "Error: " << s.ToString() << std::endl; }
    }
    double elapsed = t.ElapsedMs();
    return {"RandomRead(k" + std::to_string(key_len) + ")",
            n, elapsed, n / elapsed * 1000.0, rec.Avg(), rec.P50(), rec.P99()};
}

BenchmarkResult RunBatchWrite(lightkv::DB* db, size_t n, int batch_size, int value_len) {
    LatencyRecorder rec;
    Timer t;
    for (size_t i = 0; i < n; i += batch_size) {
        char key_buf[64], val_buf[8192];
        memset(val_buf, 'v', value_len);
        val_buf[value_len] = '\0';
        Timer op_t;
        for (int j = 0; j < batch_size && (i + j) < n; ++j) {
            snprintf(key_buf, sizeof(key_buf), "batch_%08zu", i + j);
            auto s = db->Put(lightkv::WriteOptions(), key_buf, lightkv::Slice(val_buf, value_len));
            if (!s.ok()) { std::cerr << "Error: " << s.ToString() << std::endl; }
        }
        rec.Record(op_t.ElapsedUs() / batch_size);
    }
    double elapsed = t.ElapsedMs();
    return {"BatchWrite(b" + std::to_string(batch_size) + "v" + std::to_string(value_len) + ")",
            n, elapsed, n / elapsed * 1000.0, rec.Avg(), rec.P50(), rec.P99()};
}

BenchmarkResult RunDelete(lightkv::DB* db, size_t n, int key_len) {
    LatencyRecorder rec;
    Timer t;
    for (size_t i = 0; i < n; ++i) {
        char key_buf[64];
        snprintf(key_buf, sizeof(key_buf), "%0*d", key_len, static_cast<int>(i));
        Timer op_t;
        auto s = db->Delete(lightkv::WriteOptions(), key_buf);
        rec.Record(op_t.ElapsedUs());
        if (!s.ok()) { std::cerr << "Error: " << s.ToString() << std::endl; }
    }
    double elapsed = t.ElapsedMs();
    return {"Delete(k" + std::to_string(key_len) + ")",
            n, elapsed, n / elapsed * 1000.0, rec.Avg(), rec.P50(), rec.P99()};
}

BenchmarkResult RunMixedWorkload(lightkv::DB* db, size_t n) {
    LatencyRecorder rec;
    std::mt19937 rng(42);
    std::string val;
    Timer t;
    for (size_t i = 0; i < n; ++i) {
        char key_buf[64], val_buf[128];
        int op = rng() % 100;
        Timer op_t;
        if (op < 80) {
            snprintf(key_buf, sizeof(key_buf), "mix_%08zu", static_cast<size_t>(rng() % (n * 2)));
            memset(val_buf, 'v', 100);
            val_buf[100] = '\0';
            auto s = db->Put(lightkv::WriteOptions(), key_buf, lightkv::Slice(val_buf, 100));
            if (!s.ok()) { std::cerr << "Error: " << s.ToString() << std::endl; }
        } else if (op < 95) {
            snprintf(key_buf, sizeof(key_buf), "mix_%08zu", static_cast<size_t>(rng() % (n * 2)));
            db->Get(lightkv::ReadOptions(), key_buf, &val);
        } else {
            snprintf(key_buf, sizeof(key_buf), "mix_%08zu", static_cast<size_t>(rng() % (n * 2)));
            db->Delete(lightkv::WriteOptions(), key_buf);
        }
        rec.Record(op_t.ElapsedUs());
    }
    double elapsed = t.ElapsedMs();
    return {"Mixed(80%W+15%R+5%D)",
            n, elapsed, n / elapsed * 1000.0, rec.Avg(), rec.P50(), rec.P99()};
}

int main() {
    const char* db_path = "/tmp/lightkv_bench";
    std::string cmd = "rm -rf " + std::string(db_path);
    system(cmd.c_str());

    lightkv::Options opts;
    opts.db_path = db_path;
    opts.create_if_missing = true;
    opts.memtable_size = 64 * 1024 * 1024;
    opts.block_size = 4096;
    opts.bloom_bits_per_key = 10;

    lightkv::DB* db = nullptr;
    auto s = lightkv::DB::Open(opts, &db);
    if (!s.ok()) {
        std::cerr << "Failed to open DB: " << s.ToString() << std::endl;
        return 1;
    }

    std::cout << "\n==============================================================" << std::endl;
    std::cout << "  LightKV Performance Benchmark Report" << std::endl;
    std::cout << "==============================================================" << std::endl;
    std::cout << "  MemTable size: " << (opts.memtable_size >> 20) << "MB" << std::endl;
    std::cout << "  Block size:    " << opts.block_size << " bytes" << std::endl;
    std::cout << "  Bloom bits:    " << opts.bloom_bits_per_key << " bits/key" << std::endl;
    std::cout << "==============================================================" << std::endl << std::endl;

    const size_t WRITE_N = 100000;
    const size_t READ_N = 100000;
    const size_t LARGE_WRITE_N = 500000;
    const size_t LARGE_READ_N = 500000;

    // ===== Phase 1: Write Benchmarks =====
    std::cout << "--- Phase 1: Write Benchmarks (" << WRITE_N << " ops each) ---" << std::endl << std::endl;
    PrintHeader();

    auto r1 = RunSequentialWrite(db, WRITE_N, 8, 100);
    PrintResult(r1);

    auto r2 = RunSequentialWrite(db, WRITE_N, 8, 1024);
    PrintResult(r2);

    auto r3 = RunSequentialWrite(db, WRITE_N, 8, 4096);
    PrintResult(r3);

    auto r4 = RunRandomWrite(db, WRITE_N, 8, 100);
    PrintResult(r4);

    auto r5 = RunBatchWrite(db, WRITE_N, 10, 100);
    PrintResult(r5);

    auto r6 = RunBatchWrite(db, WRITE_N, 100, 100);
    PrintResult(r6);

    // ===== Phase 2: Read Benchmarks =====
    std::cout << std::endl << "--- Phase 2: Read Benchmarks (" << READ_N << " ops each) ---" << std::endl << std::endl;
    PrintHeader();

    auto r7 = RunSequentialRead(db, READ_N, 8);
    PrintResult(r7);

    auto r8 = RunRandomRead(db, READ_N, 8, static_cast<int>(WRITE_N));
    PrintResult(r8);

    // ===== Phase 3: Delete Benchmark =====
    std::cout << std::endl << "--- Phase 3: Delete Benchmark (" << WRITE_N << " ops) ---" << std::endl << std::endl;
    PrintHeader();

    auto r9 = RunDelete(db, WRITE_N, 8);
    PrintResult(r9);

    // ===== Phase 4: Mixed Workload =====
    std::cout << std::endl << "--- Phase 4: Mixed Workload (" << WRITE_N << " ops) ---" << std::endl << std::endl;
    PrintHeader();

    auto r10 = RunMixedWorkload(db, WRITE_N);
    PrintResult(r10);

    // ===== Phase 5: Large-Scale Benchmarks =====
    std::cout << std::endl << "--- Phase 5: Large-Scale Benchmarks ---" << std::endl << std::endl;
    PrintHeader();

    // Re-open to avoid memtable saturation
    delete db;
    cmd = "rm -rf " + std::string(db_path);
    system(cmd.c_str());
    s = lightkv::DB::Open(opts, &db);
    if (!s.ok()) { std::cerr << "Failed to open DB: " << s.ToString() << std::endl; return 1; }

    auto r11 = RunSequentialWrite(db, LARGE_WRITE_N, 8, 100);
    PrintResult(r11);

    auto r12 = RunSequentialRead(db, LARGE_READ_N, 8);
    PrintResult(r12);

    auto r13 = RunRandomRead(db, LARGE_READ_N, 8, static_cast<int>(LARGE_WRITE_N));
    PrintResult(r13);

    std::cout << std::endl << "==============================================================" << std::endl;
    std::cout << "  All benchmarks completed." << std::endl;
    std::cout << "==============================================================" << std::endl;

    delete db;
    return 0;
}