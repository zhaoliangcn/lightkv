#pragma once

#include <cstdint>
#include <chrono>
#include <mutex>
#include <thread>
#include <algorithm>

namespace lightkv {

// RateLimiter — token bucket 算法，限制 compaction 写入带宽
// 详见设计草案 4.2.2
//
// 用法：
//   RateLimiter rl(10 * 1024 * 1024);  // 10MB/s
//   rl.Request(4096);                   // 请求 4KB，阻塞直到 token 可用
class RateLimiter {
public:
    // bytes_per_sec = 0 表示不限速（Request 立即返回）
    explicit RateLimiter(uint64_t bytes_per_sec = 0,
                         uint64_t burst_size = 16 * 1024 * 1024)
        : rate_(bytes_per_sec),
          burst_(burst_size == 0 ? bytes_per_sec : burst_size),
          available_(burst_size),
          last_refill_(std::chrono::steady_clock::now()) {}

    // 请求 bytes 字节的配额，阻塞直到可用
    void Request(size_t bytes) {
        if (rate_ == 0) return;  // 不限速
        while (bytes > 0) {
            size_t granted = TryAcquire(bytes);
            if (granted >= bytes) return;
            // 未获满 → 等待 token 恢复
            // 计算还需多久才能凑够剩余字节
            size_t remaining = bytes - granted;
            auto wait_ms = static_cast<uint64_t>(
                static_cast<double>(remaining) / static_cast<double>(rate_) * 1000.0);
            // 至少等 1ms，避免忙轮
            std::this_thread::sleep_for(
                std::chrono::milliseconds(std::max<uint64_t>(wait_ms, 1)));
            bytes = remaining;
        }
    }

    // 设置新的速率（运行时可调，CONFIG SET）
    void SetRate(uint64_t bytes_per_sec) {
        std::lock_guard<std::mutex> lk(mu_);
        Refill();
        rate_ = bytes_per_sec;
    }

    uint64_t GetRate() const { return rate_; }

private:
    // 尝试获取 bytes 字节，返回实际获准的字节数（可能 < bytes）
    size_t TryAcquire(size_t bytes) {
        std::lock_guard<std::mutex> lk(mu_);
        Refill();
        size_t granted = std::min(static_cast<size_t>(available_), bytes);
        available_ -= granted;
        return granted;
    }

    // 按时间流逝补充 token（持锁调用）
    void Refill() {
        auto now = std::chrono::steady_clock::now();
        auto elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(
                              now - last_refill_).count();
        if (elapsed_us <= 0) return;
        // 补充量 = rate_ * elapsed_seconds
        double elapsed_sec = static_cast<double>(elapsed_us) / 1000000.0;
        uint64_t add = static_cast<uint64_t>(static_cast<double>(rate_) * elapsed_sec);
        if (add == 0) return;  // 时间太短，不足以补 1 字节
        available_ = std::min(burst_, available_ + add);
        last_refill_ = now;
    }

    mutable std::mutex mu_;
    uint64_t rate_;        // bytes/sec，0 = 不限速
    uint64_t burst_;       // token bucket 容量上限
    uint64_t available_;   // 当前可用 token
    std::chrono::steady_clock::time_point last_refill_;
};

} // namespace lightkv