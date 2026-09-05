#pragma once
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>

using SteadyClock = std::chrono::steady_clock;
inline uint64_t ElapsedMicros(SteadyClock::time_point start,
                              SteadyClock::time_point end = SteadyClock::now()) {
    if (end <= start) return 0;
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(end - start).count());
}
// Owner-thread counters. Averages use integer division (rounded down).
struct LatencyStats {
    uint64_t count = 0, total_us = 0, max_us = 0;
    void Observe(uint64_t us) {
        ++count;
        total_us += us;
        if (us > max_us) max_us = us;
    }
    std::string ToInfo(const std::string& prefix) const {
        return prefix + "_count:" + std::to_string(count) + "\r\n" +
               prefix + "_total_us:" + std::to_string(total_us) + "\r\n" +
               prefix + "_max_us:" + std::to_string(max_us) + "\r\n" +
               prefix + "_avg_us:" + std::to_string(count ? total_us / count : 0) + "\r\n";
    }
};
struct BatchStats {
    LatencyStats latency;
    uint64_t entries = 0, bytes = 0;
    void Observe(uint64_t us, size_t n, size_t byte_count) {
        latency.Observe(us);
        entries += n;
        bytes += byte_count;
    }
    std::string ToInfo(const std::string& prefix) const {
        return latency.ToInfo(prefix) +
               prefix + "_entries:" + std::to_string(entries) + "\r\n" +
               prefix + "_bytes:" + std::to_string(bytes) + "\r\n" +
               prefix + "_avg_entries:" + std::to_string(latency.count ? entries / latency.count : 0) + "\r\n";
    }
};
