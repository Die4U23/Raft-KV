#pragma once
#include <algorithm>
#include <chrono>
#include <cstdint>

// Owner-loop policy for connections that succeed and then close. Connect failures
// remain owned by Muduo's Connector; never run two connection attempts in parallel.
class PeerRetryPolicy {
public:
    using Clock = std::chrono::steady_clock;
    struct Plan { uint64_t token = 0; int delay_ms = 0; };

    void Connected(Clock::time_point now) {
        connected_ = true;
        connected_at_ = now;
        pending_ = false;
        ++generation_;
    }

    Plan Disconnected(Clock::time_point now) {
        if (!connected_) return {};
        connected_ = false;
        if (now - connected_at_ >= std::chrono::seconds(10)) next_delay_ms_ = 500;
        const int delay = next_delay_ms_;
        next_delay_ms_ = std::min(2000, next_delay_ms_ * 2);
        pending_ = true;
        return {++generation_, delay};
    }

    bool Consume(uint64_t token) {
        if (!pending_ || token != generation_) return false;
        pending_ = false;
        return true;
    }

private:
    Clock::time_point connected_at_{};
    uint64_t generation_ = 0;
    int next_delay_ms_ = 500;
    bool connected_ = false;
    bool pending_ = false;
};
