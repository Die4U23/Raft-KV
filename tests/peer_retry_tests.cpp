#include "common/peer_retry_policy.h"
#include <iostream>
#include <stdexcept>

static void Check(bool value, const char* message) {
    if (!value) throw std::runtime_error(message);
}

int main() {
    using Clock = PeerRetryPolicy::Clock;
    using namespace std::chrono;
    PeerRetryPolicy policy;
    auto now = Clock::time_point{};
    Check(!policy.Disconnected(now).token, "unconnected client scheduled retry");
    Check(!policy.Consume(0), "empty timer consumed");
    // 100 successful-but-immediately-closed connections cannot reset or overflow
    // backoff. Duplicate close callbacks cannot schedule a second timer.
    for (int i = 0; i < 100; ++i) {
        policy.Connected(now);
        auto plan = policy.Disconnected(now + milliseconds(1));
        Check(plan.delay_ms == (i == 0 ? 500 : i == 1 ? 1000 : 2000), "flapping reset backoff");
        Check(!policy.Disconnected(now).token, "duplicate close scheduled retry");
        Check(!policy.Consume(plan.token + 1), "stale timer accepted");
        Check(policy.Consume(plan.token) && !policy.Consume(plan.token), "timer not consumed once");
        now += milliseconds(plan.delay_ms + 1);
    }
    policy.Connected(now);
    auto short_connection = policy.Disconnected(now + milliseconds(9999));
    Check(short_connection.delay_ms == 2000, "unstable connection reset backoff");
    policy.Connected(now);
    Check(!policy.Consume(short_connection.token), "replaced connection kept stale timer");
    auto stable = policy.Disconnected(now + seconds(10));
    Check(stable.delay_ms == 500, "stable connection failed to reset");
    Check(policy.Consume(stable.token), "stable retry missing");
    PeerRetryPolicy other;
    other.Connected(now);
    Check(other.Disconnected(now).delay_ms == 500, "backoff leaked across peers");
    std::cout << "PASS: retry cap, stable reset, duplicate/stale timers and peer isolation\n";
}
