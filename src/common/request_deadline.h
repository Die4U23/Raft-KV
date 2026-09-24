#pragma once

#include "common/metrics.h"

#include <chrono>

// timeout_ms <= 0 means the caller is not using a request deadline.
// The deadline is inclusive: age == timeout_ms has already expired.
inline bool RequestTimedOut(int timeout_ms, SteadyClock::duration age) {
    if (timeout_ms <= 0) return false;
    return age >= std::chrono::milliseconds(timeout_ms);
}
