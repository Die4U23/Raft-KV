#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>
#include <stdexcept>

// Owner-thread scheduling decisions only: no callbacks, timers or inline work.
// The caller queues Immediate plans and schedules Delayed plans. Cancellation
// may race a timer callback, so every callback must first Consume its token.
class BatchFlushPolicy {
public:
    static constexpr size_t kMaxEntries = 128;
    static constexpr size_t kMaxBytes = 1024 * 1024;

    enum class Action { None, Delayed, Immediate };
    struct Plan {
        Action action = Action::None;
        uint64_t token = 0;
    };

    Plan Request(size_t queued_entries, size_t queued_bytes, bool delay_elapsed_or_zero) {
        if (queued_entries == 0 || _pending == Action::Immediate) return {};
        const bool immediate = queued_entries >= kMaxEntries ||
                               queued_bytes >= kMaxBytes || delay_elapsed_or_zero;
        // Additional partial arrivals retain the original collection window.
        if (_pending == Action::Delayed && !immediate) return {};
        if (_generation == std::numeric_limits<uint64_t>::max())
            throw std::overflow_error("batch flush token overflow");
        _pending = immediate ? Action::Immediate : Action::Delayed;
        return {_pending, ++_generation};
    }

    bool Consume(uint64_t token) {
        if (_pending == Action::None || token != _generation) return false;
        _pending = Action::None;
        return true;
    }

private:
    Action _pending = Action::None;
    uint64_t _generation = 0;
};
