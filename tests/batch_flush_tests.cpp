#include "common/batch_flush_policy.h"

#include <deque>
#include <iostream>
#include <stdexcept>

using Action = BatchFlushPolicy::Action;
using Plan = BatchFlushPolicy::Plan;

static void Check(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}

// Deterministic event queue, not a Muduo timer or EventLoop implementation.
// Timers remain in the queue after promotion to exercise cancellation races.
class FakeLoop {
public:
    BatchFlushPolicy policy;
    std::deque<Plan> immediate, timers;
    std::deque<size_t> entries;
    size_t queued_bytes = 0, flushes = 0, consumed_entries = 0;

    void Schedule(bool remaining_aged = false) {
        const size_t before = flushes;
        const Plan plan = policy.Request(entries.size(), queued_bytes, remaining_aged);
        if (plan.action == Action::Immediate) immediate.push_back(plan);
        else if (plan.action == Action::Delayed) timers.push_back(plan);
        Check(flushes == before, "scheduling performed inline work");
    }

    void Add(size_t count, size_t bytes_per_entry = 1, bool zero_delay = false) {
        for (size_t i = 0; i < count; ++i) entries.push_back(bytes_per_entry);
        queued_bytes += count * bytes_per_entry;
        Schedule(zero_delay);
    }

    void Fire(Plan callback, bool remaining_aged = false) {
        if (!policy.Consume(callback.token)) return;
        ++flushes;
        size_t count = 0, bytes = 0;
        while (!entries.empty() && count < BatchFlushPolicy::kMaxEntries &&
               entries.front() <= BatchFlushPolicy::kMaxBytes - bytes) {
            bytes += entries.front();
            queued_bytes -= entries.front();
            entries.pop_front();
            ++count;
        }
        Check(count > 0, "callback flushed an empty batch");
        consumed_entries += count;
        const size_t before = flushes;
        Schedule(remaining_aged);
        Check(flushes == before, "remaining batch flushed recursively");
    }

    void RunOneTurn(bool remaining_aged = false) {
        Check(!immediate.empty(), "missing immediate event");
        const Plan callback = immediate.front();
        immediate.pop_front();
        Fire(callback, remaining_aged);
    }

    void FireOldestTimer(bool remaining_aged = false) {
        Check(!timers.empty(), "missing delayed event");
        const Plan callback = timers.front();
        timers.pop_front();
        Fire(callback, remaining_aged);
    }
};

static void EmptyPartialAndDedupe() {
    BatchFlushPolicy policy;
    const Plan empty = policy.Request(0, 0, true);
    Check(empty.action == Action::None && empty.token == 0 && !policy.Consume(0),
          "empty queue scheduled work or consumed a token");
    const Plan delayed = policy.Request(1, 1, false);
    Check(delayed.action == Action::Delayed && delayed.token != 0,
          "first partial queue was not delayed");
    const Plan unchanged = policy.Request(127, BatchFlushPolicy::kMaxBytes - 1, false);
    Check(unchanged.action == Action::None && unchanged.token == 0,
          "partial arrivals reset the initial collection window");
    Check(policy.Consume(delayed.token) && !policy.Consume(delayed.token),
          "original delayed token was lost or duplicate callback was accepted");
    const Plan immediate = policy.Request(1, 1, true);
    Check(immediate.action == Action::Immediate && immediate.token > delayed.token,
          "zero delay did not create a newer immediate token");
    Check(policy.Request(128, BatchFlushPolicy::kMaxBytes, true).action == Action::None,
          "pending immediate callback was duplicated");
    Check(!policy.Consume(delayed.token) && policy.Consume(immediate.token),
          "stale token displaced the pending immediate callback");
}

static void FullAndAgedPromotion() {
    for (int trigger = 0; trigger < 3; ++trigger) {
        BatchFlushPolicy policy;
        const Plan first = policy.Request(1, 1, false);
        const size_t count = trigger == 0 ? BatchFlushPolicy::kMaxEntries : 1;
        const size_t bytes = trigger == 1 ? BatchFlushPolicy::kMaxBytes : 1;
        const Plan promoted = policy.Request(count, bytes, trigger == 2);
        Check(promoted.action == Action::Immediate && promoted.token > first.token,
              "full or aged delayed queue was not promoted with a new token");
        Check(!policy.Consume(first.token) && policy.Consume(promoted.token),
              "promoted timer token was still consumable");
        const Plan direct = policy.Request(count, bytes, trigger == 2);
        Check(direct.action == Action::Immediate && direct.token > promoted.token,
              "full or aged fresh queue did not schedule immediate work");
        Check(policy.Consume(direct.token), "fresh immediate token not consumable");
    }
}

static void StaleTimerAfterNewGeneration() {
    FakeLoop loop;
    loop.Add(1);
    const Plan stale_timer = loop.timers.front();
    loop.Add(127);
    Check(loop.flushes == 0 && loop.immediate.size() == 1,
          "full queue flushed inline or scheduled duplicate work");
    const Plan completed = loop.immediate.front();
    loop.RunOneTurn();
    Check(loop.flushes == 1 && loop.entries.empty(), "first full batch not flushed");
    loop.Add(1);
    const Plan current_timer = loop.timers.back();
    Check(current_timer.token > completed.token && completed.token > stale_timer.token,
          "tokens were not monotonic across promotion and the next generation");
    loop.FireOldestTimer();
    loop.Fire(completed);
    Check(loop.flushes == 1 && loop.entries.size() == 1,
          "stale cancelled timer or duplicate callback flushed the next generation");
    loop.FireOldestTimer();
    loop.Fire(current_timer);
    Check(loop.flushes == 2 && loop.entries.empty(), "current timer did not flush exactly once");
}

static void SeparateTurnsAndFinalPartial() {
    FakeLoop loop;
    loop.Add(2 * BatchFlushPolicy::kMaxEntries + 1);
    Check(loop.flushes == 0 && loop.immediate.size() == 1, "initial flush ran inline");
    loop.RunOneTurn();
    Check(loop.flushes == 1 && loop.consumed_entries == 128 && loop.immediate.size() == 1,
          "multiple full batches did not yield between event turns");
    loop.RunOneTurn();
    Check(loop.flushes == 2 && loop.consumed_entries == 256 && loop.immediate.empty() &&
          loop.timers.size() == 1 && loop.entries.size() == 1,
          "fresh final partial batch did not wait for its collection window");
    loop.FireOldestTimer();
    Check(loop.flushes == 3 && loop.entries.empty(), "final partial timer did not flush");

    FakeLoop aged;
    aged.Add(BatchFlushPolicy::kMaxEntries + 1);
    aged.RunOneTurn(true);
    Check(aged.flushes == 1 && aged.immediate.size() == 1 && aged.timers.empty(),
          "already aged remainder started a new delay or flushed inline");
    aged.RunOneTurn();
    Check(aged.flushes == 2 && aged.entries.empty(), "aged remainder did not flush next turn");

    FakeLoop zero;
    zero.Add(1, 1, true);
    Check(zero.flushes == 0 && zero.timers.empty() && zero.immediate.size() == 1,
          "zero delay did not queue work for a separate event turn");
    zero.RunOneTurn();
    Check(zero.flushes == 1 && zero.entries.empty(), "zero-delay partial batch not flushed");
}

int main() {
    try {
        EmptyPartialAndDedupe();
        FullAndAgedPromotion();
        StaleTimerAfterNewGeneration();
        SeparateTurnsAndFinalPartial();
        std::cout << "PASS: deterministic batch flush policy and fake event turns; not a Muduo test\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL: " << error.what() << '\n';
        return 1;
    }
}
