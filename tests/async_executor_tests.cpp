#include "raft/serial_apply_executor.h"
#include <chrono>
#include <deque>
#include <future>
#include <iostream>
#include <stdexcept>

using namespace std::chrono_literals;
static void Check(bool ok, const char* message) {
    if (!ok) throw std::runtime_error(message);
}
template<class F> static void Throws(F call) {
    bool threw = false;
    try { call(); } catch (const std::invalid_argument&) { threw = true; }
    Check(threw, "missing functor was accepted");
}
class OwnerQueue {
public:
    void Post(std::function<void()> completion) {
        std::lock_guard<std::mutex> lock(mutex);
        queue.push_back(std::move(completion));
        ready.notify_one();
    }
    void Wait() {
        std::unique_lock<std::mutex> lock(mutex);
        Check(ready.wait_for(lock, 2s, [&] { return !queue.empty(); }), "completion dispatch timed out");
    }
    void Pump() {
        std::function<void()> completion;
        {
            std::lock_guard<std::mutex> lock(mutex);
            Check(!queue.empty(), "missing completion");
            completion = std::move(queue.front()); queue.pop_front();
        }
        completion();
    }
    bool Empty() {
        std::lock_guard<std::mutex> lock(mutex);
        return queue.empty();
    }
private:
    std::mutex mutex;
    std::condition_variable ready;
    std::deque<std::function<void()>> queue;
};
static void BoundedAndOwnerDispatched() {
    OwnerQueue owner;
    std::promise<void> started, release;
    auto started_future = started.get_future();
    auto release_future = release.get_future().share();
    const auto owner_id = std::this_thread::get_id();
    int completed = 0;
    SerialApplyExecutor executor([&](auto completion) { owner.Post(std::move(completion)); });
    Check(executor.Submit([&] {
        Check(std::this_thread::get_id() != owner_id, "work ran on owner thread");
        started.set_value();
        Check(release_future.wait_for(2s) == std::future_status::ready, "worker release timed out");
        return ApplyExecutor::Results{"done"};
    }, [&](auto results, auto error) {
        Check(std::this_thread::get_id() == owner_id && !error &&
              results == ApplyExecutor::Results{"done"}, "completion thread/results wrong");
        ++completed;
    }), "initial work rejected");
    Check(started_future.wait_for(2s) == std::future_status::ready, "worker did not start");
    Check(!executor.Submit([] { return ApplyExecutor::Results{}; }, [](auto, auto) {}),
          "busy executor accepted a second job");
    Check(completed == 0 && owner.Empty(), "blocked work completed inline");
    release.set_value();
    owner.Wait();
    Check(completed == 0, "completion ran before owner pumped");
    // The worker slot is reusable even while the first completion remains queued.
    Check(executor.Submit([]() -> ApplyExecutor::Results { throw std::runtime_error("work failed"); },
        [&](auto results, auto error) {
            Check(std::this_thread::get_id() == owner_id && results.empty() && error,
                  "work exception not dispatched to owner");
            try { std::rethrow_exception(error); }
            catch (const std::runtime_error& failure) {
                Check(std::string(failure.what()) == "work failed", "exception changed");
            }
            ++completed;
        }), "busy flag remained set after work finished");
    executor.Stop(); executor.Stop();
    Check(!executor.Submit([] { return ApplyExecutor::Results{}; }, [](auto, auto) {}),
          "stopped executor accepted work");
    owner.Pump(); owner.Pump();
    Check(completed == 2 && owner.Empty(), "completions did not run exactly once");
}
static void ShutdownDrainsAcceptedWork() {
    OwnerQueue owner;
    std::promise<void> started, release;
    auto started_future = started.get_future();
    auto release_future = release.get_future().share();
    bool finished = false, completed = false;
    {
        SerialApplyExecutor executor([&](auto completion) { owner.Post(std::move(completion)); });
        Check(executor.Submit([&] {
            started.set_value();
            Check(release_future.wait_for(2s) == std::future_status::ready, "shutdown release timed out");
            finished = true;
            return ApplyExecutor::Results{};
        }, [&](auto, auto error) { Check(!error, "drained work failed"); completed = true; }),
              "shutdown work rejected");
        Check(started_future.wait_for(2s) == std::future_status::ready, "shutdown worker did not start");
        auto stopper = std::async(std::launch::async, [&] { executor.Stop(); });
        Check(stopper.wait_for(0s) == std::future_status::timeout, "Stop skipped running work");
        release.set_value();
        Check(stopper.wait_for(2s) == std::future_status::ready, "Stop did not join worker");
        stopper.get();
        Check(finished && !completed, "Stop lost work or ran completion inline");
    }
    owner.Pump();
    Check(completed && owner.Empty(), "completion lost after executor destruction");
}
int main() {
    try {
        Throws([] { SerialApplyExecutor executor({}); });
        OwnerQueue owner;
        SerialApplyExecutor executor([&](auto completion) { owner.Post(std::move(completion)); });
        Throws([&] { executor.Submit({}, [](auto, auto) {}); });
        Throws([&] { executor.Submit([] { return ApplyExecutor::Results{}; }, {}); });
        BoundedAndOwnerDispatched(); ShutdownDrainsAcceptedWork();
        std::cout << "PASS: bounded serial executor, owner dispatch, failures and draining shutdown\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL: " << error.what() << '\n'; return 1;
    }
}
