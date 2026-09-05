#pragma once
#include "raft/apply_executor.h"
#include <condition_variable>
#include <mutex>
#include <optional>
#include <thread>

// One queued or running job. The dispatcher must enqueue onto the owner thread
// without throwing; dispatched completions may remain queued after Stop returns.
// Stop/destruction must run outside the worker and before Work dependencies die.
class SerialApplyExecutor final : public ApplyExecutor {
public:
    explicit SerialApplyExecutor(std::function<void(std::function<void()>)> post_to_owner);
    ~SerialApplyExecutor() override;
    SerialApplyExecutor(const SerialApplyExecutor&) = delete;
    SerialApplyExecutor& operator=(const SerialApplyExecutor&) = delete;
    bool Submit(Work work, Completion completion) override;
    void Stop();
private:
    struct Job { Work work; Completion completion; };
    void Run();
    std::function<void(std::function<void()>)> _post_to_owner;
    std::mutex _mutex;
    std::condition_variable _ready;
    std::optional<Job> _job;
    bool _busy = false;
    bool _stopping = false;
    std::once_flag _stop_once;
    std::thread _worker;
};
