#include "raft/serial_apply_executor.h"
#include <stdexcept>
#include <utility>

SerialApplyExecutor::SerialApplyExecutor(std::function<void(std::function<void()>)> post_to_owner)
    : _post_to_owner(std::move(post_to_owner)) {
    if (!_post_to_owner) throw std::invalid_argument("apply executor requires owner dispatcher");
    _worker = std::thread([this] { Run(); });
}
SerialApplyExecutor::~SerialApplyExecutor() { Stop(); }

bool SerialApplyExecutor::Submit(Work work, Completion completion) {
    if (!work || !completion) throw std::invalid_argument("apply executor requires work and completion");
    {
        std::lock_guard<std::mutex> lock(_mutex);
        if (_busy || _stopping) return false;
        _job.emplace(Job{std::move(work), std::move(completion)});
        _busy = true;
    }
    _ready.notify_one();
    return true;
}
void SerialApplyExecutor::Stop() {
    std::call_once(_stop_once, [this] {
        {
            std::lock_guard<std::mutex> lock(_mutex);
            _stopping = true;
        }
        _ready.notify_one();
        if (_worker.joinable()) _worker.join();
    });
}
void SerialApplyExecutor::Run() {
    for (;;) {
        Job job;
        {
            std::unique_lock<std::mutex> lock(_mutex);
            _ready.wait(lock, [this] { return _stopping || _job.has_value(); });
            if (!_job) return;
            job = std::move(*_job);
            _job.reset();
        }
        Results results;
        std::exception_ptr error;
        try { results = job.work(); } catch (...) { error = std::current_exception(); }
        {
            std::lock_guard<std::mutex> lock(_mutex);
            _busy = false;
        }
        _post_to_owner([completion = std::move(job.completion), results = std::move(results), error]() mutable {
            completion(std::move(results), error);
        });
    }
}
