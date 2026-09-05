#pragma once
#include <exception>
#include <functional>
#include <string>
#include <vector>

class ApplyExecutor {
public:
    using Results = std::vector<std::string>;
    using Work = std::function<Results()>;
    using Completion = std::function<void(Results, std::exception_ptr)>;
    virtual ~ApplyExecutor() = default;
    // Accepted work completes exactly once via a later owner-thread dispatch.
    // Never invoke completion inline or on the worker; keep Work dependencies
    // alive until the executor has drained. A false return accepts no work.
    virtual bool Submit(Work work, Completion completion) = 0;
};
