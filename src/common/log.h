#pragma once
// Process-local consensus and session log. Portable tests do not link glog,
// so RaftNode writes here. The Linux server uses the same lines on stderr.
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <utility>

enum class LogLevel { Info, Warning, Error };

class LogLine {
public:
    explicit LogLine(LogLevel level) : level_(level) {}
    LogLine(const LogLine&) = delete;
    LogLine& operator=(const LogLine&) = delete;
    LogLine(LogLine&& other) noexcept
        : level_(other.level_), out_(std::move(other.out_)), active_(other.active_) {
        other.active_ = false;
    }
    LogLine& operator=(LogLine&&) = delete;
    ~LogLine() {
        if (!active_) return;
        const std::string line = out_.str();
        std::lock_guard<std::mutex> lock(Mutex());
        std::cerr << Label(level_) << ' ' << line << '\n';
    }
    template <class T>
    LogLine& operator<<(const T& value) {
        out_ << value;
        return *this;
    }

private:
    static std::mutex& Mutex() {
        static std::mutex mu;
        return mu;
    }
    static const char* Label(LogLevel level) {
        switch (level) {
        case LogLevel::Info: return "INFO";
        case LogLevel::Warning: return "WARNING";
        case LogLevel::Error: return "ERROR";
        }
        return "INFO";
    }
    LogLevel level_;
    std::ostringstream out_;
    bool active_ = true;
};

inline LogLine EventLog(LogLevel level) { return LogLine(level); }
