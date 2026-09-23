#pragma once
#include <cstddef>
#include <string>
#include <vector>

// Per-connection FIFO admission used by src/server/main.cpp.
// Keep this as the only copy of the count/byte limits and accounting.
struct SessionQueueLimits {
    static constexpr size_t kMaxCommands = 1000;
    static constexpr size_t kMaxBytes = 4 * 1024 * 1024;

    static size_t AccountedBytes(size_t consumed, const std::vector<std::string>& args) {
        size_t bytes = consumed;
        for (const auto& arg : args)
            bytes += arg.size();
        return bytes;
    }

    static bool IsFull(size_t queued_commands, size_t queued_bytes) {
        return queued_commands >= kMaxCommands || queued_bytes >= kMaxBytes;
    }
};
