#pragma once
#include "common/command_buffer.h"
#include "common/command_type.h"
#include "common/metrics.h"
#include <cctype>
#include <cstddef>
#include <deque>
#include <string>
#include <vector>

// Per-connection FIFO used by src/server/main.cpp DrainClient/ExecuteNextCommand.
// Tests fail if admission, accounting, or classification drift from production.
struct SessionQueueLimits {
    static constexpr size_t kMaxCommands = 1000;
    static constexpr size_t kMaxBytes = 4 * 1024 * 1024;
    static constexpr size_t kMaxCommandsPerTurn = 128;

    // One count of the bytes this command contributes to the queue. The wire
    // frame already contains the arguments; adding both charged the payload twice.
    static size_t AccountedBytes(size_t consumed, const std::vector<std::string>& args) {
        size_t retained = 0;
        for (const auto& arg : args)
            retained += arg.size();
        return consumed > retained ? consumed : retained;
    }

    static bool IsFull(size_t queued_commands, size_t queued_bytes) {
        return queued_commands >= kMaxCommands || queued_bytes >= kMaxBytes;
    }
};

struct QueuedCommand {
    CommandClass::Type type = CommandClass::ERROR;
    std::vector<std::string> args;
    SteadyClock::time_point enqueued_at{};
    std::string error_message;
    size_t bytes = 0;
};

struct SessionCommandQueue {
    std::deque<QueuedCommand> items;
    size_t queued_bytes = 0;

    bool IsFull() const {
        return SessionQueueLimits::IsFull(items.size(), queued_bytes);
    }

    bool Empty() const { return items.empty(); }
    size_t size() const { return items.size(); }

    // Uppercases the verb, classifies, and accounts the same bytes DrainClient uses.
    bool Enqueue(std::vector<std::string> args, size_t consumed) {
        if (IsFull())
            return false;
        if (!args.empty()) {
            for (char& c : args[0])
                c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
        }
        const auto classified = ClassifyCommand(args);
        QueuedCommand cmd;
        cmd.type = classified.type;
        cmd.error_message = classified.error;
        cmd.args = std::move(args);
        cmd.bytes = SessionQueueLimits::AccountedBytes(consumed, cmd.args);
        cmd.enqueued_at = SteadyClock::now();
        queued_bytes += cmd.bytes;
        items.push_back(std::move(cmd));
        return true;
    }

    QueuedCommand PopFront() {
        auto cmd = std::move(items.front());
        items.pop_front();
        queued_bytes -= cmd.bytes;
        return cmd;
    }
};

// A broken RESP frame is not a queued command. Replying while another command
// is executing, waiting on Raft, or still queued makes the client treat this
// error as that command's response.
inline bool ShouldReplyInvalidFrame(bool executing, bool waiting, bool queue_empty) {
    return !executing && !waiting && queue_empty;
}

inline bool CanDeliverClientReply(bool connected, bool closing) {
    return connected && !closing;
}

struct DrainOutcome {
    bool stop_read = false;
    bool invalid = false;
    const char* invalid_error = "";
    size_t enqueued = 0;
    size_t consumed_bytes = 0;
};

// Production DrainClient parse/admission loop without Muduo.
inline DrainOutcome DrainCommands(CommandBuffer& input, SessionCommandQueue& queue,
                                  size_t max_per_turn, bool keep_reading) {
    DrainOutcome out;
    if (!keep_reading)
        return out;
    for (size_t handled = 0; handled < max_per_turn; ++handled) {
        if (queue.IsFull()) {
            out.stop_read = true;
            break;
        }
        auto parsed = input.Next();
        if (parsed.state == RespParser::State::NeedMore)
            break;
        if (parsed.state == RespParser::State::Invalid) {
            out.stop_read = true;
            out.invalid = true;
            out.invalid_error = parsed.error;
            return out;
        }
        if (!queue.Enqueue(std::move(parsed.args), parsed.consumed)) {
            out.stop_read = true;
            break;
        }
        out.consumed_bytes += parsed.consumed;
        ++out.enqueued;
    }
    return out;
}
