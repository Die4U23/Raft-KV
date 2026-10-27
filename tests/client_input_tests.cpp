// Production input buffer and drain eligibility, with no sockets or mock parser.
#include "common/command_buffer.h"
#include "common/session_queue.h"
#include <iostream>
#include <stdexcept>
#include <string>

static int checks = 0;
static void Check(bool ok, const char* message) {
    ++checks;
    if (!ok) throw std::runtime_error(message);
}
static const std::string ping = "*1\r\n$4\r\nPING\r\n";

static void FragmentedInputWaitsForNewBytes() {
    for (size_t split = 1; split < ping.size(); ++split) {
        CommandBuffer input;
        Check(input.Append(ping.substr(0, split)), "partial append failed");
        Check(input.CanParse(), "new input must permit a parse attempt");
        Check(input.Next().state == RespParser::State::NeedMore, "fragment not recognized");
        Check(!input.CanParse(), "incomplete input must not reschedule a drain");
        Check(input.UnreadBytes() == split, "incomplete input was consumed");
        Check(input.Append(""), "empty append failed");
        Check(!input.CanParse(), "empty append must not wake an incomplete frame");
        Check(input.Append(ping.substr(split)), "remaining append failed");
        Check(input.CanParse(), "new bytes must wake the parser");
        const auto result = input.Next();
        Check(result.state == RespParser::State::Complete && result.args[0] == "PING",
              "completed fragment must produce one PING");
        Check(!input.CanParse() && input.UnreadBytes() == 0, "PING consumed more than once");
    }
}

static void PipelineContinuesButPartialTailSleeps() {
    CommandBuffer input;
    std::string commands;
    for (int i = 0; i < 257; ++i) commands += ping;
    Check(input.Append(commands + ping.substr(0, 8)), "pipeline append failed");
    for (int i = 0; i < 257; ++i) {
        Check(input.CanParse(), "complete pipeline stalled across drain budget");
        Check(input.Next().state == RespParser::State::Complete, "pipeline command lost");
    }
    Check(input.Next().state == RespParser::State::NeedMore, "partial tail not recognized");
    Check(!input.CanParse(), "partial pipeline tail must wait for network input");
    Check(input.Append(ping.substr(8)), "tail append failed");
    Check(input.Next().state == RespParser::State::Complete, "tail completion lost");
    Check(!input.CanParse(), "empty buffer scheduled again");
}

static void InvalidInputMustRemainObservable() {
    CommandBuffer input;
    Check(input.Append("*1\r\n$4\r\nPINGxx"), "invalid input append failed");
    Check(input.CanParse(), "invalid frame was hidden");
    Check(input.Next().state == RespParser::State::Invalid, "malformed frame accepted");
}

static void ProductionDrainResumesAfterCapacityRelease() {
    CommandBuffer input;
    SessionCommandQueue queue;
    for (size_t i = 0; i < SessionQueueLimits::kMaxCommands; ++i)
        Check(queue.Enqueue({"PING"}, ping.size()), "queue setup failed");
    Check(input.Append(ping + ping.substr(0, 8)), "buffered pipeline append failed");
    auto drained = DrainCommands(input, queue, SessionQueueLimits::kMaxCommandsPerTurn, true);
    Check(drained.stop_read && drained.enqueued == 0 && input.CanParse(),
          "full queue must preserve buffered commands for later progress");
    queue.PopFront();
    queue.PopFront();
    drained = DrainCommands(input, queue, SessionQueueLimits::kMaxCommandsPerTurn, true);
    Check(drained.enqueued == 1 && drained.consumed_bytes == ping.size(),
          "capacity release must resume a buffered command without new input");
    Check(!input.CanParse(), "production drain must put the partial tail to sleep");
    drained = DrainCommands(input, queue, SessionQueueLimits::kMaxCommandsPerTurn, true);
    Check(drained.enqueued == 0 && !drained.invalid && !drained.stop_read,
          "idle partial tail must make no progress and keep reads enabled");
    Check(input.Append(ping.substr(8)), "tail completion append failed");
    drained = DrainCommands(input, queue, SessionQueueLimits::kMaxCommandsPerTurn, true);
    Check(drained.enqueued == 1 && !input.CanParse(),
          "production drain must resume once after the tail arrives");
}

int main() {
    try {
        FragmentedInputWaitsForNewBytes();
        PipelineContinuesButPartialTailSleeps();
        InvalidInputMustRemainObservable();
        ProductionDrainResumesAfterCapacityRelease();
        Check(checks > 0, "no assertions executed");
        std::cout << "PASS: client input (" << checks << " checks)\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL: " << error.what() << '\n';
        return 1;
    }
}
