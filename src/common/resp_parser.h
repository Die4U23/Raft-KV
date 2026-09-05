#pragma once
#include <charconv>
#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

// Stateless RESP command decoder. The caller owns a per-connection buffer and
// consumes bytes only after Complete. Null arrays/bulk strings are not commands.
class RespParser {
public:
    enum class State { Complete, NeedMore, Invalid };
    struct Result {
        State state = State::NeedMore;
        size_t consumed = 0;
        std::vector<std::string> args;
        const char* error = "";
    };
    static constexpr size_t kMaxCommandBytes = 1024 * 1024;
    static constexpr size_t kMaxArguments = 1024;

    static Result TryParseOne(std::string_view raw) {
        Result out;
        size_t pos = 0;
        auto invalid = [&](const char* error) {
            out.state = State::Invalid;
            out.error = error;
            out.args.clear();
            return out;
        };
        auto number = [&](char prefix, size_t limit, size_t* value) -> State {
            if (pos == raw.size()) return State::NeedMore;
            if (raw[pos] != prefix) return State::Invalid;
            const size_t end = raw.find("\r\n", pos);
            if (end == std::string_view::npos) {
                return raw.size() - pos > 22 ? State::Invalid : State::NeedMore;
            }
            const auto digits = raw.substr(pos + 1, end - pos - 1);
            if (digits.empty() || digits.size() > 20) return State::Invalid;
            for (char c : digits) if (c < '0' || c > '9') return State::Invalid;
            const auto parsed = std::from_chars(digits.data(),
                                               digits.data() + digits.size(), *value);
            if (parsed.ec != std::errc{} || parsed.ptr != digits.data() + digits.size()
                || *value > limit) return State::Invalid;
            pos = end + 2;
            return State::Complete;
        };
        size_t count = 0;
        auto state = number('*', kMaxArguments, &count);
        if (state == State::Invalid) return invalid("invalid array length");
        if (state == State::NeedMore) return out;
        if (count == 0) return invalid("empty command");
        out.args.reserve(count);
        for (size_t i = 0; i < count; ++i) {
            size_t length = 0;
            state = number('$', kMaxCommandBytes, &length);
            if (state == State::Invalid) return invalid("invalid bulk length");
            if (state == State::NeedMore) return out;
            if (pos > kMaxCommandBytes - 2 ||
                length > kMaxCommandBytes - pos - 2)
                return invalid("command too large");
            if (raw.size() - pos < length + 2) return out;
            if (raw[pos + length] != '\r' || raw[pos + length + 1] != '\n')
                return invalid("invalid bulk terminator");
            out.args.emplace_back(raw.substr(pos, length));
            pos += length + 2;
        }
        out.state = State::Complete;
        out.consumed = pos;
        return out;
    }
};
