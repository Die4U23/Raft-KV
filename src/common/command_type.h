#pragma once
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

// Shared command classification used by the RESP session and its tests.
// Keep this as the only copy of arity/unknown-command rules.
struct CommandClass {
    enum Type { READ, WRITE, ERROR };
    Type type = ERROR;
    std::string error;
};

// Decimal request ids start at 1 and have no leading zero. A retry repeats this id.
inline bool ParseRequestId(const std::string& text, uint64_t* id) {
    if (text.empty() || text.size() > 20 || text[0] == '0') return false;
    uint64_t value = 0;
    for (unsigned char c : text) {
        if (c < '0' || c > '9') return false;
        const uint64_t digit = c - '0';
        if (value > (UINT64_MAX - digit) / 10) return false;
        value = value * 10 + digit;
    }
    *id = value;
    return true;
}

struct IdempotentCommand {
    bool valid = false;
    std::string client_id;
    uint64_t request_id = 0;
    std::string op;
    std::string key;
    std::string value;
    std::string error;
};

inline std::string UpperCommand(std::string text) {
    for (char& c : text) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    return text;
}

// IDEMP <client-id> <request-id> SET <key> <value>
// IDEMP <client-id> <request-id> DEL <key>
// The state machine stores one reply per client id and skips a repeated id.
inline IdempotentCommand ParseIdempotentCommand(const std::vector<std::string>& args) {
    IdempotentCommand parsed;
    if (args.empty() || UpperCommand(args[0]) != "IDEMP") {
        parsed.error = "ERR unknown command";
        return parsed;
    }
    if (args.size() != 5 && args.size() != 6) {
        parsed.error = "ERR wrong number of arguments for 'IDEMP' command";
        return parsed;
    }
    if (args[1].empty() || args[1].size() > 128) {
        parsed.error = "ERR invalid client id";
        return parsed;
    }
    uint64_t request_id = 0;
    if (!ParseRequestId(args[2], &request_id)) {
        parsed.error = "ERR invalid request id";
        return parsed;
    }
    const std::string inner = UpperCommand(args[3]);
    if (inner == "SET" && args.size() == 6) {
        parsed.valid = true;
        parsed.op = "SET";
        parsed.value = args[5];
    } else if (inner == "DEL" && args.size() == 5) {
        parsed.valid = true;
        parsed.op = "DEL";
    } else if (inner == "SET" || inner == "DEL") {
        parsed.error = "ERR wrong number of arguments for '" + inner + "' command";
        return parsed;
    } else {
        parsed.error = "ERR unknown command '" + args[3] + "'";
        return parsed;
    }
    parsed.client_id = args[1];
    parsed.request_id = request_id;
    parsed.key = args[4];
    return parsed;
}

// SET/DEL store the user key at args[1]. IDEMP stores it at args[4].
inline size_t WriteKeyIndex(const std::vector<std::string>& args) {
    return !args.empty() && args[0] == "IDEMP" ? 4 : 1;
}

inline CommandClass ClassifyCommand(const std::vector<std::string>& args) {
    if (args.empty())
        return {CommandClass::ERROR, "ERR empty command"};
    const std::string& op = args[0];
    if (op == "SET" && args.size() == 3)
        return {CommandClass::WRITE, {}};
    if (op == "DEL" && args.size() == 2)
        return {CommandClass::WRITE, {}};
    if (op == "IDEMP") {
        const auto parsed = ParseIdempotentCommand(args);
        if (!parsed.valid) return {CommandClass::ERROR, parsed.error};
        return {CommandClass::WRITE, {}};
    }
    if (op == "PING" || op == "SELECT" || op == "GET" || op == "INFO") {
        if ((op == "PING" && args.size() != 1) ||
            (op == "SELECT" && args.size() != 2) ||
            (op == "GET" && args.size() != 2) ||
            (op == "INFO" && args.size() != 1)) {
            return {CommandClass::ERROR,
                    "ERR wrong number of arguments for '" + op + "' command"};
        }
        return {CommandClass::READ, {}};
    }
    if (op == "SET" || op == "DEL") {
        return {CommandClass::ERROR,
                "ERR wrong number of arguments for '" + op + "' command"};
    }
    return {CommandClass::ERROR, "ERR unknown command '" + op + "'"};
}

// GET linearizable-read failures that mean "try the current leader".
inline bool IsReadIndexRedirectError(const std::string& error) {
    return error == "not leader" || error == "no leader" ||
           error == "leadership lost" || error == "server stopped";
}

// After RequestReadIndex: serve only if the quorum succeeded and this node
// is still leader. A timeout while we are still leader stays a local error
// (MOVED would name ourselves). Losing leadership before the state-machine
// read redirects instead of returning the local value.
enum class LinearizableGetAction { Serve, Redirect, Fail };

inline LinearizableGetAction DecideLinearizableGet(bool success, bool still_leader,
                                                   const std::string& error) {
    if (success && still_leader)
        return LinearizableGetAction::Serve;
    if (!success && still_leader && !IsReadIndexRedirectError(error))
        return LinearizableGetAction::Fail;
    return LinearizableGetAction::Redirect;
}
