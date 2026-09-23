#pragma once
#include <charconv>
#include <cstdint>
#include <string>
#include <system_error>
#include <vector>

// Shared command classification used by the RESP session and its tests.
// Keep this as the only copy of arity/unknown-command rules.

// Client ids are stored under a reserved key. Empty and NUL are rejected.
// 128 bytes is the session-key budget, not a RESP frame limit.
inline bool ValidClientId(const std::string& id) {
    if (id.empty() || id.size() > 128) return false;
    return id.find('\0') == std::string::npos;
}

// Canonical decimal, no leading zeros, fits in uint64 and starts at 1.
inline bool ParseRequestId(const std::string& text, uint64_t* seq) {
    if (text.empty() || text.size() > 20 || text[0] < '1' || text[0] > '9') return false;
    for (char c : text) if (c < '0' || c > '9') return false;
    uint64_t value = 0;
    const auto parsed = std::from_chars(text.data(), text.data() + text.size(), value);
    if (parsed.ec != std::errc{} || parsed.ptr != text.data() + text.size() || value == 0)
        return false;
    *seq = value;
    return true;
}

struct IdempotentRequest {
    bool present = false;
    std::string client_id;
    uint64_t request_id = 0;
};

// op is the already-uppercased verb. False means the idempotent arity was used
// but the ids are not usable. A legacy SET/DEL leaves present false and returns true.
inline bool ParseIdempotentWrite(const std::string& op, const std::vector<std::string>& args,
                                 IdempotentRequest* out) {
    out->present = false;
    const bool set = op == "SET" && args.size() == 5;
    const bool del = op == "DEL" && args.size() == 4;
    if (!set && !del) return true;
    const std::string& client = set ? args[3] : args[2];
    const std::string& request = set ? args[4] : args[3];
    uint64_t seq = 0;
    if (!ValidClientId(client) || !ParseRequestId(request, &seq)) return false;
    out->present = true;
    out->client_id = client;
    out->request_id = seq;
    return true;
}

inline const char* AppliedWriteReply(bool is_delete, bool key_existed) {
    if (is_delete) return key_existed ? ":1\r\n" : ":0\r\n";
    return "+OK\r\n";
}
inline const char* StaleRequestReply() { return "-ERR stale request id\r\n"; }

struct CommandClass {
    enum Type { READ, WRITE, ERROR };
    Type type = ERROR;
    std::string error;
};

inline CommandClass ClassifyCommand(const std::vector<std::string>& args) {
    if (args.empty())
        return {CommandClass::ERROR, "ERR empty command"};
    const std::string& op = args[0];
    if (op == "SET" && (args.size() == 3 || args.size() == 5)) {
        IdempotentRequest idem;
        if (!ParseIdempotentWrite(op, args, &idem))
            return {CommandClass::ERROR, "ERR invalid client id or request id"};
        return {CommandClass::WRITE, {}};
    }
    if (op == "DEL" && (args.size() == 2 || args.size() == 4)) {
        IdempotentRequest idem;
        if (!ParseIdempotentWrite(op, args, &idem))
            return {CommandClass::ERROR, "ERR invalid client id or request id"};
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
