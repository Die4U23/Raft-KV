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
// but the ids are not usable. A legacy write leaves present false and returns true.
inline bool ParseIdempotentWrite(const std::string& op, const std::vector<std::string>& args,
                                 IdempotentRequest* out) {
    out->present = false;
    size_t base = 0;
    if (op == "SET" || op == "CFGSET" || op == "CFGROLLBACK") base = 3;
    else if (op == "DEL") base = 2;
    else return true;
    if (args.size() != base + 2) return true;
    const std::string& client = args[base];
    const std::string& request = args[base + 1];
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
    enum Type { READ, WRITE, ERROR, LOCAL };
    Type type = ERROR;
    std::string error;
};

inline const char* ReservedKeyReply() { return "-ERR reserved key\r\n"; }
inline const char* NoSuchConfigReply() { return "-ERR no such config version\r\n"; }
inline std::string IntegerReply(uint64_t value) {
    return ":" + std::to_string(value) + "\r\n";
}

// NUL-prefixed keys are store metadata. Empty keys stay ordinary user keys.
inline bool ReservedUserKey(const std::string& key) {
    return !key.empty() && key[0] == '\0';
}

inline bool ParsePeerIdText(const std::string& text, int* id) {
    if (text.empty() || text.size() > 10 || (text.size() > 1 && text[0] == '0')) return false;
    for (char c : text) if (c < '0' || c > '9') return false;
    int value = 0;
    const auto parsed = std::from_chars(text.data(), text.data() + text.size(), value);
    if (parsed.ec != std::errc{} || parsed.ptr != text.data() + text.size() || value < 0)
        return false;
    *id = value;
    return true;
}

inline CommandClass ClassifyCommand(const std::vector<std::string>& args) {
    if (args.empty())
        return {CommandClass::ERROR, "ERR empty command"};
    const std::string& op = args[0];
    auto idempotent_write = [&](size_t base) -> CommandClass {
        if (args.size() != base && args.size() != base + 2)
            return {CommandClass::ERROR, "ERR wrong number of arguments for '" + op + "' command"};
        if (ReservedUserKey(args[1]))
            return {CommandClass::ERROR, "ERR reserved key"};
        IdempotentRequest idem;
        if (!ParseIdempotentWrite(op, args, &idem))
            return {CommandClass::ERROR, "ERR invalid client id or request id"};
        return {CommandClass::WRITE, {}};
    };
    if (op == "SET" && (args.size() == 3 || args.size() == 5))
        return idempotent_write(3);
    if (op == "DEL" && (args.size() == 2 || args.size() == 4))
        return idempotent_write(2);
    if (op == "CFGSET" && (args.size() == 3 || args.size() == 5))
        return idempotent_write(3);
    if (op == "CFGROLLBACK" && (args.size() == 3 || args.size() == 5)) {
        auto classified = idempotent_write(3);
        if (classified.type != CommandClass::WRITE) return classified;
        uint64_t version = 0;
        if (!ParseRequestId(args[2], &version))
            return {CommandClass::ERROR, "ERR invalid config version"};
        return classified;
    }
    if (op == "MEMBER" && args.size() == 3 && (args[1] == "JOIN" || args[1] == "LEAVE")) {
        int peer = 0;
        if (!ParsePeerIdText(args[2], &peer))
            return {CommandClass::ERROR, "ERR invalid peer id"};
        return {CommandClass::WRITE, {}};
    }
    if (op == "CFGGET" && args.size() == 2) {
        if (args[1].empty() || ReservedUserKey(args[1]))
            return {CommandClass::ERROR, "ERR reserved key"};
        return {CommandClass::READ, {}};
    }
    if (op == "CFGCACHE" && args.size() == 3) {
        if (args[1].empty() || ReservedUserKey(args[1]))
            return {CommandClass::ERROR, "ERR reserved key"};
        return {CommandClass::LOCAL, {}};
    }
    if (op == "AUTH" && args.size() == 2)
        return {CommandClass::LOCAL, {}};
    if (op == "PING" || op == "SELECT" || op == "GET" || op == "INFO") {
        if (op == "GET" && args.size() == 2 && ReservedUserKey(args[1]))
            return {CommandClass::ERROR, "ERR reserved key"};
        if ((op == "PING" && args.size() != 1) ||
            (op == "SELECT" && args.size() != 2) ||
            (op == "GET" && args.size() != 2) ||
            (op == "INFO" && args.size() != 1)) {
            return {CommandClass::ERROR,
                    "ERR wrong number of arguments for '" + op + "' command"};
        }
        return {CommandClass::READ, {}};
    }
    if (op == "SET" || op == "DEL" || op == "CFGSET" || op == "CFGROLLBACK" ||
        op == "CFGGET" || op == "CFGCACHE" || op == "MEMBER" || op == "AUTH") {
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
