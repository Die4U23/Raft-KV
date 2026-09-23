#pragma once
#include <string>
#include <vector>

// Shared command classification used by the RESP session and its tests.
// Keep this as the only copy of arity/unknown-command rules.
struct CommandClass {
    enum Type { READ, WRITE, ERROR };
    Type type = ERROR;
    std::string error;
};

inline CommandClass ClassifyCommand(const std::vector<std::string>& args) {
    if (args.empty())
        return {CommandClass::ERROR, "ERR empty command"};
    const std::string& op = args[0];
    if (op == "SET" && args.size() == 3)
        return {CommandClass::WRITE, {}};
    if (op == "DEL" && args.size() == 2)
        return {CommandClass::WRITE, {}};
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
