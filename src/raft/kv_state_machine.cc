#include "raft/kv_state_machine.h"
#include "common/resp_parser.h"
#include <cctype>
#include <limits>
#include <stdexcept>
#include <utility>

KVStateMachine::KVStateMachine(const std::string& path)
    : _store(std::make_unique<RocksDBStore>(path)) {}
KVStateMachine::~KVStateMachine() = default;

std::string KVStateMachine::Apply(int64_t index, const std::string& command) {
    return ApplyBatch(index, {command})[0];
}
std::vector<std::string> KVStateMachine::ApplyBatch(
    int64_t first_index, const std::vector<std::string>& commands) {
    using Mutation = RocksDBStore::Mutation;
    std::vector<Mutation> mutations;
    mutations.reserve(commands.size());
    int64_t index = first_index;
    for (const auto& command : commands) {
        if (!mutations.empty()) {
            if (index == std::numeric_limits<int64_t>::max())
                throw std::runtime_error("state machine index overflow");
            ++index;
        }
        Mutation mutation{index, Mutation::Kind::Noop, {}, {}};
        if (!command.empty()) {
            auto parsed = RespParser::TryParseOne(command);
            if (parsed.state != RespParser::State::Complete || parsed.consumed != command.size())
                throw std::runtime_error("invalid command in committed Raft log");
            auto& parts = parsed.args;
            std::string op = parts[0];
            for (char& c : op) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
            if (op == "SET" && parts.size() == 3) {
                mutation.kind = Mutation::Kind::Put;
                mutation.value = std::move(parts[2]);
            } else if (op == "DEL" && parts.size() == 2) {
                mutation.kind = Mutation::Kind::Delete;
            } else {
                throw std::runtime_error("unsupported command in committed Raft log");
            }
            mutation.key = std::move(parts[1]);
        }
        mutations.push_back(std::move(mutation));
    }
    const auto existed = _store->ApplyBatch(mutations);
    std::vector<std::string> replies;
    replies.reserve(mutations.size());
    for (size_t i = 0; i < mutations.size(); ++i)
        replies.push_back(mutations[i].kind == Mutation::Kind::Delete
                              ? (existed[i] ? ":1\r\n" : ":0\r\n") : "+OK\r\n");
    return replies;
}
bool KVStateMachine::Get(const std::string& key, std::string* value) const {
    return _store->Get(key, value);
}
