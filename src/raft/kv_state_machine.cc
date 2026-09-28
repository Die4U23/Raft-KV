#include "raft/kv_state_machine.h"
#include "common/resp_parser.h"
#include <cctype>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <utility>

namespace {
void AppendU32(std::string* out, uint32_t value) {
    char bytes[4];
    for (int i = 3; i >= 0; --i) {
        bytes[i] = static_cast<char>(value & 0xff);
        value >>= 8;
    }
    out->append(bytes, 4);
}
uint32_t ReadU32(const char* data) {
    uint32_t value = 0;
    for (int i = 0; i < 4; ++i)
        value = (value << 8) | static_cast<uint8_t>(data[i]);
    return value;
}
}

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
std::string KVStateMachine::ExportSnapshot() const {
    const auto entries = _store->ExportEntries();
    if (entries.size() > std::numeric_limits<uint32_t>::max())
        throw std::runtime_error("KV snapshot has too many keys");
    std::string out;
    AppendU32(&out, static_cast<uint32_t>(entries.size()));
    for (const auto& entry : entries) {
        if (entry.first.size() > std::numeric_limits<uint32_t>::max() ||
            entry.second.size() > std::numeric_limits<uint32_t>::max())
            throw std::runtime_error("KV snapshot entry too large");
        AppendU32(&out, static_cast<uint32_t>(entry.first.size()));
        out.append(entry.first);
        AppendU32(&out, static_cast<uint32_t>(entry.second.size()));
        out.append(entry.second);
    }
    return out;
}
void KVStateMachine::InstallSnapshot(int64_t index, const std::string& bytes) {
    if (bytes.size() < 4) throw std::runtime_error("truncated KV snapshot");
    const uint32_t count = ReadU32(bytes.data());
    std::vector<std::pair<std::string, std::string>> entries;
    entries.reserve(count);
    size_t offset = 4;
    for (uint32_t i = 0; i < count; ++i) {
        if (bytes.size() - offset < 4) throw std::runtime_error("truncated KV snapshot");
        const uint32_t key_len = ReadU32(bytes.data() + offset);
        offset += 4;
        if (bytes.size() - offset < key_len + 4) throw std::runtime_error("truncated KV snapshot");
        std::string key = bytes.substr(offset, key_len);
        offset += key_len;
        const uint32_t value_len = ReadU32(bytes.data() + offset);
        offset += 4;
        if (bytes.size() - offset < value_len) throw std::runtime_error("truncated KV snapshot");
        std::string value = bytes.substr(offset, value_len);
        offset += value_len;
        entries.emplace_back(std::move(key), std::move(value));
    }
    if (offset != bytes.size()) throw std::runtime_error("KV snapshot has trailing bytes");
    _store->ReplaceAll(index, entries);
}
