#include "raft/kv_state_machine.h"
#include "common/command_type.h"
#include "common/resp_parser.h"
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <unordered_map>
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
std::string ClientSessionKey(const std::string& client_id) {
    return std::string(1, '\0') + "raftkv:client:" + client_id;
}
std::string EncodeSession(uint64_t request_id, const std::string& reply) {
    std::string out(8, '\0');
    for (int byte = 7; byte >= 0; --byte) {
        out[byte] = static_cast<char>(request_id & 0xff);
        request_id >>= 8;
    }
    out.append(reply);
    return out;
}
struct SessionRecord {
    uint64_t request_id = 0;
    std::string reply;
};
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
    std::vector<std::string> replies;
    mutations.reserve(commands.size());
    replies.reserve(commands.size());
    std::unordered_map<std::string, bool> present;
    std::unordered_map<std::string, SessionRecord> sessions;
    auto key_present = [&](const std::string& key) {
        const auto found = present.find(key);
        if (found != present.end()) return found->second;
        std::string ignored;
        return _store->Get(key, &ignored);
    };
    auto session_for = [&](const std::string& client_id) {
        const auto found = sessions.find(client_id);
        if (found != sessions.end()) return found->second;
        SessionRecord record;
        std::string raw;
        if (_store->Get(ClientSessionKey(client_id), &raw)) {
            if (raw.size() < 8) throw std::runtime_error("corrupt client session");
            uint64_t request_id = 0;
            for (int byte = 0; byte < 8; ++byte)
                request_id = (request_id << 8) | static_cast<unsigned char>(raw[byte]);
            record.request_id = request_id;
            record.reply = raw.substr(8);
        }
        sessions.emplace(client_id, record);
        return record;
    };
    int64_t index = first_index;
    for (const auto& command : commands) {
        if (!mutations.empty()) {
            if (index == std::numeric_limits<int64_t>::max())
                throw std::runtime_error("state machine index overflow");
            ++index;
        }
        Mutation mutation{index, Mutation::Kind::Noop, {}, {}};
        std::string reply = "+OK\r\n";
        if (!command.empty()) {
            auto parsed = RespParser::TryParseOne(command);
            if (parsed.state != RespParser::State::Complete || parsed.consumed != command.size())
                throw std::runtime_error("invalid command in committed Raft log");
            auto& parts = parsed.args;
            const std::string op = UpperCommand(parts[0]);
            if (op == "SET" && parts.size() == 3) {
                mutation.kind = Mutation::Kind::Put;
                mutation.key = std::move(parts[1]);
                mutation.value = std::move(parts[2]);
            } else if (op == "DEL" && parts.size() == 2) {
                mutation.kind = Mutation::Kind::Delete;
                mutation.key = std::move(parts[1]);
            } else if (op == "IDEMP") {
                const auto idempotent = ParseIdempotentCommand(parts);
                if (!idempotent.valid)
                    throw std::runtime_error("unsupported command in committed Raft log");
                auto record = session_for(idempotent.client_id);
                if (idempotent.request_id < record.request_id) {
                    reply = "-ERR stale request\r\n";
                } else if (idempotent.request_id == record.request_id) {
                    reply = record.reply;
                } else if (idempotent.op == "SET") {
                    mutation.kind = Mutation::Kind::Put;
                    mutation.key = idempotent.key;
                    mutation.value = idempotent.value;
                    reply = "+OK\r\n";
                } else {
                    mutation.kind = Mutation::Kind::Delete;
                    mutation.key = idempotent.key;
                    reply = key_present(idempotent.key) ? ":1\r\n" : ":0\r\n";
                }
                if (idempotent.request_id > record.request_id) {
                    record.request_id = idempotent.request_id;
                    record.reply = reply;
                    sessions[idempotent.client_id] = record;
                    mutation.session_key = ClientSessionKey(idempotent.client_id);
                    mutation.session_value = EncodeSession(record.request_id, record.reply);
                }
            } else {
                throw std::runtime_error("unsupported command in committed Raft log");
            }
        }
        if (mutation.kind == Mutation::Kind::Put) present[mutation.key] = true;
        else if (mutation.kind == Mutation::Kind::Delete) present[mutation.key] = false;
        if (mutation.kind == Mutation::Kind::Delete && reply == "+OK\r\n")
            reply = "";
        mutations.push_back(std::move(mutation));
        replies.push_back(std::move(reply));
    }
    const auto existed = _store->ApplyBatch(mutations);
    for (size_t i = 0; i < mutations.size(); ++i) {
        if (!replies[i].empty()) continue;
        replies[i] = existed[i] ? ":1\r\n" : ":0\r\n";
    }
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
