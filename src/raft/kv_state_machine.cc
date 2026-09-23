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

namespace {
constexpr char kSnapshotVersion = 1;

void AppendU32(std::string* out, uint32_t value) {
    char bytes[4];
    for (int i = 3; i >= 0; --i) {
        bytes[i] = static_cast<char>(value & 0xff);
        value >>= 8;
    }
    out->append(bytes, 4);
}
bool ReadU32(const std::string& in, size_t* offset, uint32_t* value) {
    if (*offset > in.size() || in.size() - *offset < 4) return false;
    uint32_t parsed = 0;
    for (int i = 0; i < 4; ++i)
        parsed = (parsed << 8) | static_cast<uint8_t>(in[*offset + static_cast<size_t>(i)]);
    *offset += 4;
    *value = parsed;
    return true;
}
bool DecodeSnapshot(const std::string& data,
                    std::vector<std::pair<std::string, std::string>>* entries) {
    if (data.empty() || data[0] != kSnapshotVersion) return false;
    size_t offset = 1;
    uint32_t count = 0;
    if (!ReadU32(data, &offset, &count)) return false;
    entries->clear();
    entries->reserve(count);
    for (uint32_t i = 0; i < count; ++i) {
        uint32_t key_len = 0, value_len = 0;
        if (!ReadU32(data, &offset, &key_len) || offset > data.size() ||
            data.size() - offset < key_len) return false;
        std::string key = data.substr(offset, key_len);
        offset += key_len;
        if (!ReadU32(data, &offset, &value_len) || offset > data.size() ||
            data.size() - offset < value_len) return false;
        std::string value = data.substr(offset, value_len);
        offset += value_len;
        if (key.empty() || key[0] == '\0') return false;
        entries->emplace_back(std::move(key), std::move(value));
    }
    return offset == data.size();
}
}

bool KVStateMachine::TryExportSnapshot(std::string* out) const {
    const auto entries = _store->ExportUserKeys();
    if (entries.size() > static_cast<size_t>(std::numeric_limits<uint32_t>::max())) return false;
    std::string encoded;
    encoded.push_back(kSnapshotVersion);
    AppendU32(&encoded, static_cast<uint32_t>(entries.size()));
    for (const auto& entry : entries) {
        if (entry.first.size() > static_cast<size_t>(std::numeric_limits<uint32_t>::max()) ||
            entry.second.size() > static_cast<size_t>(std::numeric_limits<uint32_t>::max()))
            return false;
        AppendU32(&encoded, static_cast<uint32_t>(entry.first.size()));
        encoded.append(entry.first);
        AppendU32(&encoded, static_cast<uint32_t>(entry.second.size()));
        encoded.append(entry.second);
    }
    *out = std::move(encoded);
    return true;
}
bool KVStateMachine::IsSnapshot(const std::string& data) const {
    std::vector<std::pair<std::string, std::string>> entries;
    return DecodeSnapshot(data, &entries);
}
bool KVStateMachine::TryInstallSnapshot(int64_t index, const std::string& data) {
    std::vector<std::pair<std::string, std::string>> entries;
    if (!DecodeSnapshot(data, &entries)) return false;
    _store->ReplaceAll(index, entries);
    return true;
}
void KVStateMachine::InstallSnapshot(int64_t index, const std::string& data) {
    if (!TryInstallSnapshot(index, data))
        throw std::runtime_error("corrupt KV snapshot");
}
