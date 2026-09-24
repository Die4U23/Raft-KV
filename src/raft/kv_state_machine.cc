#include "raft/kv_state_machine.h"
#include "common/command_type.h"
#include "common/resp_parser.h"
#include <cctype>
#include <limits>
#include <stdexcept>
#include <unordered_map>
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
    struct Seen {
        uint64_t seq = 0;
        int origin = -1;
        std::string reply;
    };
    struct Plan {
        int cached_from = -1;
        bool from_store = false;
        bool rejected = false;
        std::string stored_reply;
    };
    std::vector<Mutation> mutations;
    std::vector<Plan> plans;
    mutations.reserve(commands.size());
    plans.reserve(commands.size());
    std::unordered_map<std::string, Seen> sessions;
    int64_t index = first_index;
    for (const auto& command : commands) {
        if (!mutations.empty()) {
            if (index == std::numeric_limits<int64_t>::max())
                throw std::runtime_error("state machine index overflow");
            ++index;
        }
        Mutation mutation{index, Mutation::Kind::Noop, {}, {}};
        Plan plan;
        if (!command.empty()) {
            auto parsed = RespParser::TryParseOne(command);
            if (parsed.state != RespParser::State::Complete || parsed.consumed != command.size())
                throw std::runtime_error("invalid command in committed Raft log");
            auto& parts = parsed.args;
            std::string op = parts[0];
            for (char& c : op) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
            const bool write = (op == "SET" && (parts.size() == 3 || parts.size() == 5)) ||
                               (op == "DEL" && (parts.size() == 2 || parts.size() == 4));
            if (!write) throw std::runtime_error("unsupported command in committed Raft log");
            IdempotentRequest idem;
            if (!ParseIdempotentWrite(op, parts, &idem))
                throw std::runtime_error("invalid idempotent command in committed Raft log");
            if (idem.present) {
                auto found = sessions.find(idem.client_id);
                if (found == sessions.end()) {
                    Seen seen;
                    if (_store->ReadSession(idem.client_id, &seen.seq, &seen.reply))
                        seen.origin = -1;
                    found = sessions.emplace(idem.client_id, std::move(seen)).first;
                }
                auto& seen = found->second;
                const bool first = seen.seq == 0 && idem.request_id == 1;
                const bool next = seen.seq > 0 && seen.seq != std::numeric_limits<uint64_t>::max() &&
                                  idem.request_id == seen.seq + 1;
                if (seen.seq == idem.request_id) {
                    if (seen.origin >= 0) plan.cached_from = seen.origin;
                    else {
                        plan.from_store = true;
                        plan.stored_reply = seen.reply;
                    }
                } else if (first || next) {
                    mutation.remember_session = true;
                    mutation.session_client = idem.client_id;
                    mutation.session_seq = idem.request_id;
                    seen.seq = idem.request_id;
                    seen.origin = static_cast<int>(mutations.size());
                    seen.reply.clear();
                } else {
                    plan.rejected = true;
                }
            }
            if (!plan.rejected && plan.cached_from < 0 && !plan.from_store) {
                if (op == "SET") {
                    mutation.kind = Mutation::Kind::Put;
                    mutation.value = std::move(parts[2]);
                } else {
                    mutation.kind = Mutation::Kind::Delete;
                }
                mutation.key = std::move(parts[1]);
            }
        }
        mutations.push_back(std::move(mutation));
        plans.push_back(std::move(plan));
    }
    const auto existed = _store->ApplyBatch(mutations);
    std::vector<std::string> replies;
    replies.reserve(mutations.size());
    for (size_t i = 0; i < mutations.size(); ++i) {
        if (plans[i].rejected) replies.push_back(StaleRequestReply());
        else if (plans[i].from_store) replies.push_back(plans[i].stored_reply);
        else if (plans[i].cached_from >= 0)
            replies.push_back(replies[static_cast<size_t>(plans[i].cached_from)]);
        else
            replies.push_back(AppliedWriteReply(mutations[i].kind == Mutation::Kind::Delete,
                                                existed[i]));
    }
    return replies;
}
bool KVStateMachine::Get(const std::string& key, std::string* value) const {
    return _store->Get(key, value);
}

namespace {
constexpr char kSnapshotVersion = 2;

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
struct SnapshotImage {
    std::vector<std::pair<std::string, std::string>> users;
    std::vector<std::pair<std::string, std::string>> sessions;
};

bool ReadBytes(const std::string& in, size_t* offset, uint32_t len, std::string* out) {
    if (*offset > in.size() || in.size() - *offset < len) return false;
    *out = in.substr(*offset, len);
    *offset += len;
    return true;
}

bool DecodeSnapshot(const std::string& data, SnapshotImage* image) {
    if (data.empty() || (data[0] != 1 && data[0] != kSnapshotVersion)) return false;
    size_t offset = 1;
    uint32_t count = 0;
    if (!ReadU32(data, &offset, &count)) return false;
    image->users.clear();
    image->sessions.clear();
    image->users.reserve(count);
    for (uint32_t i = 0; i < count; ++i) {
        uint32_t key_len = 0, value_len = 0;
        std::string key, value;
        if (!ReadU32(data, &offset, &key_len) || !ReadBytes(data, &offset, key_len, &key) ||
            !ReadU32(data, &offset, &value_len) || !ReadBytes(data, &offset, value_len, &value))
            return false;
        if (key.empty() || key[0] == '\0') return false;
        image->users.emplace_back(std::move(key), std::move(value));
    }
    if (data[0] == 1) return offset == data.size();
    uint32_t sessions = 0;
    if (!ReadU32(data, &offset, &sessions)) return false;
    image->sessions.reserve(sessions);
    for (uint32_t i = 0; i < sessions; ++i) {
        uint32_t client_len = 0, value_len = 0;
        std::string client, raw;
        if (!ReadU32(data, &offset, &client_len) || !ReadBytes(data, &offset, client_len, &client) ||
            !ReadU32(data, &offset, &value_len) || !ReadBytes(data, &offset, value_len, &raw))
            return false;
        uint64_t seq = 0;
        std::string reply;
        if (!ValidClientId(client) || !RocksDBStore::DecodeSessionValue(raw, &seq, &reply))
            return false;
        for (const auto& existing : image->sessions)
            if (existing.first == client) return false;
        image->sessions.emplace_back(std::move(client), std::move(raw));
    }
    return offset == data.size();
}
}

bool KVStateMachine::TryExportSnapshot(std::string* out) const {
    const auto entries = _store->ExportUserKeys();
    const auto sessions = _store->ExportSessions();
    if (entries.size() > static_cast<size_t>(std::numeric_limits<uint32_t>::max()) ||
        sessions.size() > static_cast<size_t>(std::numeric_limits<uint32_t>::max()))
        return false;
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
    AppendU32(&encoded, static_cast<uint32_t>(sessions.size()));
    for (const auto& session : sessions) {
        if (session.first.size() > static_cast<size_t>(std::numeric_limits<uint32_t>::max()) ||
            session.second.size() > static_cast<size_t>(std::numeric_limits<uint32_t>::max()))
            return false;
        AppendU32(&encoded, static_cast<uint32_t>(session.first.size()));
        encoded.append(session.first);
        AppendU32(&encoded, static_cast<uint32_t>(session.second.size()));
        encoded.append(session.second);
    }
    *out = std::move(encoded);
    return true;
}
bool KVStateMachine::IsSnapshot(const std::string& data) const {
    SnapshotImage image;
    return DecodeSnapshot(data, &image);
}
bool KVStateMachine::TryInstallSnapshot(int64_t index, const std::string& data) {
    SnapshotImage image;
    if (!DecodeSnapshot(data, &image)) return false;
    _store->ReplaceAll(index, image.users, &image.sessions);
    return true;
}
void KVStateMachine::InstallSnapshot(int64_t index, const std::string& data) {
    if (!TryInstallSnapshot(index, data))
        throw std::runtime_error("corrupt KV snapshot");
}
