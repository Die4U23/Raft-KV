#include "raft/kv_state_machine.h"
#include "common/command_type.h"
#include "common/resp_parser.h"
#include "raft/membership.h"
#include <cctype>
#include <limits>
#include <map>
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
        std::string explicit_reply;
    };
    struct ConfigOverlay {
        bool loaded = false;
        uint64_t current = 0;
        std::map<uint64_t, std::string> fresh;
    };
    std::vector<Mutation> mutations;
    std::vector<Plan> plans;
    mutations.reserve(commands.size());
    plans.reserve(commands.size());
    std::unordered_map<std::string, Seen> sessions;
    std::unordered_map<std::string, ConfigOverlay> configs;
    int64_t index = first_index;
    auto note_session = [&](IdempotentRequest& idem, Seen& seen, Plan* plan, Mutation* mutation) {
        if (!idem.present || plan->rejected || plan->cached_from >= 0 || plan->from_store) return;
        mutation->remember_session = true;
        mutation->session_client = idem.client_id;
        mutation->session_seq = idem.request_id;
        if (!plan->explicit_reply.empty()) mutation->session_reply = plan->explicit_reply;
        seen.seq = idem.request_id;
        seen.origin = static_cast<int>(mutations.size());
        seen.reply.clear();
    };
    auto lookup_session = [&](const IdempotentRequest& idem, Plan* plan) -> Seen* {
        if (!idem.present) return nullptr;
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
            if (seen.origin >= 0) plan->cached_from = seen.origin;
            else {
                plan->from_store = true;
                plan->stored_reply = seen.reply;
            }
        } else if (!first && !next) {
            plan->rejected = true;
        }
        return &seen;
    };
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
            MemberCommand member;
            bool member_ok = false;
            if (ParseMemberCommand(command, &member, &member_ok)) {
                if (!member_ok) throw std::runtime_error("invalid membership command in committed Raft log");
                plan.explicit_reply = "+OK\r\n";
            } else {
            const bool write = (op == "SET" && (parts.size() == 3 || parts.size() == 5)) ||
                               (op == "DEL" && (parts.size() == 2 || parts.size() == 4));
            const bool config = (op == "CFGSET" || op == "CFGROLLBACK") &&
                                (parts.size() == 3 || parts.size() == 5);
            if (!write && !config)
                throw std::runtime_error("unsupported command in committed Raft log");
            IdempotentRequest idem;
            if (!ParseIdempotentWrite(op, parts, &idem))
                throw std::runtime_error("invalid idempotent command in committed Raft log");
            if (config && (parts[1].empty() || ReservedUserKey(parts[1])))
                throw std::runtime_error("invalid config command in committed Raft log");
            uint64_t rollback_version = 0;
            if (op == "CFGROLLBACK" && !ParseRequestId(parts[2], &rollback_version))
                throw std::runtime_error("invalid config command in committed Raft log");
            Seen* seen = lookup_session(idem, &plan);
            if (!plan.rejected && plan.cached_from < 0 && !plan.from_store &&
                ReservedUserKey(parts[1])) {
                plan.explicit_reply = ReservedKeyReply();
            } else if (!plan.rejected && plan.cached_from < 0 && !plan.from_store && config) {
                auto& overlay = configs[parts[1]];
                if (!overlay.loaded) {
                    uint64_t current = 0;
                    std::string value;
                    if (_store->ReadCurrentConfig(parts[1], &current, &value))
                        overlay.current = current;
                    overlay.loaded = true;
                }
                if (op == "CFGROLLBACK") {
                    std::string previous;
                    const auto fresh = overlay.fresh.find(rollback_version);
                    const bool found = fresh != overlay.fresh.end() ||
                        _store->ReadConfigVersion(parts[1], rollback_version, &previous);
                    if (fresh != overlay.fresh.end()) previous = fresh->second;
                    if (!found || rollback_version == 0 || rollback_version > overlay.current) {
                        plan.explicit_reply = NoSuchConfigReply();
                    } else if (overlay.current == std::numeric_limits<uint64_t>::max()) {
                        throw std::runtime_error("config version exhausted");
                    } else {
                        const uint64_t next = overlay.current + 1;
                        overlay.current = next;
                        overlay.fresh[next] = previous;
                        mutation.config_write = true;
                        mutation.config_version = next;
                        mutation.key = parts[1];
                        mutation.value = previous;
                        plan.explicit_reply = IntegerReply(next);
                    }
                } else if (overlay.current == std::numeric_limits<uint64_t>::max()) {
                    throw std::runtime_error("config version exhausted");
                } else {
                    const uint64_t next = overlay.current + 1;
                    overlay.current = next;
                    overlay.fresh[next] = parts[2];
                    mutation.config_write = true;
                    mutation.config_version = next;
                    mutation.key = parts[1];
                    mutation.value = parts[2];
                    plan.explicit_reply = IntegerReply(next);
                }
            } else if (!plan.rejected && plan.cached_from < 0 && !plan.from_store) {
                if (op == "SET") {
                    mutation.kind = Mutation::Kind::Put;
                    mutation.value = std::move(parts[2]);
                } else {
                    mutation.kind = Mutation::Kind::Delete;
                }
                mutation.key = std::move(parts[1]);
            }
            if (seen != nullptr && (mutation.config_write || mutation.kind != Mutation::Kind::Noop))
                note_session(idem, *seen, &plan, &mutation);
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
        else if (!plans[i].explicit_reply.empty())
            replies.push_back(plans[i].explicit_reply);
        else
            replies.push_back(AppliedWriteReply(mutations[i].kind == Mutation::Kind::Delete,
                                                existed[i]));
    }
    return replies;
}
bool KVStateMachine::Get(const std::string& key, std::string* value) const {
    return _store->Get(key, value);
}
bool KVStateMachine::GetConfig(const std::string& key, uint64_t* version, std::string* value) const {
    return _store->ReadCurrentConfig(key, version, value);
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
    std::vector<RocksDBStore::ConfigHistory> configs;
};

bool ReadBytes(const std::string& in, size_t* offset, uint32_t len, std::string* out) {
    if (*offset > in.size() || in.size() - *offset < len) return false;
    *out = in.substr(*offset, len);
    *offset += len;
    return true;
}

bool ReadU64(const std::string& in, size_t* offset, uint64_t* value) {
    if (*offset > in.size() || in.size() - *offset < 8) return false;
    uint64_t parsed = 0;
    for (int i = 0; i < 8; ++i)
        parsed = (parsed << 8) | static_cast<uint8_t>(in[*offset + static_cast<size_t>(i)]);
    *offset += 8;
    *value = parsed;
    return true;
}
void AppendU64(std::string* out, uint64_t value) {
    char bytes[8];
    for (int i = 7; i >= 0; --i) {
        bytes[i] = static_cast<char>(value & 0xff);
        value >>= 8;
    }
    out->append(bytes, 8);
}

bool DecodeSnapshot(const std::string& data, SnapshotImage* image) {
    if (data.empty() || (data[0] != 1 && data[0] != 2 && data[0] != 3)) return false;
    size_t offset = 1;
    uint32_t count = 0;
    if (!ReadU32(data, &offset, &count)) return false;
    image->users.clear();
    image->sessions.clear();
    image->configs.clear();
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
    if (data[0] != 3) return offset == data.size();
    uint32_t histories = 0;
    if (!ReadU32(data, &offset, &histories)) return false;
    image->configs.reserve(histories);
    for (uint32_t i = 0; i < histories; ++i) {
        uint32_t name_len = 0, versions = 0;
        std::string name;
        if (!ReadU32(data, &offset, &name_len) || !ReadBytes(data, &offset, name_len, &name) ||
            name.empty() || name[0] == '\0' || !ReadU32(data, &offset, &versions) || versions == 0)
            return false;
        RocksDBStore::ConfigHistory history;
        history.name = std::move(name);
        for (uint32_t v = 0; v < versions; ++v) {
            uint64_t version = 0;
            uint32_t value_len = 0;
            std::string value;
            if (!ReadU64(data, &offset, &version) || version != static_cast<uint64_t>(v) + 1 ||
                !ReadU32(data, &offset, &value_len) || !ReadBytes(data, &offset, value_len, &value))
                return false;
            history.versions.emplace_back(version, std::move(value));
        }
        for (const auto& existing : image->configs)
            if (existing.name == history.name) return false;
        image->configs.push_back(std::move(history));
    }
    return offset == data.size();
}
}

bool KVStateMachine::TryExportSnapshot(std::string* out) const {
    const auto entries = _store->ExportUserKeys();
    const auto sessions = _store->ExportSessions();
    const auto configs = _store->ExportConfigs();
    if (entries.size() > static_cast<size_t>(std::numeric_limits<uint32_t>::max()) ||
        sessions.size() > static_cast<size_t>(std::numeric_limits<uint32_t>::max()) ||
        configs.size() > static_cast<size_t>(std::numeric_limits<uint32_t>::max()))
        return false;
    std::string encoded;
    encoded.push_back(configs.empty() ? kSnapshotVersion : 3);
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
    if (!configs.empty()) {
        AppendU32(&encoded, static_cast<uint32_t>(configs.size()));
        for (const auto& config : configs) {
            if (config.versions.size() > static_cast<size_t>(std::numeric_limits<uint32_t>::max()) ||
                config.name.size() > static_cast<size_t>(std::numeric_limits<uint32_t>::max()))
                return false;
            AppendU32(&encoded, static_cast<uint32_t>(config.name.size()));
            encoded.append(config.name);
            AppendU32(&encoded, static_cast<uint32_t>(config.versions.size()));
            for (const auto& version : config.versions) {
                if (version.second.size() > static_cast<size_t>(std::numeric_limits<uint32_t>::max()))
                    return false;
                AppendU64(&encoded, version.first);
                AppendU32(&encoded, static_cast<uint32_t>(version.second.size()));
                encoded.append(version.second);
            }
        }
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
    _store->ReplaceAll(index, image.users, &image.sessions, &image.configs);
    return true;
}
void KVStateMachine::InstallSnapshot(int64_t index, const std::string& data) {
    if (!TryInstallSnapshot(index, data))
        throw std::runtime_error("corrupt KV snapshot");
}
