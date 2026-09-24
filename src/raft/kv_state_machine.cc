#include "raft/kv_state_machine.h"
#include "common/command_type.h"
#include "common/resp_parser.h"
#include "raft/membership.h"
#include <algorithm>
#include <cctype>
#include <cstring>
#include <limits>
#include <map>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>
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

class MemorySource : public ByteSource {
public:
    explicit MemorySource(const std::string& data) : _data(data) {}
    bool Read(char* out, size_t n) override {
        if (n > _data.size() - _pos) return false;
        if (n != 0) std::memcpy(out, _data.data() + _pos, n);
        _pos += n;
        return true;
    }
    void Rewind() override { _pos = 0; }
private:
    const std::string& _data;
    size_t _pos = 0;
};

void WriteU32(ByteSink* out, uint32_t value) {
    char bytes[4];
    for (int i = 3; i >= 0; --i) {
        bytes[i] = static_cast<char>(value & 0xff);
        value >>= 8;
    }
    out->Write(bytes, 4);
}
void WriteU64(ByteSink* out, uint64_t value) {
    char bytes[8];
    for (int i = 7; i >= 0; --i) {
        bytes[i] = static_cast<char>(value & 0xff);
        value >>= 8;
    }
    out->Write(bytes, 8);
}
bool ReadU32(ByteSource* in, uint32_t* value) {
    unsigned char bytes[4];
    if (!in->Read(reinterpret_cast<char*>(bytes), 4)) return false;
    *value = (uint32_t(bytes[0]) << 24) | (uint32_t(bytes[1]) << 16) |
             (uint32_t(bytes[2]) << 8) | bytes[3];
    return true;
}
bool ReadU64(ByteSource* in, uint64_t* value) {
    unsigned char bytes[8];
    if (!in->Read(reinterpret_cast<char*>(bytes), 8)) return false;
    uint64_t parsed = 0;
    for (unsigned char byte : bytes) parsed = (parsed << 8) | byte;
    *value = parsed;
    return true;
}
bool ReadString(ByteSource* in, uint32_t len, std::string* out) {
    out->assign(len, '\0');
    return len == 0 || in->Read(&(*out)[0], len);
}
bool Discard(ByteSource* in, uint32_t len) {
    char buffer[4096];
    while (len != 0) {
        const size_t n = std::min(static_cast<size_t>(len), sizeof buffer);
        if (!in->Read(buffer, n)) return false;
        len -= static_cast<uint32_t>(n);
    }
    return true;
}
bool TooWide(size_t n) {
    return n > static_cast<size_t>(std::numeric_limits<uint32_t>::max());
}

// install == nullptr checks the image and drops user values as they are read.
bool WalkSnapshot(ByteSource* in, RocksDBStore* install) {
    char version = 0;
    if (!in->Read(&version, 1) || (version != 1 && version != 2 && version != 3)) return false;
    uint32_t count = 0;
    if (!ReadU32(in, &count)) return false;
    for (uint32_t i = 0; i < count; ++i) {
        uint32_t key_len = 0, value_len = 0;
        std::string key;
        if (!ReadU32(in, &key_len) || !ReadString(in, key_len, &key) ||
            key.empty() || key[0] == '\0' || !ReadU32(in, &value_len))
            return false;
        if (install) {
            std::string value;
            if (!ReadString(in, value_len, &value)) return false;
            install->QueueSnapshotPut(key, value);
        } else if (!Discard(in, value_len)) {
            return false;
        }
    }
    if (version == 1) return true;
    uint32_t sessions = 0;
    if (!ReadU32(in, &sessions)) return false;
    std::unordered_set<std::string> seen_clients;
    for (uint32_t i = 0; i < sessions; ++i) {
        uint32_t client_len = 0, value_len = 0;
        std::string client, raw;
        if (!ReadU32(in, &client_len) || !ReadString(in, client_len, &client) ||
            !ReadU32(in, &value_len) || !ReadString(in, value_len, &raw))
            return false;
        uint64_t seq = 0;
        std::string reply;
        if (!ValidClientId(client) || !RocksDBStore::DecodeSessionValue(raw, &seq, &reply) ||
            !seen_clients.insert(client).second)
            return false;
        if (install) install->QueueSnapshotSession(client, raw);
    }
    if (version != 3) return true;
    uint32_t histories = 0;
    if (!ReadU32(in, &histories)) return false;
    std::unordered_set<std::string> seen_names;
    for (uint32_t i = 0; i < histories; ++i) {
        uint32_t name_len = 0, versions = 0;
        std::string name;
        if (!ReadU32(in, &name_len) || !ReadString(in, name_len, &name) ||
            name.empty() || name[0] == '\0' || !ReadU32(in, &versions) || versions == 0 ||
            !seen_names.insert(name).second)
            return false;
        for (uint32_t v = 0; v < versions; ++v) {
            uint64_t version_id = 0;
            uint32_t value_len = 0;
            std::string value;
            if (!ReadU64(in, &version_id) || version_id != static_cast<uint64_t>(v) + 1 ||
                !ReadU32(in, &value_len) || !ReadString(in, value_len, &value))
                return false;
            if (install)
                install->QueueSnapshotConfig(name, version_id, value, v + 1 == versions);
        }
    }
    return true;
}
}

bool KVStateMachine::WriteSnapshot(ByteSink* out) const {
    uint64_t users = 0;
    bool too_big = false;
    _store->VisitUserKeys([&](const std::string& key, const std::string& value) {
        if (TooWide(key.size()) || TooWide(value.size()) ||
            users == static_cast<uint64_t>(std::numeric_limits<uint32_t>::max()))
            too_big = true;
        else
            ++users;
    });
    if (too_big) return false;
    const auto sessions = _store->ExportSessions();
    const auto configs = _store->ExportConfigs();
    if (TooWide(sessions.size()) || TooWide(configs.size())) return false;
    for (const auto& session : sessions)
        if (TooWide(session.first.size()) || TooWide(session.second.size())) return false;
    for (const auto& config : configs) {
        if (TooWide(config.name.size()) || TooWide(config.versions.size())) return false;
        for (const auto& version : config.versions)
            if (TooWide(version.second.size())) return false;
    }
    const char version = configs.empty() ? kSnapshotVersion : 3;
    out->Write(&version, 1);
    WriteU32(out, static_cast<uint32_t>(users));
    _store->VisitUserKeys([&](const std::string& key, const std::string& value) {
        WriteU32(out, static_cast<uint32_t>(key.size()));
        out->Write(key.data(), key.size());
        WriteU32(out, static_cast<uint32_t>(value.size()));
        out->Write(value.data(), value.size());
    });
    WriteU32(out, static_cast<uint32_t>(sessions.size()));
    for (const auto& session : sessions) {
        WriteU32(out, static_cast<uint32_t>(session.first.size()));
        out->Write(session.first.data(), session.first.size());
        WriteU32(out, static_cast<uint32_t>(session.second.size()));
        out->Write(session.second.data(), session.second.size());
    }
    if (!configs.empty()) {
        WriteU32(out, static_cast<uint32_t>(configs.size()));
        for (const auto& config : configs) {
            WriteU32(out, static_cast<uint32_t>(config.name.size()));
            out->Write(config.name.data(), config.name.size());
            WriteU32(out, static_cast<uint32_t>(config.versions.size()));
            for (const auto& version : config.versions) {
                WriteU64(out, version.first);
                WriteU32(out, static_cast<uint32_t>(version.second.size()));
                out->Write(version.second.data(), version.second.size());
            }
        }
    }
    return true;
}
bool KVStateMachine::TryExportSnapshot(std::string* out) const {
    struct StringSink : ByteSink {
        std::string data;
        void Write(const char* bytes, size_t n) override { data.append(bytes, n); }
    };
    StringSink sink;
    if (!WriteSnapshot(&sink)) return false;
    *out = std::move(sink.data);
    return true;
}
bool KVStateMachine::CheckSnapshot(ByteSource* in) const {
    if (!WalkSnapshot(in, nullptr)) return false;
    char extra = 0;
    return !in->Read(&extra, 1);
}
bool KVStateMachine::IsSnapshot(const std::string& data) const {
    MemorySource in(data);
    return CheckSnapshot(&in);
}
bool KVStateMachine::TryInstallSnapshot(int64_t index, const std::string& data) {
    MemorySource in(data);
    if (!CheckSnapshot(&in)) return false;
    in.Rewind();
    InstallSnapshot(index, &in);
    return true;
}
void KVStateMachine::InstallSnapshot(int64_t index, ByteSource* in) {
    if (!CheckSnapshot(in)) throw std::runtime_error("corrupt KV snapshot");
    if (index <= 0) throw std::runtime_error("invalid snapshot index");
    if (index < _store->LastApplied())
        throw std::runtime_error("snapshot is behind the applied index");
    in->Rewind();
    _store->PrepareSnapshotInstall(index);
    if (!WalkSnapshot(in, _store.get())) throw std::runtime_error("corrupt KV snapshot");
    char extra = 0;
    if (in->Read(&extra, 1)) throw std::runtime_error("corrupt KV snapshot");
    _store->FinishSnapshotInstall();
}
void KVStateMachine::InstallSnapshot(int64_t index, const std::string& data) {
    MemorySource in(data);
    InstallSnapshot(index, &in);
}
