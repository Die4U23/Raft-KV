#include "rocksdb_store.h"
#include "common/command_type.h"
#include "storage/storage_error.h"
#include <limits>
#include <unordered_map>

std::string RocksDBStore::AppliedKey() {
    // Client keys always start with an ASCII namespace followed by ':'.
    return std::string(1, '\0') + "raftkv:last_applied";
}
std::string SessionPrefix() {
    return std::string(1, '\0') + "raftkv:client:";
}
std::string SessionKey(const std::string& client_id) {
    return SessionPrefix() + client_id;
}
std::string EncodeSession(uint64_t seq, const std::string& reply) {
    std::string out(8, '\0');
    auto value = seq;
    for (int i = 7; i >= 0; --i) {
        out[static_cast<size_t>(i)] = static_cast<char>(value & 0xff);
        value >>= 8;
    }
    out += reply;
    return out;
}
bool RocksDBStore::DecodeSessionValue(const std::string& raw, uint64_t* seq, std::string* reply) {
    if (raw.size() < 8) return false;
    uint64_t value = 0;
    for (int i = 0; i < 8; ++i)
        value = (value << 8) | static_cast<uint8_t>(raw[static_cast<size_t>(i)]);
    if (value == 0) return false;
    *seq = value;
    *reply = raw.substr(8);
    return true;
}
RocksDBStore::RocksDBStore(const std::string& path) {
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::DB* raw = nullptr;
    auto status = rocksdb::DB::Open(options, path, &raw);
    _db.reset(raw);
    RequireStorageOK(status, "open KV store");
    std::string value;
    status = _db->Get(rocksdb::ReadOptions(), AppliedKey(), &value);
    if (status.IsNotFound()) {
        std::unique_ptr<rocksdb::Iterator> it(_db->NewIterator(rocksdb::ReadOptions()));
        it->SeekToFirst();
        RequireStorageOK(it->status(), "inspect KV store");
        if (it->Valid())
            throw std::runtime_error("legacy KV database has no lastApplied marker; "
                                     "preserve it and use a fresh paired KV/log path");
        return;
    }
    RequireStorageOK(status, "read lastApplied");
    if (value.size() != 8) throw std::runtime_error("corrupt lastApplied marker");
    uint64_t index = 0;
    for (unsigned char byte : value) index = (index << 8) | byte;
    if (index > uint64_t(INT64_MAX)) throw std::runtime_error("invalid lastApplied");
    _last_applied = static_cast<int64_t>(index);
}
RocksDBStore::~RocksDBStore() = default;

bool RocksDBStore::Get(const std::string& key, std::string* value) const {
    auto status = _db->Get(rocksdb::ReadOptions(), key, value);
    if (status.IsNotFound()) return false;
    RequireStorageOK(status, "read KV");
    return true;
}
std::vector<bool> RocksDBStore::ApplyBatch(const std::vector<Mutation>& mutations) {
    if (mutations.empty()) return {};
    int64_t last = LastApplied();
    for (const auto& mutation : mutations) {
        if (last == std::numeric_limits<int64_t>::max() || mutation.index != last + 1)
            throw std::runtime_error("out-of-order state machine application");
        last = mutation.index;
    }
    rocksdb::WriteBatch batch;
    std::unordered_map<std::string, bool> present;
    std::vector<bool> existed(mutations.size(), false);
    for (size_t i = 0; i < mutations.size(); ++i) {
        const auto& mutation = mutations[i];
        switch (mutation.kind) {
        case Mutation::Kind::Put:
            batch.Put(mutation.key, mutation.value);
            present[mutation.key] = true;
            break;
        case Mutation::Kind::Delete: {
            auto found = present.find(mutation.key);
            if (found == present.end()) {
                std::string old_value;
                found = present.emplace(mutation.key, Get(mutation.key, &old_value)).first;
            }
            existed[i] = found->second;
            found->second = false;
            batch.Delete(mutation.key);
            break;
        }
        case Mutation::Kind::Noop: break;
        default: throw std::runtime_error("invalid state machine mutation");
        }
        if (!mutation.remember_session) continue;
        if (mutation.session_seq == 0 || !ValidClientId(mutation.session_client) ||
            (mutation.kind != Mutation::Kind::Put && mutation.kind != Mutation::Kind::Delete))
            throw std::runtime_error("invalid session update");
        batch.Put(SessionKey(mutation.session_client),
                  EncodeSession(mutation.session_seq,
                                AppliedWriteReply(mutation.kind == Mutation::Kind::Delete,
                                                  existed[i])));
    }
    std::string value(8, '\0');
    auto encoded = static_cast<uint64_t>(last);
    for (int i = 7; i >= 0; --i) {
        value[i] = static_cast<char>(encoded & 0xff);
        encoded >>= 8;
    }
    batch.Put(AppliedKey(), value);
    RequireStorageOK(_db->Write(DurableWriteOptions(), &batch), "apply KV batch");
    _last_applied.store(last, std::memory_order_release);
    return existed;
}
void RocksDBStore::ApplyPut(int64_t index, const std::string& key, const std::string& value) {
    ApplyBatch({{index, Mutation::Kind::Put, key, value}});
}
bool RocksDBStore::ApplyDelete(int64_t index, const std::string& key) {
    return ApplyBatch({{index, Mutation::Kind::Delete, key, {}}})[0];
}
void RocksDBStore::ApplyNoop(int64_t index) {
    ApplyBatch({{index, Mutation::Kind::Noop, {}, {}}});
}
std::vector<std::pair<std::string, std::string>> RocksDBStore::ExportUserKeys() const {
    std::vector<std::pair<std::string, std::string>> entries;
    std::unique_ptr<rocksdb::Iterator> it(_db->NewIterator(rocksdb::ReadOptions()));
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        const std::string key = it->key().ToString();
        if (!key.empty() && key[0] == '\0') continue;
        entries.emplace_back(key, it->value().ToString());
    }
    RequireStorageOK(it->status(), "export KV snapshot");
    return entries;
}
bool RocksDBStore::ReadSession(const std::string& client_id, uint64_t* seq,
                               std::string* reply) const {
    std::string raw;
    if (!Get(SessionKey(client_id), &raw)) return false;
    if (!DecodeSessionValue(raw, seq, reply))
        throw std::runtime_error("corrupt client session");
    return true;
}
std::vector<std::pair<std::string, std::string>> RocksDBStore::ExportSessions() const {
    std::vector<std::pair<std::string, std::string>> sessions;
    const auto prefix = SessionPrefix();
    std::unique_ptr<rocksdb::Iterator> it(_db->NewIterator(rocksdb::ReadOptions()));
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        const std::string key = it->key().ToString();
        if (key.size() < prefix.size() || key.compare(0, prefix.size(), prefix) != 0) continue;
        const std::string client = key.substr(prefix.size());
        uint64_t seq = 0;
        std::string reply;
        if (!ValidClientId(client) || !DecodeSessionValue(it->value().ToString(), &seq, &reply))
            throw std::runtime_error("corrupt client session");
        sessions.emplace_back(client, it->value().ToString());
    }
    RequireStorageOK(it->status(), "export client sessions");
    return sessions;
}
void RocksDBStore::ReplaceAll(int64_t index,
                              const std::vector<std::pair<std::string, std::string>>& entries,
                              const std::vector<std::pair<std::string, std::string>>* sessions) {
    if (index <= 0) throw std::runtime_error("invalid snapshot index");
    if (index < LastApplied()) throw std::runtime_error("snapshot is behind the applied index");
    for (const auto& entry : entries) {
        if (entry.first.empty() || entry.first[0] == '\0')
            throw std::runtime_error("snapshot contains a reserved key");
    }
    if (sessions) {
        for (const auto& session : *sessions) {
            uint64_t seq = 0;
            std::string reply;
            if (!ValidClientId(session.first) ||
                !DecodeSessionValue(session.second, &seq, &reply))
                throw std::runtime_error("snapshot contains a corrupt client session");
        }
    }
    const auto prefix = SessionPrefix();
    std::vector<std::string> stale;
    std::vector<std::string> stale_sessions;
    std::unique_ptr<rocksdb::Iterator> it(_db->NewIterator(rocksdb::ReadOptions()));
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        const std::string key = it->key().ToString();
        if (!key.empty() && key[0] == '\0') {
            if (sessions && key.size() >= prefix.size() &&
                key.compare(0, prefix.size(), prefix) == 0)
                stale_sessions.push_back(key);
            continue;
        }
        stale.push_back(key);
    }
    RequireStorageOK(it->status(), "scan KV before snapshot install");
    rocksdb::WriteBatch batch;
    for (const auto& key : stale) batch.Delete(key);
    for (const auto& key : stale_sessions) batch.Delete(key);
    for (const auto& entry : entries) batch.Put(entry.first, entry.second);
    if (sessions) {
        for (const auto& session : *sessions)
            batch.Put(SessionKey(session.first), session.second);
    }
    std::string value(8, '\0');
    auto encoded = static_cast<uint64_t>(index);
    for (int i = 7; i >= 0; --i) {
        value[i] = static_cast<char>(encoded & 0xff);
        encoded >>= 8;
    }
    batch.Put(AppliedKey(), value);
    RequireStorageOK(_db->Write(DurableWriteOptions(), &batch), "install KV snapshot");
    _last_applied.store(index, std::memory_order_release);
}
