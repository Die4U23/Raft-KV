#include "rocksdb_store.h"
#include "storage/storage_error.h"
#include <limits>
#include <unordered_map>

std::string RocksDBStore::AppliedKey() {
    // Client keys always start with an ASCII namespace followed by ':'.
    return std::string(1, '\0') + "raftkv:last_applied";
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
