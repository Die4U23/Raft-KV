#pragma once
#include <rocksdb/db.h>
#include <rocksdb/write_batch.h>
#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

class RocksDBStore {
public:
    struct Mutation {
        enum class Kind { Put, Delete, Noop };
        Mutation(int64_t index, Kind kind, std::string key = {}, std::string value = {})
            : index(index), kind(kind), key(std::move(key)), value(std::move(value)) {}
        int64_t index;
        Kind kind;
        std::string key, value;
        // Set for a new client request id. The reply stored with the session
        // is the same string AppliedWriteReply produces for this mutation.
        bool remember_session = false;
        std::string session_client;
        uint64_t session_seq = 0;
    };
    explicit RocksDBStore(const std::string& db_path);
    ~RocksDBStore();
    RocksDBStore(const RocksDBStore&) = delete;
    RocksDBStore& operator=(const RocksDBStore&) = delete;
    bool Get(const std::string& key, std::string* value) const;
    void ApplyPut(int64_t index, const std::string& key, const std::string& value);
    bool ApplyDelete(int64_t index, const std::string& key);
    void ApplyNoop(int64_t index);
    // Apply methods have one serial writer; Get and LastApplied may run concurrently.
    // Delete results observe earlier mutations in the same atomic batch.
    std::vector<bool> ApplyBatch(const std::vector<Mutation>& mutations);
    int64_t LastApplied() const { return _last_applied.load(std::memory_order_acquire); }
    // User keys only. Keys that start with NUL are reserved for store metadata.
    std::vector<std::pair<std::string, std::string>> ExportUserKeys() const;
    // client_id -> raw session value. Corrupt records throw.
    std::vector<std::pair<std::string, std::string>> ExportSessions() const;
    // False when this client has no session. Corrupt records throw.
    bool ReadSession(const std::string& client_id, uint64_t* seq, std::string* reply) const;
    static bool DecodeSessionValue(const std::string& raw, uint64_t* seq, std::string* reply);
    // Replace user keys and set lastApplied. index must not move backwards.
    // sessions == nullptr keeps existing client sessions (not used for snapshots).
    // A non-null vector replaces the session table, including an empty one.
    void ReplaceAll(int64_t index,
                    const std::vector<std::pair<std::string, std::string>>& entries,
                    const std::vector<std::pair<std::string, std::string>>* sessions = nullptr);
private:
    static std::string AppliedKey();
    std::unique_ptr<rocksdb::DB> _db;
    std::atomic<int64_t> _last_applied{0};
};
