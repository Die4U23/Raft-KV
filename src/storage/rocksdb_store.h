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
        int64_t index;
        Kind kind;
        std::string key, value;
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
private:
    static std::string AppliedKey();
    std::unique_ptr<rocksdb::DB> _db;
    std::atomic<int64_t> _last_applied{0};
};
