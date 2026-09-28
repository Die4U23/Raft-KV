#pragma once
#include <cstdint>
#include <memory>
#include <string>
#include <vector>
#include "storage/rocksdb_store.h"

class KVStateMachine {
public:
    explicit KVStateMachine(const std::string& db_path);
    ~KVStateMachine();
    // Empty commands are internal no-ops; errors throw and must fail-stop.
    std::string Apply(int64_t index, const std::string& command);
    std::vector<std::string> ApplyBatch(int64_t first_index,
                                      const std::vector<std::string>& commands);
    bool Get(const std::string& key, std::string* value) const;
    int64_t LastApplied() const { return _store->LastApplied(); }
    // Encoded user keys at LastApplied(). Does not include the applied marker.
    std::string ExportSnapshot() const;
    void InstallSnapshot(int64_t index, const std::string& bytes);
private:
    std::unique_ptr<RocksDBStore> _store;
};
