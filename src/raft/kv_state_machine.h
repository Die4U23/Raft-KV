#pragma once
#include <cstdint>
#include <memory>
#include <string>
#include <vector>
#include "common/byte_sink.h"
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
    // False when this name has no published version.
    bool GetConfig(const std::string& key, uint64_t* version, std::string* value) const;
    int64_t LastApplied() const { return _store->LastApplied(); }
    // Versioned image of applied user keys and client sessions.
    // Version 2 is current. Version 1 is a user-key image and clears sessions on install.
    // User keys are written one at a time. False means the image could not be built.
    bool WriteSnapshot(ByteSink* out) const;
    bool TryExportSnapshot(std::string* out) const;
    bool CheckSnapshot(ByteSource* in) const;
    bool IsSnapshot(const std::string& data) const;
    // False means the bytes are not a snapshot. Storage failures still throw.
    bool TryInstallSnapshot(int64_t index, const std::string& data);
    void InstallSnapshot(int64_t index, ByteSource* in);
    void InstallSnapshot(int64_t index, const std::string& data);
private:
    std::unique_ptr<RocksDBStore> _store;
};
