#pragma once
#include <rocksdb/db.h>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>
#include "raft_messages.pb.h"

// Compatible with the original layout: 8-byte big-endian log index;
// index 0 stores the 8-byte little-endian (term, voted_for) hard state.
// Storage failures throw: callers must fail-stop, never acknowledge the write.
class RaftLog {
public:
    explicit RaftLog(const std::string& db_path);
    ~RaftLog();
    RaftLog(const RaftLog&) = delete;
    RaftLog& operator=(const RaftLog&) = delete;
    void Append(const raftcore::LogEntry& entry);
    void AppendBatch(const std::vector<raftcore::LogEntry>& entries);
    bool Get(int64_t index, raftcore::LogEntry* entry) const;
    int64_t LastIndex() const { return _last_index; }
    int64_t LastTerm() const;
    int64_t GetTerm(int64_t index) const;
    void TruncateSuffix(int64_t start_index);
    void SaveHardState(int32_t term, int32_t voted_for);
    bool LoadHardState(int32_t* term, int32_t* voted_for);
private:
    static std::string IndexToKey(int64_t index);
    std::unique_ptr<rocksdb::DB> _db;
    int64_t _last_index = 0;
};
