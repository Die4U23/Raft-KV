#pragma once
#include <rocksdb/db.h>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>
#include "common/byte_sink.h"
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
    int64_t LastTerm() const { return _last_term; }
    int64_t GetTerm(int64_t index) const;
    void TruncateSuffix(int64_t start_index);
    void SaveHardState(int32_t term, int32_t voted_for);
    bool LoadHardState(int32_t* term, int32_t* voted_for);
    // Applied voter set. Missing means the caller should use every configured peer.
    void SaveMembership(const std::string& blob);
    bool LoadMembership(std::string* blob) const;
    // Drop the log prefix through index. A matching entry keeps the suffix.
    // A missing or conflicting index discards the suffix as well.
    void SaveSnapshot(int64_t index, int32_t term, const std::string& data);
    int64_t SnapshotIndex() const { return _snapshot_index; }
    int32_t SnapshotTerm() const { return _snapshot_term; }
    size_t SnapshotSize() const { return _snapshot_size; }
    // Copies one range. The stored image stays in chunk keys, not in this object.
    void ReadSnapshot(size_t offset, size_t n, std::string* out) const;
    std::string SnapshotData() const;
    // Receiver staging. Chunks live in the log database until PromoteStaging.
    void ClearStaging();
    void StageSnapshotBytes(size_t offset, const std::string& data);
    size_t StagingSize() const { return _staging_size; }
    bool StagingMatches(size_t offset, const std::string& data) const;
    void ReadStaging(size_t offset, size_t n, std::string* out) const;
    void PromoteStaging(int64_t index, int32_t term);
    // Writes at most kStoreChunkBytes before flushing a chunk key.
    class SnapshotWriter : public ByteSink {
    public:
        explicit SnapshotWriter(RaftLog* log);
        ~SnapshotWriter() override;
        void Write(const char* data, size_t n) override;
        void Commit(int64_t index, int32_t term);
    private:
        RaftLog* _log;
        std::string _pending;
        uint32_t _generation = 0;
        uint32_t _chunks = 0;
        size_t _total = 0;
        bool _committed = false;
    };
    static constexpr size_t kStoreChunkBytes = 1024 * 1024;
private:
    static std::string IndexToKey(int64_t index);
    static std::string SnapshotMetaKey();
    static std::string SnapshotDataKey();
    static std::string MembershipKey();
    static std::string SnapshotGenKey();
    static std::string ChunkKey(char kind, uint32_t generation, uint32_t index);
    void LoadSnapshot();
    uint32_t NextGeneration() const;
    void WriteChunk(char kind, uint32_t generation, uint32_t index, const std::string& data);
    void ReadChunk(char kind, uint32_t generation, uint32_t index, std::string* out) const;
    void DeleteChunks(char kind, uint32_t generation, uint32_t begin, uint32_t end);
    void ReadRange(char kind, uint32_t generation, size_t size, size_t offset, size_t n,
                   std::string* out) const;
    void FinishSnapshot(int64_t index, int32_t term, size_t bytes, uint32_t chunks,
                        uint32_t generation);
    void DiscardPartialChunks();
    std::unique_ptr<rocksdb::DB> _db;
    int64_t _last_index = 0;
    int64_t _last_term = 0;
    int64_t _snapshot_index = 0;
    int32_t _snapshot_term = 0;
    size_t _snapshot_size = 0;
    uint32_t _snapshot_chunks = 0;
    uint32_t _snapshot_generation = 0;
    size_t _staging_size = 0;
    uint32_t _staging_chunks = 0;
    // Only set when opening a snapshot written before chunked storage.
    std::string _legacy_snapshot;
};
