#pragma once

#include <rocksdb/db.h>
#include <string>
#include <vector>

#include "raft_messages.pb.h"

// Raft 日志存储，基于 RocksDB
// Key: log_index (固定 8 字节大端序)
// Value: 序列化的 raftcore::LogEntry
class RaftLog {
public:
    RaftLog(const std::string& db_path);
    ~RaftLog();

    // 追加一条日志（index 由内部自动分配）
    void Append(const raftcore::LogEntry& entry);

    // 获取指定 index 的日志，成功返回 true
    bool Get(int64_t index, raftcore::LogEntry* entry) const;

    // 获取最后一条日志的 index 和 term
    int64_t LastIndex() const;
    int64_t LastTerm() const;

    // 获取指定 index 处日志的 term
    int64_t GetTerm(int64_t index) const;

    // 删除从 start_index 开始（含）之后的所有日志
    void TruncateSuffix(int64_t start_index);

    // ---- Raft 硬状态持久化（currentTerm & votedFor）----
    // 使用固定 sentinel key（index=0 不可用于日志条目）存储
    void SaveHardState(int32_t term, int32_t voted_for);
    bool LoadHardState(int32_t* term, int32_t* voted_for);

private:
    // 将 int64 转为定长大端序 key
    static std::string IndexToKey(int64_t index);

    rocksdb::DB* _db;
    int64_t _last_index;
};
