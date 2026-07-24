#pragma once
#include <string>
#include <memory>
#include "storage/rocksdb_store.h"

// Raft 状态机：封装 RocksDBStore，提供 Apply 接口
// Apply 接收序列化的 RESP 命令（如 "*3\r\n$3\r\nSET\r\n..."），
// 执行后返回 RESP 格式的响应字符串
class KVStateMachine {
public:
    explicit KVStateMachine(const std::string& db_path);
    ~KVStateMachine();

    // 应用一条已提交的 Raft 日志命令到状态机
    // command_bytes: 序列化的 RESP 命令（存入 LogEntry.command 的内容）
    // 返回: RESP 响应字符串，如 "+OK\r\n", ":1\r\n", "-ERR ...\r\n"
    std::string Apply(const std::string& command_bytes);

    // 直接读（不经过 Raft，用于 GET）
    bool Get(const std::string& key, std::string* value) const;

private:
    std::unique_ptr<RocksDBStore> _store;
};
