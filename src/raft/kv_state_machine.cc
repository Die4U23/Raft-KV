#include "raft/kv_state_machine.h"
#include <glog/logging.h>
#include <stdexcept>

// 简单的内联 RESP 解析（避免引入 resp_parser.h 的静态成员重复定义问题）
// 仅解析单个 RESP 数组命令（格式: *N\r\n$L\r\n...）
static bool ParseRespCommand(const std::string& raw,
                             std::vector<std::string>* parts) {
    parts->clear();
    if (raw.empty() || raw[0] != '*') return false;

    size_t pos = 1;
    // 读取数组长度
    size_t crlf = raw.find("\r\n", pos);
    if (crlf == std::string::npos) return false;
    int count = std::stoi(raw.substr(pos, crlf - pos));
    pos = crlf + 2;

    for (int i = 0; i < count; ++i) {
        if (pos >= raw.size() || raw[pos] != '$') return false;
        pos++;  // skip '$'
        crlf = raw.find("\r\n", pos);
        if (crlf == std::string::npos) return false;
        int len = std::stoi(raw.substr(pos, crlf - pos));
        pos = crlf + 2;
        if (pos + len + 2 > raw.size()) return false;
        parts->push_back(raw.substr(pos, len));
        pos += len + 2;  // skip data + \r\n
    }
    return true;
}

KVStateMachine::KVStateMachine(const std::string& db_path) {
    _store.reset(new RocksDBStore(db_path));
}

KVStateMachine::~KVStateMachine() = default;

std::string KVStateMachine::Apply(const std::string& command_bytes) {
    std::vector<std::string> parts;
    if (!ParseRespCommand(command_bytes, &parts) || parts.empty()) {
        return "-ERR empty command\r\n";
    }

    std::string op = parts[0];
    for (auto& c : op) c = toupper(c);

    if (op == "SET") {
        if (parts.size() != 3) return "-ERR wrong number of arguments for SET\r\n";
        _store->Put(parts[1], parts[2]);
        return "+OK\r\n";
    } else if (op == "DEL") {
        if (parts.size() != 2) return "-ERR wrong number of arguments for DEL\r\n";
        _store->Delete(parts[1]);
        return ":1\r\n";
    } else {
        return "-ERR unknown command in state machine\r\n";
    }
}

bool KVStateMachine::Get(const std::string& key, std::string* value) const {
    return _store->Get(key, value);
}
