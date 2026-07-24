#include "raft_log.h"
#include <glog/logging.h>
#include <cstring>

RaftLog::RaftLog(const std::string& db_path) : _db(nullptr), _last_index(0) {
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::Status s = rocksdb::DB::Open(options, db_path, &_db);
    if (!s.ok()) {
        LOG(FATAL) << "Failed to open RaftLog RocksDB: " << s.ToString();
    }

    // 初始化时读取当前最后一条日志的 index
    raftcore::LogEntry last_entry;
    if (Get(LastIndex(), &last_entry)) {
        _last_index = last_entry.index();
    } else {
        _last_index = 0;
    }
    LOG(INFO) << "RaftLog initialized, last_index=" << _last_index;
}

RaftLog::~RaftLog() {
    delete _db;
}

std::string RaftLog::IndexToKey(int64_t index) {
    // 大端序编码
    std::string key(8, '\0');
    for (int i = 7; i >= 0; --i) {
        key[i] = static_cast<char>(index & 0xFF);
        index >>= 8;
    }
    return key;
}

void RaftLog::Append(const raftcore::LogEntry& entry) {
    std::string key = IndexToKey(entry.index());
    std::string value;
    entry.SerializeToString(&value);

    rocksdb::Status s = _db->Put(rocksdb::WriteOptions(), key, value);
    if (!s.ok()) {
        LOG(ERROR) << "RaftLog Append failed: " << s.ToString();
        return;
    }
    if (entry.index() > _last_index) {
        _last_index = entry.index();
    }
}

bool RaftLog::Get(int64_t index, raftcore::LogEntry* entry) const {
    std::string key = IndexToKey(index);
    std::string value;
    rocksdb::Status s = _db->Get(rocksdb::ReadOptions(), key, &value);
    if (!s.ok()) return false;
    return entry->ParseFromString(value);
}

int64_t RaftLog::LastIndex() const {
    return _last_index;
}

int64_t RaftLog::LastTerm() const {
    raftcore::LogEntry entry;
    if (Get(_last_index, &entry)) {
        return entry.term();
    }
    return 0;
}

int64_t RaftLog::GetTerm(int64_t index) const {
    raftcore::LogEntry entry;
    if (Get(index, &entry)) {
        return entry.term();
    }
    return -1;  // 不存在
}

void RaftLog::TruncateSuffix(int64_t start_index) {
    // 删除从 start_index 到 _last_index 的所有日志
    int64_t end = _last_index;
    rocksdb::WriteBatch batch;
    for (int64_t i = start_index; i <= end; ++i) {
        batch.Delete(IndexToKey(i));
    }
    _db->Write(rocksdb::WriteOptions(), &batch);
    _last_index = start_index - 1;
    if (_last_index < 0) _last_index = 0;
}

// ---- 硬状态持久化 ----
// 用 index=0 的 sentinel key 存储 (term, voted_for)
// 格式：两个 int32 小端序，共 8 字节

void RaftLog::SaveHardState(int32_t term, int32_t voted_for) {
    std::string key = IndexToKey(0);
    std::string value(8, '\0');
    // little-endian
    value[0] = static_cast<char>(term & 0xFF);
    value[1] = static_cast<char>((term >> 8) & 0xFF);
    value[2] = static_cast<char>((term >> 16) & 0xFF);
    value[3] = static_cast<char>((term >> 24) & 0xFF);
    value[4] = static_cast<char>(voted_for & 0xFF);
    value[5] = static_cast<char>((voted_for >> 8) & 0xFF);
    value[6] = static_cast<char>((voted_for >> 16) & 0xFF);
    value[7] = static_cast<char>((voted_for >> 24) & 0xFF);
    rocksdb::Status s = _db->Put(rocksdb::WriteOptions(), key, value);
    if (!s.ok()) {
        LOG(ERROR) << "RaftLog SaveHardState failed: " << s.ToString();
    }
}

bool RaftLog::LoadHardState(int32_t* term, int32_t* voted_for) {
    std::string key = IndexToKey(0);
    std::string value;
    rocksdb::Status s = _db->Get(rocksdb::ReadOptions(), key, &value);
    if (!s.ok() || value.size() < 8) {
        *term = 0;
        *voted_for = -1;
        return false;
    }
    *term = (static_cast<uint8_t>(value[0])) |
            (static_cast<uint8_t>(value[1]) << 8) |
            (static_cast<uint8_t>(value[2]) << 16) |
            (static_cast<uint8_t>(value[3]) << 24);
    *voted_for = (static_cast<uint8_t>(value[4])) |
                 (static_cast<uint8_t>(value[5]) << 8) |
                 (static_cast<uint8_t>(value[6]) << 16) |
                 (static_cast<uint8_t>(value[7]) << 24);
    return true;
}
