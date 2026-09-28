#include "raft_log.h"
#include "storage/storage_error.h"
#include <rocksdb/write_batch.h>
#include <cstdint>
#include <cstring>
#include <limits>

RaftLog::RaftLog(const std::string& path) {
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::DB* raw = nullptr;
    const auto status = rocksdb::DB::Open(options, path, &raw);
    _db.reset(raw);
    RequireStorageOK(status, "open Raft log");
    LoadSnapshot();
    _last_index = _snapshot_index;
    _last_term = _snapshot_term;

    // Validate the durable sequence instead of interpreting hard state as a log.
    std::unique_ptr<rocksdb::Iterator> it(_db->NewIterator(rocksdb::ReadOptions()));
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        const std::string key = it->key().ToString();
        if (key == IndexToKey(0) || key == SnapshotMetaKey() || key == SnapshotDataKey()) continue;
        raftcore::LogEntry entry;
        if (_last_index == std::numeric_limits<int64_t>::max() ||
            key != IndexToKey(_last_index + 1) ||
            !entry.ParseFromString(it->value().ToString()) ||
            entry.index() != _last_index + 1 || entry.term() <= 0)
            throw std::runtime_error("corrupt or non-contiguous Raft log");
        ++_last_index;
        _last_term = entry.term();
    }
    RequireStorageOK(it->status(), "scan Raft log");
}
RaftLog::~RaftLog() = default;

std::string RaftLog::SnapshotMetaKey() { return "raftkv-snapshot-meta"; }
std::string RaftLog::SnapshotDataKey() { return "raftkv-snapshot-data"; }
std::string RaftLog::IndexToKey(int64_t index) {
    std::string key(8, '\0');
    auto value = static_cast<uint64_t>(index);
    for (int i = 7; i >= 0; --i) {
        key[i] = static_cast<char>(value & 0xff);
        value >>= 8;
    }
    return key;
}
void RaftLog::Append(const raftcore::LogEntry& entry) {
    AppendBatch({entry});
}
void RaftLog::AppendBatch(const std::vector<raftcore::LogEntry>& entries) {
    if (entries.empty()) return;
    rocksdb::WriteBatch batch;
    int64_t last = _last_index;
    for (const auto& entry : entries) {
        if (last == std::numeric_limits<int64_t>::max() ||
            entry.index() != last + 1 || entry.term() <= 0)
            throw std::runtime_error("invalid Raft append index or term");
        std::string value;
        if (!entry.SerializeToString(&value)) throw std::runtime_error("serialize Raft log");
        batch.Put(IndexToKey(entry.index()), value);
        last = entry.index();
    }
    RequireStorageOK(_db->Write(DurableWriteOptions(), &batch), "append Raft log batch");
    _last_index = last;
    _last_term = entries.back().term();
}
bool RaftLog::Get(int64_t index, raftcore::LogEntry* entry) const {
    if (index <= _snapshot_index || index > _last_index) return false;
    std::string value;
    RequireStorageOK(_db->Get(rocksdb::ReadOptions(), IndexToKey(index), &value),
                     "read Raft log");
    if (!entry->ParseFromString(value) || entry->index() != index || entry->term() <= 0)
        throw std::runtime_error("corrupt Raft log entry");
    return true;
}
int64_t RaftLog::GetTerm(int64_t index) const {
    if (index == 0) return 0;
    if (index == _snapshot_index) return _snapshot_term;
    if (index < _snapshot_index) return -1;
    raftcore::LogEntry entry;
    return Get(index, &entry) ? entry.term() : -1;
}
void RaftLog::TruncateSuffix(int64_t start) {
    if (start <= _snapshot_index) throw std::runtime_error("cannot truncate snapshotted prefix");
    if (start > _last_index) return;
    const int64_t new_last_index = start - 1;
    const int64_t new_last_term = new_last_index == 0 ? 0 : GetTerm(new_last_index);
    rocksdb::WriteBatch batch;
    for (int64_t index = start;; ++index) {
        batch.Delete(IndexToKey(index));
        if (index == _last_index) break;
    }
    RequireStorageOK(_db->Write(DurableWriteOptions(), &batch), "truncate Raft log");
    _last_index = new_last_index;
    _last_term = new_last_term;
}
void RaftLog::SaveHardState(int32_t term, int32_t voted_for) {
    if (term < 0 || voted_for < -1) throw std::runtime_error("invalid hard state");
    std::string value(8, '\0');
    uint32_t fields[] = {static_cast<uint32_t>(term), static_cast<uint32_t>(voted_for)};
    for (int field = 0; field < 2; ++field)
        for (int byte = 0; byte < 4; ++byte)
            value[field * 4 + byte] = static_cast<char>((fields[field] >> (8 * byte)) & 0xff);
    RequireStorageOK(_db->Put(DurableWriteOptions(), IndexToKey(0), value),
                     "persist Raft hard state");
}
bool RaftLog::LoadHardState(int32_t* term, int32_t* voted_for) {
    std::string value;
    const auto status = _db->Get(rocksdb::ReadOptions(), IndexToKey(0), &value);
    if (status.IsNotFound() && _last_index == 0) {
        *term = 0;
        *voted_for = -1;
        return false;
    }
    RequireStorageOK(status, "read Raft hard state");
    if (value.size() != 8) throw std::runtime_error("corrupt Raft hard state");
    uint32_t fields[2] = {};
    for (int field = 0; field < 2; ++field)
        for (int byte = 0; byte < 4; ++byte)
            fields[field] |= uint32_t(static_cast<uint8_t>(value[field * 4 + byte])) << (8 * byte);
    if (fields[0] > uint32_t(INT32_MAX) ||
        (fields[1] > uint32_t(INT32_MAX) && fields[1] != UINT32_MAX))
        throw std::runtime_error("invalid Raft hard state");
    *term = static_cast<int32_t>(fields[0]);
    *voted_for = fields[1] == UINT32_MAX ? -1 : static_cast<int32_t>(fields[1]);
    return true;
}
void RaftLog::LoadSnapshot() {
    std::string meta;
    const auto status = _db->Get(rocksdb::ReadOptions(), SnapshotMetaKey(), &meta);
    if (status.IsNotFound()) return;
    RequireStorageOK(status, "read Raft snapshot");
    if (meta.size() != 16) throw std::runtime_error("corrupt Raft snapshot metadata");
    uint64_t fields[2] = {};
    for (int field = 0; field < 2; ++field)
        for (int byte = 0; byte < 8; ++byte)
            fields[field] = (fields[field] << 8) |
                            static_cast<uint8_t>(meta[field * 8 + byte]);
    if (fields[0] == 0 || fields[0] > uint64_t(INT64_MAX) || fields[1] == 0 ||
        fields[1] > uint64_t(INT64_MAX))
        throw std::runtime_error("invalid Raft snapshot metadata");
    std::string data;
    RequireStorageOK(_db->Get(rocksdb::ReadOptions(), SnapshotDataKey(), &data),
                     "read Raft snapshot data");
    if (data.size() > kMaxSnapshotBytes) throw std::runtime_error("Raft snapshot too large");
    _snapshot_index = static_cast<int64_t>(fields[0]);
    _snapshot_term = static_cast<int64_t>(fields[1]);
    _snapshot_data = std::move(data);
}
void RaftLog::CompactApplied(int64_t index, int64_t term, const std::string& data) {
    if (index <= _snapshot_index) return;
    if (index > _last_index || GetTerm(index) != term)
        throw std::runtime_error("cannot snapshot an absent Raft entry");
    PersistSnapshot(index, term, data, true);
}
void RaftLog::InstallSnapshot(int64_t index, int64_t term, const std::string& data) {
    if (index <= 0 || term <= 0) throw std::runtime_error("invalid Raft snapshot");
    if (index < _snapshot_index) throw std::runtime_error("Raft snapshot would move backwards");
    if (index == _snapshot_index) {
        if (term != _snapshot_term) throw std::runtime_error("Raft snapshot term mismatch");
        return;
    }
    const bool keep_suffix = index <= _last_index && GetTerm(index) == term;
    PersistSnapshot(index, term, data, keep_suffix);
}
void RaftLog::PersistSnapshot(int64_t index, int64_t term, const std::string& data, bool keep_suffix) {
    if (index <= 0 || term <= 0 || data.size() > kMaxSnapshotBytes)
        throw std::runtime_error("invalid Raft snapshot");
    std::string meta(16, '\0');
    uint64_t fields[] = {static_cast<uint64_t>(index), static_cast<uint64_t>(term)};
    for (int field = 0; field < 2; ++field) {
        auto value = fields[field];
        for (int byte = 7; byte >= 0; --byte) {
            meta[field * 8 + byte] = static_cast<char>(value & 0xff);
            value >>= 8;
        }
    }
    rocksdb::WriteBatch batch;
    batch.Put(SnapshotMetaKey(), meta);
    batch.Put(SnapshotDataKey(), data);
    const int64_t delete_through = keep_suffix ? index : _last_index;
    for (int64_t entry = _snapshot_index + 1; entry <= delete_through; ++entry) {
        batch.Delete(IndexToKey(entry));
        if (entry == delete_through) break;
    }
    RequireStorageOK(_db->Write(DurableWriteOptions(), &batch), "persist Raft snapshot");
    _snapshot_index = index;
    _snapshot_term = term;
    _snapshot_data = data;
    if (!keep_suffix) {
        _last_index = index;
        _last_term = term;
    }
}
