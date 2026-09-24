#include "raft_log.h"
#include "storage/storage_error.h"
#include <rocksdb/write_batch.h>
#include <cstring>
#include <limits>

std::string RaftLog::SnapshotMetaKey() { return std::string("\x01snapmeta", 9); }
std::string RaftLog::SnapshotDataKey() { return std::string("\x01snapdata", 9); }
std::string RaftLog::MembershipKey() { return std::string("\x01members", 9); }
void RaftLog::LoadSnapshot() {
    std::string meta, data;
    const auto meta_status = _db->Get(rocksdb::ReadOptions(), SnapshotMetaKey(), &meta);
    const auto data_status = _db->Get(rocksdb::ReadOptions(), SnapshotDataKey(), &data);
    if (meta_status.IsNotFound() && data_status.IsNotFound()) return;
    RequireStorageOK(meta_status, "read Raft snapshot meta");
    RequireStorageOK(data_status, "read Raft snapshot data");
    if (meta.size() != 13 || meta[0] != 1 || data.empty())
        throw std::runtime_error("corrupt Raft snapshot");
    uint64_t index = 0;
    for (int i = 1; i <= 8; ++i)
        index = (index << 8) | static_cast<uint8_t>(meta[static_cast<size_t>(i)]);
    uint32_t term = 0;
    for (int i = 0; i < 4; ++i)
        term |= uint32_t(static_cast<uint8_t>(meta[9 + i])) << (8 * i);
    if (index == 0 || index > uint64_t(std::numeric_limits<int64_t>::max()) ||
        term == 0 || term > uint32_t(std::numeric_limits<int32_t>::max()))
        throw std::runtime_error("invalid Raft snapshot");
    _snapshot_index = static_cast<int64_t>(index);
    _snapshot_term = static_cast<int32_t>(term);
    _snapshot_data = std::move(data);
}
RaftLog::RaftLog(const std::string& path) {
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::DB* raw = nullptr;
    const auto status = rocksdb::DB::Open(options, path, &raw);
    _db.reset(raw);
    RequireStorageOK(status, "open Raft log");
    LoadSnapshot();

    // Validate the durable sequence instead of interpreting hard state as a log.
    // Entries begin at the snapshot index. The snapshot keys are not log indexes.
    int64_t previous = _snapshot_index;
    std::unique_ptr<rocksdb::Iterator> it(_db->NewIterator(rocksdb::ReadOptions()));
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        const std::string key = it->key().ToString();
        if (key == IndexToKey(0) || key == SnapshotMetaKey() || key == SnapshotDataKey() ||
            key == MembershipKey()) continue;
        raftcore::LogEntry entry;
        if (previous == std::numeric_limits<int64_t>::max() ||
            key != IndexToKey(previous + 1) ||
            !entry.ParseFromString(it->value().ToString()) ||
            entry.index() != previous + 1 || entry.term() <= 0)
            throw std::runtime_error("corrupt or non-contiguous Raft log");
        previous = entry.index();
        _last_index = entry.index();
        _last_term = entry.term();
    }
    RequireStorageOK(it->status(), "scan Raft log");
    if (_snapshot_index > _last_index) {
        _last_index = _snapshot_index;
        _last_term = _snapshot_term;
    }
}
RaftLog::~RaftLog() = default;

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
    if (index <= 0 || index > _last_index || index <= _snapshot_index) return false;
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
void RaftLog::SaveSnapshot(int64_t index, int32_t term, const std::string& data) {
    if (index <= 0 || term <= 0 || data.empty())
        throw std::runtime_error("invalid snapshot");
    if (index < _snapshot_index || (index == _snapshot_index && term != _snapshot_term))
        throw std::runtime_error("snapshot conflicts with the compacted prefix");
    const bool matches = (index == _snapshot_index && term == _snapshot_term) ||
        (index > _snapshot_index && index <= _last_index && GetTerm(index) == term);
    const bool keep_suffix = matches && index < _last_index;
    const int64_t delete_to = keep_suffix ? index : _last_index;
    rocksdb::WriteBatch batch;
    std::string meta(13, '\0');
    meta[0] = 1;
    auto encoded_index = static_cast<uint64_t>(index);
    for (int i = 8; i >= 1; --i) {
        meta[i] = static_cast<char>(encoded_index & 0xff);
        encoded_index >>= 8;
    }
    auto encoded_term = static_cast<uint32_t>(term);
    for (int i = 0; i < 4; ++i)
        meta[9 + i] = static_cast<char>((encoded_term >> (8 * i)) & 0xff);
    batch.Put(SnapshotMetaKey(), meta);
    batch.Put(SnapshotDataKey(), data);
    if (delete_to > 0) {
        for (int64_t entry = 1;; ++entry) {
            batch.Delete(IndexToKey(entry));
            if (entry == delete_to) break;
        }
    }
    RequireStorageOK(_db->Write(DurableWriteOptions(), &batch), "persist Raft snapshot");
    if (!keep_suffix) {
        _last_index = index;
        _last_term = term;
    }
    _snapshot_index = index;
    _snapshot_term = term;
    _snapshot_data = data;
}
void RaftLog::TruncateSuffix(int64_t start) {
    if (start <= 0) throw std::runtime_error("cannot truncate hard state");
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
void RaftLog::SaveMembership(const std::string& blob) {
    if (blob.empty()) throw std::runtime_error("empty membership record");
    RequireStorageOK(_db->Put(DurableWriteOptions(), MembershipKey(), blob),
                     "persist Raft membership");
}
bool RaftLog::LoadMembership(std::string* blob) const {
    const auto status = _db->Get(rocksdb::ReadOptions(), MembershipKey(), blob);
    if (status.IsNotFound()) return false;
    RequireStorageOK(status, "read Raft membership");
    if (blob->empty()) throw std::runtime_error("corrupt Raft membership");
    return true;
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
