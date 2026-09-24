#include "raft_log.h"
#include "storage/storage_error.h"
#include <rocksdb/write_batch.h>
#include <algorithm>
#include <cstring>
#include <limits>

std::string RaftLog::SnapshotMetaKey() { return std::string("\x01snapmeta", 9); }
std::string RaftLog::SnapshotDataKey() { return std::string("\x01snapdata", 9); }
std::string RaftLog::MembershipKey() { return std::string("\x01members", 9); }
std::string RaftLog::SnapshotGenKey() { return std::string("\x01snapgen", 9); }
std::string RaftLog::ChunkKey(char kind, uint32_t generation, uint32_t index) {
    std::string key(1, '\x01');
    key.append(kind == 'c' ? "snapc" : kind == 'r' ? "snapr" : "snapn");
    auto put = [&](uint32_t value) {
        key.push_back(static_cast<char>((value >> 24) & 0xff));
        key.push_back(static_cast<char>((value >> 16) & 0xff));
        key.push_back(static_cast<char>((value >> 8) & 0xff));
        key.push_back(static_cast<char>(value & 0xff));
    };
    if (kind == 'c') put(generation);
    put(index);
    return key;
}
void RaftLog::LoadSnapshot() {
    std::string meta;
    const auto meta_status = _db->Get(rocksdb::ReadOptions(), SnapshotMetaKey(), &meta);
    if (meta_status.IsNotFound()) return;
    RequireStorageOK(meta_status, "read Raft snapshot meta");
    if (meta.size() < 13 || (meta[0] != 1 && meta[0] != 2))
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
    if (meta[0] == 1) {
        const auto data_status = _db->Get(rocksdb::ReadOptions(), SnapshotDataKey(), &_legacy_snapshot);
        RequireStorageOK(data_status, "read Raft snapshot data");
        if (_legacy_snapshot.empty()) throw std::runtime_error("corrupt Raft snapshot");
        _snapshot_size = _legacy_snapshot.size();
        _snapshot_chunks = 1;
        return;
    }
    if (meta.size() != 21) throw std::runtime_error("corrupt Raft snapshot");
    uint64_t bytes = 0;
    for (int i = 13; i < 21; ++i)
        bytes = (bytes << 8) | static_cast<uint8_t>(meta[static_cast<size_t>(i)]);
    if (bytes == 0) throw std::runtime_error("corrupt Raft snapshot");
    _snapshot_size = static_cast<size_t>(bytes);
    _snapshot_chunks = static_cast<uint32_t>((_snapshot_size + kStoreChunkBytes - 1) / kStoreChunkBytes);
    std::string generation;
    const auto gen_status = _db->Get(rocksdb::ReadOptions(), SnapshotGenKey(), &generation);
    if (!gen_status.IsNotFound()) {
        RequireStorageOK(gen_status, "read Raft snapshot generation");
        if (generation.size() != 4) throw std::runtime_error("corrupt Raft snapshot");
        uint32_t parsed = 0;
        for (unsigned char byte : generation) parsed = (parsed << 8) | byte;
        _snapshot_generation = parsed;
    }
}
RaftLog::RaftLog(const std::string& path) {
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::DB* raw = nullptr;
    const auto status = rocksdb::DB::Open(options, path, &raw);
    _db.reset(raw);
    RequireStorageOK(status, "open Raft log");
    LoadSnapshot();
    DiscardPartialChunks();

    // Validate the durable sequence instead of interpreting hard state as a log.
    // Entries begin at the snapshot index. The snapshot keys are not log indexes.
    int64_t previous = _snapshot_index;
    std::unique_ptr<rocksdb::Iterator> it(_db->NewIterator(rocksdb::ReadOptions()));
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        const std::string key = it->key().ToString();
        if (key == IndexToKey(0) || (!key.empty() && key[0] == '\x01')) continue;
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
    const uint32_t generation = NextGeneration();
    uint32_t count = 0;
    for (size_t offset = 0; offset < data.size(); offset += kStoreChunkBytes) {
        const size_t n = std::min(kStoreChunkBytes, data.size() - offset);
        WriteChunk('c', generation, count, data.substr(offset, n));
        ++count;
    }
    FinishSnapshot(index, term, data.size(), count, generation);
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
uint32_t RaftLog::NextGeneration() const {
    if (_snapshot_generation == std::numeric_limits<uint32_t>::max())
        throw std::runtime_error("snapshot generation exhausted");
    return _snapshot_generation + 1;
}
void RaftLog::WriteChunk(char kind, uint32_t generation, uint32_t index, const std::string& data) {
    RequireStorageOK(_db->Put(DurableWriteOptions(), ChunkKey(kind, generation, index), data),
                     "persist Raft snapshot chunk");
}
void RaftLog::ReadChunk(char kind, uint32_t generation, uint32_t index, std::string* out) const {
    const auto status = _db->Get(rocksdb::ReadOptions(), ChunkKey(kind, generation, index), out);
    RequireStorageOK(status, "read Raft snapshot chunk");
}
void RaftLog::DeleteChunks(char kind, uint32_t generation, uint32_t begin, uint32_t end) {
    for (uint32_t index = begin; index < end; ++index)
        RequireStorageOK(_db->Delete(DurableWriteOptions(), ChunkKey(kind, generation, index)),
                         "drop Raft snapshot chunk");
}
void RaftLog::ReadRange(char kind, uint32_t generation, size_t size, size_t offset, size_t n,
                        std::string* out) const {
    out->clear();
    if (n == 0) return;
    if (offset > size || n > size - offset)
        throw std::runtime_error("snapshot read out of range");
    while (n > 0) {
        const uint32_t index = static_cast<uint32_t>(offset / kStoreChunkBytes);
        const size_t inner = offset % kStoreChunkBytes;
        std::string chunk;
        ReadChunk(kind, generation, index, &chunk);
        if (inner >= chunk.size()) throw std::runtime_error("corrupt Raft snapshot chunk");
        const size_t take = std::min(n, chunk.size() - inner);
        out->append(chunk, inner, take);
        offset += take;
        n -= take;
    }
}
void RaftLog::ReadSnapshot(size_t offset, size_t n, std::string* out) const {
    if (!_legacy_snapshot.empty()) {
        out->clear();
        if (n == 0) return;
        if (offset > _snapshot_size || n > _snapshot_size - offset)
            throw std::runtime_error("snapshot read out of range");
        out->assign(_legacy_snapshot, offset, n);
        return;
    }
    ReadRange('c', _snapshot_generation, _snapshot_size, offset, n, out);
}
std::string RaftLog::SnapshotData() const {
    std::string data;
    if (_snapshot_size == 0) return data;
    ReadSnapshot(0, _snapshot_size, &data);
    return data;
}
void RaftLog::ClearStaging() {
    DeleteChunks('r', 0, 0, _staging_chunks);
    _staging_size = 0;
    _staging_chunks = 0;
}
void RaftLog::StageSnapshotBytes(size_t offset, const std::string& data) {
    if (offset != _staging_size) throw std::runtime_error("snapshot staging gap");
    size_t copied = 0;
    while (copied < data.size()) {
        const uint32_t index = static_cast<uint32_t>(_staging_size / kStoreChunkBytes);
        const size_t inner = _staging_size % kStoreChunkBytes;
        const size_t room = kStoreChunkBytes - inner;
        const size_t take = std::min(room, data.size() - copied);
        std::string chunk;
        if (inner != 0) ReadChunk('r', 0, index, &chunk);
        chunk.append(data, copied, take);
        WriteChunk('r', 0, index, chunk);
        if (index >= _staging_chunks) _staging_chunks = index + 1;
        _staging_size += take;
        copied += take;
    }
}
bool RaftLog::StagingMatches(size_t offset, const std::string& data) const {
    if (data.empty() || offset > _staging_size || data.size() > _staging_size - offset) return false;
    size_t left = data.size();
    size_t at = offset;
    while (left > 0) {
        const uint32_t index = static_cast<uint32_t>(at / kStoreChunkBytes);
        const size_t inner = at % kStoreChunkBytes;
        std::string chunk;
        ReadChunk('r', 0, index, &chunk);
        if (inner >= chunk.size()) return false;
        const size_t take = std::min(left, chunk.size() - inner);
        if (data.compare(data.size() - left, take, chunk, inner, take) != 0) return false;
        at += take;
        left -= take;
    }
    return true;
}
void RaftLog::ReadStaging(size_t offset, size_t n, std::string* out) const {
    ReadRange('r', 0, _staging_size, offset, n, out);
}
void RaftLog::FinishSnapshot(int64_t index, int32_t term, size_t bytes, uint32_t chunks,
                             uint32_t generation) {
    if (index <= 0 || term <= 0 || bytes == 0)
        throw std::runtime_error("invalid snapshot");
    if (index < _snapshot_index || (index == _snapshot_index && term != _snapshot_term))
        throw std::runtime_error("snapshot conflicts with the compacted prefix");
    const bool matches = (index == _snapshot_index && term == _snapshot_term) ||
        (index > _snapshot_index && index <= _last_index && GetTerm(index) == term);
    const bool keep_suffix = matches && index < _last_index;
    const int64_t delete_to = keep_suffix ? index : _last_index;
    rocksdb::WriteBatch batch;
    std::string meta(21, '\0');
    meta[0] = 2;
    auto encoded_index = static_cast<uint64_t>(index);
    for (int i = 8; i >= 1; --i) {
        meta[static_cast<size_t>(i)] = static_cast<char>(encoded_index & 0xff);
        encoded_index >>= 8;
    }
    auto encoded_term = static_cast<uint32_t>(term);
    for (int i = 0; i < 4; ++i)
        meta[9 + i] = static_cast<char>((encoded_term >> (8 * i)) & 0xff);
    auto encoded_bytes = static_cast<uint64_t>(bytes);
    for (int i = 20; i >= 13; --i) {
        meta[static_cast<size_t>(i)] = static_cast<char>(encoded_bytes & 0xff);
        encoded_bytes >>= 8;
    }
    batch.Put(SnapshotMetaKey(), meta);
    batch.Delete(SnapshotDataKey());
    std::string generation_bytes(4, '\0');
    auto encoded_generation = generation;
    for (int i = 3; i >= 0; --i) {
        generation_bytes[static_cast<size_t>(i)] = static_cast<char>(encoded_generation & 0xff);
        encoded_generation >>= 8;
    }
    batch.Put(SnapshotGenKey(), generation_bytes);
    if (delete_to > 0) {
        for (int64_t entry = 1;; ++entry) {
            batch.Delete(IndexToKey(entry));
            if (entry == delete_to) break;
        }
    }
    RequireStorageOK(_db->Write(DurableWriteOptions(), &batch), "persist Raft snapshot");
    if (generation != _snapshot_generation)
        DeleteChunks('c', _snapshot_generation, 0, _snapshot_chunks);
    if (!keep_suffix) {
        _last_index = index;
        _last_term = term;
    }
    _legacy_snapshot.clear();
    _snapshot_index = index;
    _snapshot_term = term;
    _snapshot_size = bytes;
    _snapshot_chunks = chunks;
    _snapshot_generation = generation;
}
void RaftLog::DiscardPartialChunks() {
    std::vector<std::string> drop;
    std::unique_ptr<rocksdb::Iterator> it(_db->NewIterator(rocksdb::ReadOptions()));
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        const std::string key = it->key().ToString();
        if (key.size() < 6 || key[0] != '\x01') continue;
        if (key.compare(1, 5, "snapr") == 0 || key.compare(1, 5, "snapn") == 0) {
            drop.push_back(key);
            continue;
        }
        if (key.size() == 14 && key.compare(1, 5, "snapc") == 0) {
            uint32_t generation = 0;
            for (int i = 6; i < 10; ++i)
                generation = (generation << 8) | static_cast<uint8_t>(key[static_cast<size_t>(i)]);
            const bool current = _legacy_snapshot.empty() && _snapshot_index > 0 &&
                                 generation == _snapshot_generation && generation != 0;
            if (!current) drop.push_back(key);
        }
    }
    RequireStorageOK(it->status(), "scan partial snapshot chunks");
    for (const auto& key : drop)
        RequireStorageOK(_db->Delete(DurableWriteOptions(), key), "drop partial snapshot chunk");
    _staging_size = 0;
    _staging_chunks = 0;
}
void RaftLog::PromoteStaging(int64_t index, int32_t term) {
    if (_staging_size == 0) throw std::runtime_error("empty snapshot staging");
    const uint32_t generation = NextGeneration();
    std::string chunk;
    for (uint32_t index_chunk = 0; index_chunk < _staging_chunks; ++index_chunk) {
        ReadChunk('r', 0, index_chunk, &chunk);
        WriteChunk('c', generation, index_chunk, chunk);
    }
    const uint32_t chunks = _staging_chunks;
    const size_t bytes = _staging_size;
    ClearStaging();
    FinishSnapshot(index, term, bytes, chunks, generation);
}
RaftLog::SnapshotWriter::SnapshotWriter(RaftLog* log)
    : _log(log), _generation(log->NextGeneration()) {}
RaftLog::SnapshotWriter::~SnapshotWriter() {
    if (!_committed) _log->DeleteChunks('c', _generation, 0, _chunks);
}
void RaftLog::SnapshotWriter::Write(const char* data, size_t n) {
    _total += n;
    size_t offset = 0;
    while (offset < n) {
        const size_t room = RaftLog::kStoreChunkBytes - _pending.size();
        const size_t take = std::min(room, n - offset);
        _pending.append(data + offset, take);
        offset += take;
        if (_pending.size() == RaftLog::kStoreChunkBytes) {
            _log->WriteChunk('c', _generation, _chunks, _pending);
            _pending.clear();
            ++_chunks;
        }
    }
}
void RaftLog::SnapshotWriter::Commit(int64_t index, int32_t term) {
    if (!_pending.empty()) {
        _log->WriteChunk('c', _generation, _chunks, _pending);
        _pending.clear();
        ++_chunks;
    }
    if (_total == 0) throw std::runtime_error("invalid snapshot");
    _committed = true;
    _log->FinishSnapshot(index, term, _total, _chunks, _generation);
}
