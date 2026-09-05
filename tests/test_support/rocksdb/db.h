#pragma once
// TEST DOUBLE: per-path in-process map with atomic copy-and-swap writes and
// injected I/O failures. This models the storage contract, not RocksDB's WAL,
// fsync, file locks, compaction, power loss, or actual filesystem persistence.
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace rocksdb {
class Status {
public:
    Status() = default;
    static Status NotFound() { return Status(1, "NotFound"); }
    static Status IOError() { return Status(2, "injected I/O failure"); }
    bool ok() const { return code_ == 0; }
    bool IsNotFound() const { return code_ == 1; }
    std::string ToString() const { return text_; }
private:
    Status(int code, std::string text) : code_(code), text_(std::move(text)) {}
    int code_ = 0;
    std::string text_ = "OK";
};
struct Options { bool create_if_missing = false; };
struct ReadOptions {};
struct WriteOptions { bool sync = false; };
class Slice {
public:
    explicit Slice(std::string value) : value_(std::move(value)) {}
    std::string ToString() const { return value_; }
private:
    std::string value_;
};
class WriteBatch {
public:
    struct Operation { bool erase; std::string key; std::string value; };
    void Put(const std::string& key, const std::string& value) {
        operations.push_back({false, key, value});
    }
    void Delete(const std::string& key) { operations.push_back({true, key, {}}); }
    std::vector<Operation> operations;
};
namespace testing {
struct State {
    std::map<std::string, std::string> data;
    int fail_reads = 0;
    int fail_writes = 0;
    int fail_opens = 0;
    int fail_iterators = 0;
    size_t successful_writes = 0;
    size_t non_sync_writes = 0;
};
inline std::map<std::string, std::shared_ptr<State>> paths;
inline std::shared_ptr<State> StateFor(const std::string& path) {
    auto& state = paths[path];
    if (!state) state = std::make_shared<State>();
    return state;
}
inline bool Consume(int& count) { if (count <= 0) return false; --count; return true; }
}
class Iterator {
public:
    Iterator(std::map<std::string, std::string> data, Status status)
        : data_(std::move(data)), status_(std::move(status)), cursor_(data_.end()) {}
    void SeekToFirst() { cursor_ = data_.begin(); }
    bool Valid() const { return status_.ok() && cursor_ != data_.end(); }
    void Next() { ++cursor_; }
    Slice key() const { return Slice(cursor_->first); }
    Slice value() const { return Slice(cursor_->second); }
    Status status() const { return status_; }
private:
    std::map<std::string, std::string> data_;
    Status status_;
    std::map<std::string, std::string>::const_iterator cursor_;
};
class DB {
public:
    static Status Open(const Options&, const std::string& path, DB** out) {
        *out = nullptr;
        auto state = testing::StateFor(path);
        if (testing::Consume(state->fail_opens)) return Status::IOError();
        *out = new DB(std::move(state));
        return {};
    }
    Status Get(const ReadOptions&, const std::string& key, std::string* out) {
        if (testing::Consume(state_->fail_reads)) return Status::IOError();
        const auto found = state_->data.find(key);
        if (found == state_->data.end()) return Status::NotFound();
        *out = found->second;
        return {};
    }
    Status Put(const WriteOptions& options, const std::string& key, const std::string& value) {
        WriteBatch batch;
        batch.Put(key, value);
        return Write(options, &batch);
    }
    Status Write(const WriteOptions& options, WriteBatch* batch) {
        if (testing::Consume(state_->fail_writes)) return Status::IOError();
        auto staged = state_->data;
        for (const auto& op : batch->operations) {
            if (op.erase) staged.erase(op.key);
            else staged[op.key] = op.value;
        }
        state_->data.swap(staged);
        ++state_->successful_writes;
        if (!options.sync) ++state_->non_sync_writes;
        return {};
    }
    Iterator* NewIterator(const ReadOptions&) {
        const auto status = testing::Consume(state_->fail_iterators) ? Status::IOError() : Status();
        return new Iterator(state_->data, status);
    }
private:
    explicit DB(std::shared_ptr<testing::State> state) : state_(std::move(state)) {}
    std::shared_ptr<testing::State> state_;
};
}
