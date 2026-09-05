#pragma once
// TEST DOUBLE: snapshots live only in this process. Tokens are NOT protobuf wire
// data and cannot prove wire compatibility, generated-code integration, or
// cross-process disk recovery. Production message field accessors are mirrored.
#include <cstdint>
#include <map>
#include <string>
#include <vector>

namespace test_proto {
template<class T> class Snapshot {
public:
    bool SerializeToString(std::string* out) const {
        *out = "test-snapshot:" + std::to_string(++sequence_);
        snapshots_[*out] = static_cast<const T&>(*this);
        return true;
    }
    bool ParseFromString(const std::string& token) {
        const auto found = snapshots_.find(token);
        if (found == snapshots_.end()) return false;
        static_cast<T&>(*this) = found->second;
        return true;
    }
private:
    inline static uint64_t sequence_ = 0;
    inline static std::map<std::string, T> snapshots_;
};
}

#define TEST_PROTO_FIELD(Type, Name) \
private: Type Name##_{}; \
public: Type Name() const { return Name##_; } \
    void set_##Name(Type value) { Name##_ = value; }

namespace raftcore {
class LogEntry : public test_proto::Snapshot<LogEntry> {
    TEST_PROTO_FIELD(int64_t, index)
    TEST_PROTO_FIELD(int32_t, term)
public:
    const std::string& command() const { return command_; }
    void set_command(const std::string& value) { command_ = value; }
    // Deliberately conservative estimate, not protobuf's exact encoded length.
    size_t ByteSizeLong() const { return command_.size() + 32; }
private:
    std::string command_;
};
class RequestVote : public test_proto::Snapshot<RequestVote> {
    TEST_PROTO_FIELD(int32_t, term)
    TEST_PROTO_FIELD(int32_t, candidate_id)
    TEST_PROTO_FIELD(int64_t, last_log_index)
    TEST_PROTO_FIELD(int64_t, last_log_term)
};
class RequestVoteResponse : public test_proto::Snapshot<RequestVoteResponse> {
    TEST_PROTO_FIELD(int32_t, term)
    TEST_PROTO_FIELD(bool, vote_granted)
};
class AppendEntries : public test_proto::Snapshot<AppendEntries> {
    TEST_PROTO_FIELD(int32_t, term)
    TEST_PROTO_FIELD(int32_t, leader_id)
    TEST_PROTO_FIELD(int64_t, prev_log_index)
    TEST_PROTO_FIELD(int64_t, prev_log_term)
    TEST_PROTO_FIELD(int64_t, leader_commit)
    TEST_PROTO_FIELD(uint64_t, rpc_id)
public:
    int entries_size() const { return static_cast<int>(entries_.size()); }
    const LogEntry& entries(int index) const { return entries_.at(static_cast<size_t>(index)); }
    LogEntry* add_entries() { entries_.emplace_back(); return &entries_.back(); }
private:
    std::vector<LogEntry> entries_;
};
class AppendEntriesResponse : public test_proto::Snapshot<AppendEntriesResponse> {
    TEST_PROTO_FIELD(int32_t, term)
    TEST_PROTO_FIELD(bool, success)
    TEST_PROTO_FIELD(int64_t, last_log_index)
    TEST_PROTO_FIELD(uint64_t, rpc_id)
};
}
#undef TEST_PROTO_FIELD
