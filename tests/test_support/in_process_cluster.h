#pragma once
// In-process cluster around the production RaftNode, KV, and log sources.
// Transport and RocksDB are test doubles; this still fails if RaftNode is wrong.
#include "raft/raft_node.h"
#include "raft/apply_executor.h"
#include <cstdint>
#include <deque>
#include <initializer_list>
#include <map>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

inline void Check(bool ok, const char* message) {
    if (!ok) throw std::runtime_error(message);
}

inline uint64_t Metric(const RaftNode& node, const std::string& name) {
    const auto info = node.MetricsInfo();
    const auto start = info.find(name + ":");
    Check(start != std::string::npos && (start == 0 || info[start - 1] == '\n'),
          "missing metric field");
    return std::stoull(info.substr(start + name.size() + 1));
}

template<class F>
inline void Throws(F call) {
    bool threw = false;
    try { call(); } catch (const std::runtime_error&) { threw = true; }
    Check(threw, "expected storage/consistency failure");
}

inline std::string Command(std::initializer_list<std::string> args) {
    std::string out = "*" + std::to_string(args.size()) + "\r\n";
    for (const auto& arg : args)
        out += "$" + std::to_string(arg.size()) + "\r\n" + arg + "\r\n";
    return out;
}

inline raftcore::LogEntry Entry(int64_t index, int term, std::string command = {}) {
    raftcore::LogEntry entry;
    entry.set_index(index); entry.set_term(term); entry.set_command(command);
    return entry;
}

struct Message { int from, to; RaftMsgType type; std::string payload; };
struct Member {
    std::unique_ptr<KVStateMachine> sm;
    std::unique_ptr<PeerManager> transport;
    std::unique_ptr<RaftNode> raft;
};

class ManualApplyExecutor : public ApplyExecutor {
public:
    bool Submit(Work work, Completion completion) override {
        Check(!busy, "more than one apply batch submitted before completion");
        busy = true;
        worked = false;
        ++submissions;
        work_ = std::move(work);
        completion_ = std::move(completion);
        return true;
    }
    void RunWork() {
        Check(busy && !worked, "invalid manual apply work transition");
        worked = true;
        try { results_ = work_(); } catch (...) { error_ = std::current_exception(); }
        work_ = {};
    }
    void Complete() {
        Check(busy && worked, "completion arrived before apply work");
        auto completion = std::move(completion_);
        auto results = std::move(results_);
        auto error = std::move(error_);
        error_ = nullptr;
        busy = false;
        worked = false;
        completion(std::move(results), error);
    }
    void Finish() { RunWork(); Complete(); }
    bool busy = false;
    bool worked = false;
    int submissions = 0;
private:
    Work work_;
    Completion completion_;
    Results results_;
    std::exception_ptr error_;
};

class Cluster {
public:
    explicit Cluster(std::vector<int> ids = {10, 30, 50}, ApplyExecutor* executor = nullptr) {
        static int sequence = 0;
        prefix = "cluster-" + std::to_string(++sequence) + "/";
        if (executor) executors[ids.front()] = executor;
        for (int id : ids) peers.push_back({id, "127.0.0.1", 9000 + id});
        for (int id : ids) Restart(id);
    }
    std::string Path(int id, const char* suffix) const {
        return prefix + std::to_string(id) + suffix;
    }
    void Restart(int id) {
        members.erase(id);
        auto member = std::make_unique<Member>();
        member->sm = std::make_unique<KVStateMachine>(Path(id, "/kv"));
        member->transport = std::make_unique<PeerManager>(id, peers,
            [this](int from, int to, RaftMsgType type, const std::string& payload) {
                messages.push_back({from, to, type, payload});
            });
        member->raft = std::make_unique<RaftNode>(id, peers, nullptr, Path(id, "/log"),
            member->sm.get(), member->transport.get(),
            executors.count(id) ? executors.at(id) : nullptr);
        member->raft->Start();
        members.emplace(id, std::move(member));
    }
    RaftNode& Node(int id) { return *members.at(id)->raft; }
    KVStateMachine& State(int id) { return *members.at(id)->sm; }
    void Candidate(int id) {
        for (int ticks = 0; ticks < 31 && std::string(Node(id).StateName()) == "follower"; ++ticks)
            Node(id).Tick();
        Check(std::string(Node(id).StateName()) == "candidate" || Node(id).IsLeader(),
              "election did not start within bounded ticks");
    }
    bool Dropped(const Message& message) const {
        return partitioned.count(message.from) || partitioned.count(message.to);
    }
    void Partition(int id) { partitioned.insert(id); }
    void Heal(int id) { partitioned.erase(id); }
    void Deliver(const Message& message) {
        if (!members.count(message.to) || Dropped(message)) return;
        auto& node = Node(message.to);
        switch (message.type) {
        case RaftMsgType::kRequestVote: {
            raftcore::RequestVote rpc; Check(rpc.ParseFromString(message.payload), "vote decode");
            node.HandleRequestVote(message.from, rpc); break;
        }
        case RaftMsgType::kRequestVoteResponse: {
            raftcore::RequestVoteResponse rpc; Check(rpc.ParseFromString(message.payload), "vote reply decode");
            node.HandleRequestVoteResponse(message.from, rpc); break;
        }
        case RaftMsgType::kAppendEntries: {
            raftcore::AppendEntries rpc; Check(rpc.ParseFromString(message.payload), "append decode");
            node.HandleAppendEntries(message.from, rpc); break;
        }
        case RaftMsgType::kAppendEntriesResponse: {
            raftcore::AppendEntriesResponse rpc; Check(rpc.ParseFromString(message.payload), "append reply decode");
            node.HandleAppendEntriesResponse(message.from, rpc); break;
        }
        }
    }
    void Pump() {
        int remaining = 10000;
        while (!messages.empty()) {
            Check(--remaining > 0, "message processing did not settle");
            auto message = messages.front(); messages.pop_front();
            if (Dropped(message)) continue;
            Deliver(message);
        }
    }
    void Settle() {
        for (int tick = 0; tick < 10; ++tick) {
            for (auto& member : members) member.second->raft->Tick();
            Pump();
        }
    }
    void Elect(int id) { Candidate(id); Pump(); Settle(); Check(Node(id).IsLeader(), "leader election failed"); }
    // Tick the given followers/candidates until one of them is leader. Use this
    // after isolating a live leader: followers ignore RequestVote until their
    // election timer expires, so Elect(id) would stall.
    int ElectAmong(std::initializer_list<int> ids) {
        for (int round = 0; round < 80; ++round) {
            for (int id : ids) {
                if (!members.count(id) || Node(id).IsLeader()) continue;
                Node(id).Tick();
            }
            Pump();
            for (int id : ids) {
                if (members.count(id) && Node(id).IsLeader()) {
                    Settle();
                    return id;
                }
            }
        }
        Check(false, "majority did not elect a leader");
        return -1;
    }
    std::vector<PeerInfo> peers;
    std::map<int, std::unique_ptr<Member>> members;
    std::map<int, ApplyExecutor*> executors;
    std::deque<Message> messages;
    std::set<int> partitioned;
    std::string prefix;
};

inline raftcore::AppendEntries Append(int term, uint64_t rpc, int64_t prev, int prev_term, int64_t commit) {
    raftcore::AppendEntries request;
    request.set_term(term); request.set_leader_id(30); request.set_rpc_id(rpc);
    request.set_prev_log_index(prev); request.set_prev_log_term(prev_term); request.set_leader_commit(commit);
    return request;
}

inline raftcore::AppendEntriesResponse LastResponse(Cluster& cluster) {
    Check(!cluster.messages.empty(), "missing follower response");
    raftcore::AppendEntriesResponse response;
    Check(cluster.messages.back().type == RaftMsgType::kAppendEntriesResponse &&
          response.ParseFromString(cluster.messages.back().payload), "invalid follower response");
    cluster.messages.clear(); return response;
}

inline raftcore::RequestVoteResponse LastVoteResponse(Cluster& cluster) {
    Check(!cluster.messages.empty(), "missing vote response");
    raftcore::RequestVoteResponse response;
    Check(cluster.messages.back().type == RaftMsgType::kRequestVoteResponse &&
          response.ParseFromString(cluster.messages.back().payload), "invalid vote response");
    cluster.messages.clear();
    return response;
}

inline std::vector<Message> TakeAppendsFrom(Cluster& cluster, int leader) {
    std::vector<Message> appends;
    std::deque<Message> rest;
    for (auto& message : cluster.messages) {
        if (message.type == RaftMsgType::kAppendEntries && message.from == leader)
            appends.push_back(message);
        else
            rest.push_back(std::move(message));
    }
    cluster.messages = std::move(rest);
    return appends;
}

inline Message DeliverAppendAndTakeAck(Cluster& cluster, const Message& append) {
    cluster.Deliver(append);
    Check(!cluster.messages.empty() &&
          cluster.messages.back().type == RaftMsgType::kAppendEntriesResponse,
          "follower did not answer append");
    auto ack = cluster.messages.back();
    cluster.messages.pop_back();
    return ack;
}

inline uint64_t AppendRpcId(const Message& message) {
    raftcore::AppendEntries rpc;
    Check(rpc.ParseFromString(message.payload), "append decode");
    return rpc.rpc_id();
}
