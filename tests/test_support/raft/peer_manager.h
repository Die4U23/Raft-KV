#pragma once
// TEST DOUBLE: deterministic queued delivery; no sockets, Muduo, reconnects, or
// thread scheduling. The real RaftNode is compiled against this narrow surface.
#include "raft/raft_codec.h"
#include "raft/peers.h"
#include <functional>
#include <utility>

namespace muduo { namespace net { class EventLoop {}; } }

class PeerManager {
public:
    using Sink = std::function<void(int, int, RaftMsgType, const std::string&)>;
    PeerManager(int self, std::vector<PeerInfo> peers, Sink sink)
        : self_(self), peers_(std::move(peers)), sink_(std::move(sink)) {}
    void Send(int peer, RaftMsgType type, const std::string& payload, int shard = 0) {
        (void)shard;
        sink_(self_, peer, type, payload);
    }
    void Broadcast(RaftMsgType type, const std::string& payload, int shard = 0) {
        for (const auto& peer : peers_) if (peer.id != self_) Send(peer.id, type, payload, shard);
    }
    void LearnPeer(const PeerInfo& peer) {
        for (const auto& existing : peers_) if (existing.id == peer.id) return;
        peers_.push_back(peer);
    }
private:
    int self_;
    std::vector<PeerInfo> peers_;
    Sink sink_;
};
