#pragma once
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

struct PeerInfo {
    int id;
    std::string host;
    int raft_port;
};
inline void ValidatePeers(int self_id, const std::vector<PeerInfo>& peers) {
    std::set<int> ids;
    for (const auto& peer : peers) {
        if (peer.id < 0 || peer.host.empty() || peer.raft_port < 1 ||
            peer.raft_port > 65535 || !ids.insert(peer.id).second)
            throw std::invalid_argument("invalid or duplicate Raft peer");
    }
    if (!ids.count(self_id)) throw std::invalid_argument("self is not a Raft peer");
}
