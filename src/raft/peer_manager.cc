#include "raft/peer_manager.h"
#include "common/frame_mac.h"

PeerManager::PeerManager(muduo::net::EventLoop* loop,
                         int self_id,
                         int listen_port,
                         const std::vector<PeerInfo>& all_peers)
    : _loop(loop)
    , _self_id(self_id)
    , _all_peers(all_peers)
    , _server(loop,
              muduo::net::InetAddress(static_cast<uint16_t>(listen_port)),
              "RaftPeerServer")
{
    ValidatePeers(self_id, all_peers);
    std::weak_ptr<int> alive = _lifetime;
    _server.setConnectionCallback(
        [this, alive](const auto& conn) { if (!alive.expired()) OnServerConnection(conn); });
    _server.setMessageCallback(
        [this, alive](const auto& conn, auto* buf, auto ts) {
            if (!alive.expired()) OnServerMessage(conn, buf, ts);
        });
}

PeerManager::~PeerManager() {
    _lifetime.reset();
}

void PeerManager::SetMessageHandler(MessageHandler handler) {
    _handler = std::move(handler);
}

void PeerManager::Start() {
    _loop->assertInLoopThread();
    if (_started) return;
    _started = true;
    _server.start();
    LOG(INFO) << "PeerManager[" << _self_id
              << "]: listening for peers on 0.0.0.0:" << _server.ipPort();

    // 首次连接：向所有 id > self_id 的节点发起连接
    for (const auto& peer : _all_peers) {
        if (peer.id == _self_id) continue;
        if (peer.id > _self_id) {
            ConnectToPeer(peer);
        }
    }

}

// ================================================================
// 服务端（接受来自其他节点的连接）
// ================================================================

void PeerManager::OnServerConnection(const muduo::net::TcpConnectionPtr& conn) {
    if (conn->connected()) {
        // 接受所有连接，对端身份在收到第一条消息时通过 sender_id 识别
    } else {
        _connection_peers.erase(conn->name());
        // 连接断开：清理 _connections 中的对应条目
        for (auto it = _connections.begin(); it != _connections.end(); ) {
            if (it->second == conn) {
                LOG(INFO) << "PeerManager[" << _self_id
                          << "]: peer " << it->first << " disconnected";
                it = _connections.erase(it);
            } else {
                ++it;
            }
        }
    }
}

void PeerManager::OnServerMessage(const muduo::net::TcpConnectionPtr& conn,
                                   muduo::net::Buffer* buf, muduo::Timestamp) {
    ParseAndDispatch(conn, buf);
}

// ================================================================
// 客户端（向 id > self_id 的节点主动连接）
// ================================================================

void PeerManager::ConnectToPeer(const PeerInfo& peer) {
    auto addr = muduo::net::InetAddress(peer.host,
                                         static_cast<uint16_t>(peer.raft_port));
    auto client = std::make_unique<muduo::net::TcpClient>(
        _loop, addr, "RaftPeerClient-" + std::to_string(peer.id) +
                    "-" + std::to_string(++_client_generation));

    int peer_id = peer.id;
    std::weak_ptr<int> alive = _lifetime;
    client->setConnectionCallback(
        [this, peer_id, alive](const auto& conn) {
            if (!alive.expired()) OnClientConnection(conn, peer_id);
        });
    client->setMessageCallback(
        [this, peer_id, alive](const auto& conn, auto* buf, auto ts) {
            if (!alive.expired()) OnClientMessage(conn, buf, ts, peer_id);
        });

    // Do not enable TcpClient's immediate restart after a successful connection
    // closes. Connector still backs off failed connects on this fresh client.
    // Replace only from a delayed callback, after old removeConnection returned;
    // calling connect() again on its kConnected Connector would assert in Muduo.
    _clients[peer_id] = std::move(client);
    _clients.at(peer_id)->connect();
}

void PeerManager::OnClientConnection(const muduo::net::TcpConnectionPtr& conn,
                                      int peer_id) {
    if (conn->connected()) {
        _retry[peer_id].Connected(PeerRetryPolicy::Clock::now());
        LOG(INFO) << "PeerManager[" << _self_id
                  << "]: connected to peer " << peer_id;
        _connections[peer_id] = conn;
        _connection_peers[conn->name()] = peer_id;
    } else {
        _connection_peers.erase(conn->name());
        const auto current = _connections.find(peer_id);
        if (current != _connections.end() && current->second == conn) {
            _connections.erase(current);
            const auto plan = _retry[peer_id].Disconnected(PeerRetryPolicy::Clock::now());
            if (!plan.token) return;
            for (const auto& peer : _all_peers) {
                if (peer.id != peer_id) continue;
                std::weak_ptr<int> alive = _lifetime;
                _loop->runAfter(plan.delay_ms / 1000.0, [this, alive, peer, token = plan.token]() {
                    if (alive.expired()) return;
                    if (_retry.at(peer.id).Consume(token)) ConnectToPeer(peer);
                });
                break;
            }
        }
    }
}

void PeerManager::OnClientMessage(const muduo::net::TcpConnectionPtr& conn,
                                   muduo::net::Buffer* buf, muduo::Timestamp,
                                   int /*peer_id_expected*/) {
    ParseAndDispatch(conn, buf);
}

// ================================================================
// 发送
// ================================================================

void PeerManager::Send(int peer_id, RaftMsgType type,
                        const std::string& payload, int shard) {
    auto it = _connections.find(peer_id);
    if (it == _connections.end() || !it->second || !it->second->connected()) {
        return;  // 静默丢弃（心跳会重试）
    }
    if (_shard_count > 1 && (shard < 0 || shard >= _shard_count)) return;

    std::string body;
    try {
        body = PrefixShardPayload(_shard_count, shard, payload);
    } catch (const std::invalid_argument&) {
        return;
    }
    std::string frame = SealFrame(_cluster_token, RaftCodec::Encode(type, _self_id, body));
    // A paused follower must not accumulate an unlimited number of RPC retries.
    // Defer whole frames; the Raft retry timer will try again after TCP drains.
    constexpr size_t max_queued = 4 * 1024 * 1024;
    const size_t queued = it->second->outputBuffer()->readableBytes();
    if (queued > max_queued || frame.size() > max_queued - queued) return;
    it->second->send(frame);
}

void PeerManager::Broadcast(RaftMsgType type, const std::string& payload, int shard) {
    for (const auto& peer : _all_peers) {
        if (peer.id == _self_id) continue;
        Send(peer.id, type, payload, shard);
    }
}

// ================================================================
// 帧解析与分发
// ================================================================

void PeerManager::ParseAndDispatch(const muduo::net::TcpConnectionPtr& conn,
                                    muduo::net::Buffer* buf) {
    while (buf->readableBytes() >= 4) {
        std::string frame;
        size_t consumed = 0;
        const auto sealed = UnsealFrame(_cluster_token, buf->peek(), buf->readableBytes(),
                                        &frame, &consumed);
        if (sealed == FrameSealStatus::NeedMore) return;
        if (sealed == FrameSealStatus::Reject) {
            LOG(ERROR) << "rejected Raft frame";
            conn->forceClose();
            return;
        }
        DecodedRaftMsg message;
        size_t used = 0;
        try {
            if (!RaftCodec::TryDecode(frame.data(), frame.size(), &message, &used) ||
                used != frame.size()) {
                LOG(ERROR) << "invalid Raft frame";
                conn->forceClose();
                return;
            }
        } catch (const std::invalid_argument& error) {
            LOG(ERROR) << error.what();
            conn->forceClose();
            return;
        }
        int shard = 0;
        std::string payload;
        if (!StripShardPayload(_shard_count, message.payload, &shard, &payload)) {
            LOG(ERROR) << "invalid Raft shard";
            conn->forceClose();
            return;
        }
        if (!RegisterPeerConnection(message.sender_id, conn)) {
            conn->forceClose();
            return;
        }
        buf->retrieve(consumed);
        // Storage/consensus exceptions must reach the process boundary.
        if (_handler) _handler(message.sender_id, message.type, payload, shard);
    }
}

bool PeerManager::RegisterPeerConnection(
    int peer_id, const muduo::net::TcpConnectionPtr& conn) {
    bool known = false;
    for (const auto& peer : _all_peers)
        if (peer.id == peer_id && peer_id != _self_id) known = true;
    if (!known) return false;
    const auto bound = _connection_peers.find(conn->name());
    if (bound != _connection_peers.end() && bound->second != peer_id) return false;
    // Only smaller IDs initiate incoming connections. Outgoing ones are bound
    // to their configured peer when the connection is established.
    if (bound == _connection_peers.end() && peer_id > _self_id) return false;
    _connection_peers[conn->name()] = peer_id;
    _connections[peer_id] = conn;
    return true;
}
