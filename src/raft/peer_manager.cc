#include "raft/peer_manager.h"

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
    _server.setConnectionCallback(
        [this](const auto& conn) { OnServerConnection(conn); });
    _server.setMessageCallback(
        [this](const auto& conn, auto* buf, auto ts) {
            OnServerMessage(conn, buf, ts);
        });
}

PeerManager::~PeerManager() = default;

void PeerManager::SetMessageHandler(MessageHandler handler) {
    _handler = std::move(handler);
}

void PeerManager::Start() {
    _server.start();
    LOG(INFO) << "PeerManager: listening for peers on port "
              << _server.ipPort();

    // 对 id 大于自己的节点发起连接
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
        LOG(INFO) << "Peer connected (inbound): " << conn->peerAddress().toIpPort();
    } else {
        // 连接断开：清理 _connections 中的对应条目
        for (auto it = _connections.begin(); it != _connections.end(); ) {
            if (it->second == conn) {
                LOG(INFO) << "Peer " << it->first << " disconnected (inbound)";
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
    auto client = std::make_unique<muduo::net::TcpClient>(_loop, addr, "RaftPeerClient");

    int peer_id = peer.id;
    client->setConnectionCallback(
        [this, peer_id](const auto& conn) {
            OnClientConnection(conn, peer_id);
        });
    client->setMessageCallback(
        [this, peer_id](const auto& conn, auto* buf, auto ts) {
            OnClientMessage(conn, buf, ts, peer_id);
        });

    client->enableRetry();  // muduo 自动重连
    client->connect();

    auto pc = std::make_unique<PeerClient>();
    pc->client = std::move(client);
    _clients[peer_id] = std::move(pc);

    LOG(INFO) << "Connecting to peer " << peer_id << " at "
              << peer.host << ":" << peer.raft_port;
}

void PeerManager::OnClientConnection(const muduo::net::TcpConnectionPtr& conn,
                                      int peer_id) {
    if (conn->connected()) {
        LOG(INFO) << "Connected to peer " << peer_id << " (outbound)";
        _connections[peer_id] = conn;
        auto it = _clients.find(peer_id);
        if (it != _clients.end()) {
            it->second->conn = conn;
        }
    } else {
        LOG(WARNING) << "Connection to peer " << peer_id << " lost (outbound)";
        _connections.erase(peer_id);
        auto it = _clients.find(peer_id);
        if (it != _clients.end()) {
            it->second->conn.reset();
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
                        const std::string& payload) {
    auto it = _connections.find(peer_id);
    if (it == _connections.end() || !it->second || !it->second->connected()) {
        return;  // 静默丢弃（心跳会重试）
    }

    std::string frame = RaftCodec::Encode(type, _self_id, payload);
    it->second->send(frame);
}

void PeerManager::Broadcast(RaftMsgType type, const std::string& payload) {
    for (const auto& peer : _all_peers) {
        if (peer.id == _self_id) continue;
        Send(peer.id, type, payload);
    }
}

// ================================================================
// 帧解析与分发
// ================================================================

void PeerManager::ParseAndDispatch(const muduo::net::TcpConnectionPtr& conn,
                                    muduo::net::Buffer* buf) {
    while (buf->readableBytes() >= 4) {
        // Peek 4-byte payload_len header (big-endian)
        uint32_t payload_len =
            (static_cast<uint8_t>(buf->peek()[0]) << 24) |
            (static_cast<uint8_t>(buf->peek()[1]) << 16) |
            (static_cast<uint8_t>(buf->peek()[2]) << 8)  |
            static_cast<uint8_t>(buf->peek()[3]);

        if (payload_len > RaftCodec::kMaxFrameSize) {
            LOG(ERROR) << "Frame too large (" << payload_len
                       << "), closing connection";
            conn->shutdown();
            return;
        }

        size_t total = 4 + static_cast<size_t>(payload_len);
        if (buf->readableBytes() < total) {
            return;  // 等待更多数据
        }

        std::string frame = buf->retrieveAsString(total);

        DecodedRaftMsg msg;
        size_t consumed = 0;
        if (!RaftCodec::TryDecode(frame.data(), frame.size(), &msg, &consumed)) {
            LOG(ERROR) << "Failed to decode frame, skipping";
            continue;
        }

        int actual_sender = msg.sender_id;

        // 通过 sender_id 注册/更新对端连接映射
        RegisterPeerConnection(actual_sender, conn);

        // 分发到上层
        if (_handler) {
            _handler(actual_sender, msg.type, msg.payload);
        }
    }
}

void PeerManager::RegisterPeerConnection(
    int peer_id, const muduo::net::TcpConnectionPtr& conn) {
    // 检查是否已有这个 peer 的连接
    auto it = _connections.find(peer_id);
    if (it != _connections.end() && it->second != conn) {
        // 对端重连了（新连接），更新映射
        LOG(INFO) << "Peer " << peer_id << " reconnected, updating connection";
        it->second = conn;
    } else if (it == _connections.end()) {
        LOG(INFO) << "Peer " << peer_id
                  << " identified via frame (inbound connection)";
        _connections[peer_id] = conn;
    }
    // else: 同一连接，无需更新
}
