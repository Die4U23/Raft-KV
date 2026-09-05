#pragma once
#include <muduo/net/EventLoop.h>
#include <muduo/net/TcpServer.h>
#include <muduo/net/TcpClient.h>
#include <muduo/net/TcpConnection.h>
#include <muduo/net/InetAddress.h>
#include <muduo/net/Buffer.h>
#include <glog/logging.h>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "raft/raft_codec.h"
#include "raft/peers.h"

// PeerManager: 管理与其他 Raft 节点之间的 TCP 连接
//
// 连接策略:
//   - 每个节点启动一个 TcpServer 监听在 raft_port 上
//   - 对 node_id 大于自己的节点主动发起 TcpClient 连接
//
// 重连策略:
//   - 使用 muduo enableRetry()，由 Connector 管理连接状态和退避。
//   - 不额外定时调用 connect()，避免与正在进行的连接/重试冲突。
//
// 对端识别:
//   - 通过帧头中的 sender_id 字段识别对端身份
//
// 所有操作都在 muduo EventLoop 线程中进行，无需加锁
class PeerManager {
public:
    using MessageHandler = std::function<void(int from_peer_id,
                                              RaftMsgType type,
                                              const std::string& payload)>;

    PeerManager(muduo::net::EventLoop* loop,
                int self_id,
                int listen_port,
                const std::vector<PeerInfo>& all_peers);

    ~PeerManager();

    void SetMessageHandler(MessageHandler handler);
    void Start();

    // 发送 Raft RPC 到指定节点
    void Send(int peer_id, RaftMsgType type, const std::string& payload);

    // 广播到所有其他节点
    void Broadcast(RaftMsgType type, const std::string& payload);

    int ClusterSize() const { return static_cast<int>(_all_peers.size()); }
    int SelfId() const { return _self_id; }

private:
    // 服务端：接受其他节点的连接
    void OnServerConnection(const muduo::net::TcpConnectionPtr& conn);
    void OnServerMessage(const muduo::net::TcpConnectionPtr& conn,
                         muduo::net::Buffer* buf, muduo::Timestamp);

    // 客户端：向其他节点发起连接
    void ConnectToPeer(const PeerInfo& peer);
    void OnClientConnection(const muduo::net::TcpConnectionPtr& conn,
                            int peer_id);
    void OnClientMessage(const muduo::net::TcpConnectionPtr& conn,
                         muduo::net::Buffer* buf, muduo::Timestamp,
                         int peer_id);

    // 帧解析（入站消息处理）
    void ParseAndDispatch(const muduo::net::TcpConnectionPtr& conn,
                          muduo::net::Buffer* buf);

    // 注册/更新 peer 连接
    bool RegisterPeerConnection(int peer_id,
                                const muduo::net::TcpConnectionPtr& conn);

    muduo::net::EventLoop* _loop;
    int _self_id;
    std::vector<PeerInfo> _all_peers;

    muduo::net::TcpServer _server;

    // 仅对 id > _self_id 的 peer 创建
    std::map<int, std::unique_ptr<muduo::net::TcpClient>> _clients;

    // peer_id → TcpConnectionPtr（所有已建立的连接）
    std::map<int, muduo::net::TcpConnectionPtr> _connections;
    std::map<std::string, int> _connection_peers;

    MessageHandler _handler;

};
