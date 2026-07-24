#include <muduo/net/EventLoop.h>
#include <muduo/net/TcpServer.h>
#include <muduo/net/TcpConnection.h>
#include <muduo/net/InetAddress.h>
#include <muduo/net/Buffer.h>
#include <muduo/base/Logging.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <functional>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "common/resp_parser.h"
#include "namespace/namespace_manager.h"
#include "raft/kv_state_machine.h"
#include "raft/peer_manager.h"
#include "raft/raft_node.h"
#include "raft/raft_codec.h"
#include "raft_messages.pb.h"

// ---- gflags 配置 ----
DEFINE_int32(node_id, 0, "Raft node ID (0, 1, 2, ...)");
DEFINE_int32(client_port, 8080, "Client-facing TCP port (RESP protocol)");
DEFINE_int32(raft_port, 9080, "Raft peer RPC port");
DEFINE_string(db_path, "/tmp/kv_db", "RocksDB data path for state machine");
DEFINE_string(raft_log_path, "/tmp/raft_log", "RocksDB path for Raft log");
DEFINE_string(peers, "0:127.0.0.1:9080,1:127.0.0.1:9081,2:127.0.0.1:9082",
              "Cluster peers: id:host:raft_port,...");
DEFINE_bool(leader_only_reads, false, "Only serve reads from leader");

// ---- 全局组件 ----
static std::unique_ptr<KVStateMachine>   g_sm;
static std::unique_ptr<PeerManager>       g_peer_mgr;
static std::unique_ptr<RaftNode>          g_raft_node;
static NamespaceManager                   g_ns_mgr;
static muduo::net::EventLoop*             g_loop = nullptr;

// ---- RESP 格式化辅助函数 ----
static std::string respBulkNull() { return "$-1\r\n"; }
static std::string respError(const std::string& err) { return "-ERR " + err + "\r\n"; }
static std::string respBulkString(const std::string& data) {
    return "$" + std::to_string(data.size()) + "\r\n" + data + "\r\n";
}
static std::string respInteger(int64_t n) { return ":" + std::to_string(n) + "\r\n"; }
static std::string respSimpleString(const std::string& s) { return "+" + s + "\r\n"; }

// ---- 命令序列化（用于存入 Raft 日志） ----
static std::string SerializeCommand(const std::vector<std::string>& parts) {
    std::string cmd = "*" + std::to_string(parts.size()) + "\r\n";
    for (const auto& p : parts) {
        cmd += "$" + std::to_string(p.size()) + "\r\n" + p + "\r\n";
    }
    return cmd;
}

// ---- Peer 列表解析 ----
static std::vector<PeerInfo> ParsePeers(const std::string& peers_str) {
    std::vector<PeerInfo> result;
    std::istringstream ss(peers_str);
    std::string token;
    while (std::getline(ss, token, ',')) {
        if (token.empty()) continue;
        // 格式: id:host:port
        size_t c1 = token.find(':');
        size_t c2 = token.rfind(':');
        if (c1 == std::string::npos || c2 == std::string::npos || c1 == c2) {
            LOG(FATAL) << "Invalid peer format: " << token
                       << " (expected id:host:port)";
        }
        PeerInfo p;
        p.id = std::stoi(token.substr(0, c1));
        p.host = token.substr(c1 + 1, c2 - c1 - 1);
        p.raft_port = std::stoi(token.substr(c2 + 1));
        result.push_back(p);
    }
    return result;
}

// ---- Peer 消息分发 → RaftNode ----
static void OnRaftMessage(int from_peer, RaftMsgType type,
                           const std::string& payload) {
    switch (type) {
    case RaftMsgType::kRequestVote: {
        raftcore::RequestVote req;
        if (req.ParseFromString(payload)) {
            g_raft_node->HandleRequestVote(from_peer, req);
        }
        break;
    }
    case RaftMsgType::kRequestVoteResponse: {
        raftcore::RequestVoteResponse resp;
        if (resp.ParseFromString(payload)) {
            g_raft_node->HandleRequestVoteResponse(from_peer, resp);
        }
        break;
    }
    case RaftMsgType::kAppendEntries: {
        raftcore::AppendEntries req;
        if (req.ParseFromString(payload)) {
            g_raft_node->HandleAppendEntries(from_peer, req);
        }
        break;
    }
    case RaftMsgType::kAppendEntriesResponse: {
        raftcore::AppendEntriesResponse resp;
        if (resp.ParseFromString(payload)) {
            g_raft_node->HandleAppendEntriesResponse(from_peer, resp);
        }
        break;
    }
    }
}

// ---- 客户端连接回调 ----
static void OnClientConnection(const muduo::net::TcpConnectionPtr& conn) {
    if (conn->connected()) {
        LOG(INFO) << "Client connected: " << conn->peerAddress().toIpPort();
    } else {
        LOG(INFO) << "Client disconnected: " << conn->peerAddress().toIpPort();
        g_ns_mgr.Remove(conn->name());
    }
}

// ---- 客户端命令处理 ----
static void OnClientMessage(const muduo::net::TcpConnectionPtr& conn,
                             muduo::net::Buffer* buf, muduo::Timestamp) {
    std::string msg = buf->retrieveAllAsString();
    auto commands = RespParser::Parse(msg);

    for (const auto& parts : commands) {
        if (parts.empty()) continue;

        std::string op = parts[0];
        for (auto& c : op) c = toupper(c);

        if (op == "PING") {
            conn->send("+PONG\r\n");

        } else if (op == "SELECT") {
            // 切换命名空间（本地操作，不经过 Raft）
            if (parts.size() != 2) {
                conn->send(respError("wrong number of arguments for SELECT"));
                continue;
            }
            const std::string& ns = parts[1];
            if (!NamespaceManager::IsValidName(ns)) {
                conn->send(respError("invalid namespace name: " + ns));
                continue;
            }
            g_ns_mgr.SetNs(conn->name(), ns);
            conn->send("+OK\r\n");

        } else if (op == "GET") {
            if (parts.size() != 2) {
                conn->send(respError("wrong number of arguments for GET"));
                continue;
            }
            if (FLAGS_leader_only_reads && !g_raft_node->IsLeader()) {
                int leader = g_raft_node->GetLeaderId();
                conn->send(respError("MOVED " + std::to_string(leader)));
                continue;
            }
            // 加上 namespace 前缀读
            std::string ns_key = g_ns_mgr.MakeKey(conn->name(), parts[1]);
            std::string value;
            if (g_sm->Get(ns_key, &value)) {
                conn->send(respBulkString(value));
            } else {
                conn->send(respBulkNull());
            }

        } else if (op == "SET") {
            if (parts.size() != 3) {
                conn->send(respError("wrong number of arguments for SET"));
                continue;
            }
            // 加上 namespace 前缀写入 Raft 日志
            std::vector<std::string> ns_parts = parts;
            ns_parts[1] = g_ns_mgr.MakeKey(conn->name(), parts[1]);
            std::string cmd = SerializeCommand(ns_parts);
            int64_t index = g_raft_node->Propose(cmd,
                [conn](bool success, const std::string& result) {
                    if (success) {
                        conn->send(result);
                    } else {
                        conn->send(respError(result));
                    }
                });
            if (index < 0) {
                int leader = g_raft_node->GetLeaderId();
                conn->send(respError("MOVED " + std::to_string(leader)));
            }

        } else if (op == "DEL") {
            if (parts.size() != 2) {
                conn->send(respError("wrong number of arguments for DEL"));
                continue;
            }
            // 加上 namespace 前缀写入 Raft 日志
            std::vector<std::string> ns_parts = parts;
            ns_parts[1] = g_ns_mgr.MakeKey(conn->name(), parts[1]);
            std::string cmd = SerializeCommand(ns_parts);
            int64_t index = g_raft_node->Propose(cmd,
                [conn](bool success, const std::string& result) {
                    if (success) {
                        conn->send(result);
                    } else {
                        conn->send(respError(result));
                    }
                });
            if (index < 0) {
                int leader = g_raft_node->GetLeaderId();
                conn->send(respError("MOVED " + std::to_string(leader)));
            }

        } else if (op == "INFO") {
            std::string info;
            info += "node_id:" + std::to_string(g_raft_node->GetNodeId()) + "\r\n";
            info += "state:" + std::string(g_raft_node->IsLeader() ? "leader" :
                                           (g_raft_node->GetLeaderId() >= 0 ? "follower" : "candidate")) + "\r\n";
            info += "leader_id:" + std::to_string(g_raft_node->GetLeaderId()) + "\r\n";
            info += "term:" + std::to_string(g_raft_node->GetCurrentTerm()) + "\r\n";
            info += "commit_index:" + std::to_string(g_raft_node->GetCommitIndex()) + "\r\n";
            info += "namespace:" + g_ns_mgr.GetNs(conn->name()) + "\r\n";
            conn->send(respBulkString(info));

        } else {
            conn->send(respError("unknown command: " + op));
        }
    }
}

// ---- 主函数 ----
int main(int argc, char* argv[]) {
    gflags::SetUsageMessage("Raft-KV server (3-node Raft consensus, RocksDB-backed)");
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    google::InitGoogleLogging(argv[0]);

    // 解析 peer 列表
    auto peers = ParsePeers(FLAGS_peers);
    LOG(INFO) << "Configured " << peers.size() << " peers";

    // 初始化组件
    muduo::net::EventLoop loop;
    g_loop = &loop;

    g_sm.reset(new KVStateMachine(FLAGS_db_path));
    LOG(INFO) << "State machine opened at " << FLAGS_db_path;

    g_peer_mgr.reset(new PeerManager(&loop, FLAGS_node_id,
                                      FLAGS_raft_port, peers));
    g_peer_mgr->SetMessageHandler(OnRaftMessage);

    g_raft_node.reset(new RaftNode(FLAGS_node_id, peers, &loop,
                                    FLAGS_raft_log_path,
                                    g_sm.get(), g_peer_mgr.get()));

    // 启动 Peer 网络
    g_peer_mgr->Start();

    // 启动 Raft 状态机
    g_raft_node->Start();

    // 定时器：每 10ms tick Raft 选举/心跳
    loop.runEvery(0.01, []() {
        g_raft_node->Tick();
    });

    // 客户端 TCP 服务器
    muduo::net::InetAddress clientAddr(static_cast<uint16_t>(FLAGS_client_port));
    muduo::net::TcpServer clientServer(&loop, clientAddr, "RaftKVClient");

    clientServer.setConnectionCallback(OnClientConnection);
    clientServer.setMessageCallback(OnClientMessage);
    clientServer.start();

    LOG(INFO) << "Raft-KV server [node " << FLAGS_node_id
              << "] listening for clients on port " << FLAGS_client_port
              << ", Raft peers on port " << FLAGS_raft_port;

    loop.loop();
    return 0;
}
