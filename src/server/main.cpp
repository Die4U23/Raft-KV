#include <muduo/net/EventLoop.h>
#include <muduo/net/TcpServer.h>
#include <muduo/net/InetAddress.h>
#include <muduo/net/TcpConnection.h>
#include <muduo/base/Logging.h>
#include <gflags/gflags.h>
#include "common/resp_parser.h"
#include "storage/rocksdb_store.h"
#include <memory>

DEFINE_int32(port, 8080, "TCP client port");
DEFINE_string(db_path, "/tmp/raft_kv_db", "RocksDB data path");

static std::unique_ptr<RocksDBStore> g_store;

// RESP 响应辅助函数
std::string respBulkNull() { return "$-1\r\n"; }
std::string respError(const std::string& err) { return "-ERR " + err + "\r\n"; }
std::string respBulkString(const std::string& data) {
    return "$" + std::to_string(data.size()) + "\r\n" + data + "\r\n";
}
std::string respInteger(int64_t n) { return ":" + std::to_string(n) + "\r\n"; }

void onConnection(const muduo::net::TcpConnectionPtr& conn) {
    LOG_INFO << "New connection: " << conn->peerAddress().toIpPort();
}

void onMessage(const muduo::net::TcpConnectionPtr& conn,
               muduo::net::Buffer* buf,
               muduo::Timestamp time) {
    std::string msg = buf->retrieveAllAsString();
    auto commands = RespParser::Parse(msg);
    for (const auto& cmd : commands) {
        if (cmd.empty()) continue;
        std::string op = cmd[0];
        for (auto& c : op) c = toupper(c);

        if (op == "SET") {
            if (cmd.size() != 3) {
                conn->send(respError("wrong number of arguments for 'SET'"));
                continue;
            }
            g_store->Put(cmd[1], cmd[2]);
            conn->send("+OK\r\n");
        }
        else if (op == "GET") {
            if (cmd.size() != 2) {
                conn->send(respError("wrong number of arguments for 'GET'"));
                continue;
            }
            std::string value;
            if (g_store->Get(cmd[1], &value)) {
                conn->send(respBulkString(value));
            } else {
                conn->send(respBulkNull());
            }
        }
        else if (op == "DEL") {
            if (cmd.size() != 2) {
                conn->send(respError("wrong number of arguments for 'DEL'"));
                continue;
            }
            g_store->Delete(cmd[1]);
            conn->send(":1\r\n");
        }
        else if (op == "PING") {
            conn->send("+PONG\r\n");
        }
        else {
            conn->send(respError("unknown command '" + cmd[0] + "'"));
        }
    }
}

int main(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    g_store.reset(new RocksDBStore(FLAGS_db_path));
    LOG_INFO << "RocksDB opened at " << FLAGS_db_path;

    muduo::net::EventLoop loop;
    muduo::net::InetAddress listenAddr(FLAGS_port);
    muduo::net::TcpServer server(&loop, listenAddr, "RaftKV");

    server.setConnectionCallback(onConnection);
    server.setMessageCallback(onMessage);
    server.start();

    LOG_INFO << "Raft-KV server listening on port " << FLAGS_port;
    loop.loop();

    return 0;
}
