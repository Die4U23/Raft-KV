#include <muduo/net/EventLoop.h>
#include <muduo/net/TcpServer.h>
#include <muduo/net/InetAddress.h>
#include <muduo/net/TcpConnection.h>
#include <muduo/base/Logging.h>
#include <gflags/gflags.h>
#include "common/resp_parser.h"
#include "storage/rocksdb_store.h"

DEFINE_int32(port, 8080, "TCP client port");
DEFINE_string(db_path, "/tmp/kv_db", "RocksDB data path");

static std::unique_ptr<RocksDBStore> g_store;

static std::string respBulkNull() { return "$-1\r\n"; }
static std::string respError(const std::string& err) { return "-ERR " + err + "\r\n"; }
static std::string respBulkString(const std::string& data) {
    return "$" + std::to_string(data.size()) + "\r\n" + data + "\r\n";
}
static std::string respInteger(int64_t n) { return ":" + std::to_string(n) + "\r\n"; }

void onConnection(const muduo::net::TcpConnectionPtr& conn) {
    LOG_INFO << "New connection: " << conn->peerAddress().toIpPort();
}

void onMessage(const muduo::net::TcpConnectionPtr& conn,
               muduo::net::Buffer* buf, muduo::Timestamp) {
    std::string msg = buf->retrieveAllAsString();
    auto commands = RespParser::Parse(msg);
    for (const auto& parts : commands) {
        if (parts.empty()) continue;
        std::string op = parts[0];
        for (auto& c : op) c = toupper(c);

        if (op == "PING") {
            conn->send("+PONG\r\n");
        } else if (op == "SET") {
            if (parts.size() != 3) { conn->send(respError("wrong number of arguments")); continue; }
            g_store->Put(parts[1], parts[2]);
            conn->send("+OK\r\n");
        } else if (op == "GET") {
            if (parts.size() != 2) { conn->send(respError("wrong number of arguments")); continue; }
            std::string value;
            if (g_store->Get(parts[1], &value)) conn->send(respBulkString(value));
            else conn->send(respBulkNull());
        } else if (op == "DEL") {
            if (parts.size() != 2) { conn->send(respError("wrong number of arguments")); continue; }
            g_store->Delete(parts[1]);
            conn->send(":1\r\n");
        } else {
            conn->send(respError("unknown command"));
        }
    }
}

int main(int argc, char* argv[]) {
    gflags::SetUsageMessage("Raft-KV server (single-node, RocksDB-backed)");
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
