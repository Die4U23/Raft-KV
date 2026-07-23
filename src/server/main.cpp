#include <muduo/net/EventLoop.h>
#include <muduo/net/TcpServer.h>
#include <muduo/net/InetAddress.h>
#include <muduo/net/TcpConnection.h>
#include <muduo/base/Logging.h>
#include <gflags/gflags.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <string>
#include <sstream>
#include <vector>
#include <memory>

DEFINE_int32(port, 8080, "TCP port for client connections");
DEFINE_string(db_path, "/tmp/raft_kv_db", "Path to RocksDB data directory");

// 全局 RocksDB 实例
rocksdb::DB* db = nullptr;

// 分割字符串
std::vector<std::string> split(const std::string& s) {
    std::vector<std::string> tokens;
    std::istringstream iss(s);
    std::string token;
    while (iss >> token) {
        tokens.push_back(token);
    }
    return tokens;
}

void onConnection(const muduo::net::TcpConnectionPtr& conn) {
    LOG_INFO << "New connection: " << conn->peerAddress().toIpPort();
}

void onMessage(const muduo::net::TcpConnectionPtr& conn,
               muduo::net::Buffer* buf,
               muduo::Timestamp time) {
    std::string msg = buf->retrieveAllAsString();
    while (!msg.empty() && (msg.back() == '\n' || msg.back() == '\r')) {
        msg.pop_back();
    }

    LOG_INFO << "Received: " << msg;

    auto tokens = split(msg);
    if (tokens.empty()) {
        conn->send("-ERR empty command\r\n");
        return;
    }

    std::string cmd = tokens[0];
    for (char& c : cmd) c = toupper(c);

    rocksdb::Status s;
    std::string value;

    if (cmd == "SET") {
        if (tokens.size() != 3) {
            conn->send("-ERR wrong number of arguments for 'SET'\r\n");
            return;
        }
        s = db->Put(rocksdb::WriteOptions(), tokens[1], tokens[2]);
        if (s.ok()) {
            conn->send("+OK\r\n");
        } else {
            conn->send("-ERR " + s.ToString() + "\r\n");
        }
    }
    else if (cmd == "GET") {
        if (tokens.size() != 2) {
            conn->send("-ERR wrong number of arguments for 'GET'\r\n");
            return;
        }
        s = db->Get(rocksdb::ReadOptions(), tokens[1], &value);
        if (s.ok()) {
            conn->send("$" + std::to_string(value.size()) + "\r\n" + value + "\r\n");
        } else if (s.IsNotFound()) {
            conn->send("$-1\r\n");
        } else {
            conn->send("-ERR " + s.ToString() + "\r\n");
        }
    }
    else if (cmd == "DEL") {
        if (tokens.size() != 2) {
            conn->send("-ERR wrong number of arguments for 'DEL'\r\n");
            return;
        }
        s = db->Delete(rocksdb::WriteOptions(), tokens[1]);
        if (s.ok()) {
            conn->send(":1\r\n");
        } else {
            conn->send(":0\r\n");
        }
    }
    else if (cmd == "PING") {
        conn->send("+PONG\r\n");
    }
    else {
        conn->send("-ERR unknown command '" + tokens[0] + "'\r\n");
    }
}

int main(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    // 初始化 RocksDB
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::Status status = rocksdb::DB::Open(options, FLAGS_db_path, &db);
    if (!status.ok()) {
        LOG_ERROR << "Failed to open RocksDB: " << status.ToString();
        return 1;
    }
    LOG_INFO << "RocksDB opened at " << FLAGS_db_path;

    muduo::net::EventLoop loop;
    muduo::net::InetAddress listenAddr(FLAGS_port);
    muduo::net::TcpServer server(&loop, listenAddr, "RaftKV");

    server.setConnectionCallback(onConnection);
    server.setMessageCallback(onMessage);
    server.start();

    LOG_INFO << "Raft-KV server listening on port " << FLAGS_port;
    loop.loop();

    // 程序退出时清理（实际上 loop 会一直运行，但为了完美）
    delete db;
    return 0;
}
