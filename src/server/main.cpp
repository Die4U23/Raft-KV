#include <muduo/net/EventLoop.h>
#include <muduo/net/TcpServer.h>
#include <muduo/net/InetAddress.h>
#include <muduo/net/TcpConnection.h>
#include <muduo/base/Logging.h>
#include <gflags/gflags.h>
#include <unordered_map>
#include <string>
#include <sstream>
#include <vector>

DEFINE_int32(port, 8080, "TCP port for client connections");

// 简单的内存存储
std::unordered_map<std::string, std::string> store;

// 分割字符串（按空白字符）
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
    // 去掉可能存在的换行符
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
    // 统一转大写，忽略大小写
    for (char& c : cmd) c = toupper(c);

    if (cmd == "SET") {
        if (tokens.size() != 3) {
            conn->send("-ERR wrong number of arguments for 'SET'\r\n");
            return;
        }
        store[tokens[1]] = tokens[2];
        conn->send("+OK\r\n");
    }
    else if (cmd == "GET") {
        if (tokens.size() != 2) {
            conn->send("-ERR wrong number of arguments for 'GET'\r\n");
            return;
        }
        auto it = store.find(tokens[1]);
        if (it != store.end()) {
            // 返回长度和内容（类似 Redis Bulk String）
            std::string val = it->second;
            conn->send("$" + std::to_string(val.size()) + "\r\n" + val + "\r\n");
        } else {
            conn->send("$-1\r\n"); // nil
        }
    }
    else if (cmd == "DEL") {
        if (tokens.size() != 2) {
            conn->send("-ERR wrong number of arguments for 'DEL'\r\n");
            return;
        }
        int removed = store.erase(tokens[1]);
        conn->send(":" + std::to_string(removed) + "\r\n"); // 整数回复
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
