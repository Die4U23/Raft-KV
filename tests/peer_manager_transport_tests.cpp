// Real Linux Muduo lifecycle regression: initially refused connects, immediate
// close loops, healing, and destroying PeerManager with a reconnect pending.
#include "raft/peer_manager.h"
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <iostream>
#include <stdexcept>

static void Check(bool ok, const char* message) {
    if (!ok) throw std::runtime_error(message);
}

static int UnusedPort() {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    Check(fd >= 0, "socket failed");
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    socklen_t length = sizeof(addr);
    const bool ok = ::bind(fd, reinterpret_cast<sockaddr*>(&addr), length) == 0 &&
        ::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &length) == 0;
    ::close(fd);
    Check(ok, "ephemeral port allocation failed");
    return ntohs(addr.sin_port);
}

int main(int argc, char** argv) {
    (void)argc;
    google::InitGoogleLogging(argv[0]);
    try {
        muduo::net::EventLoop loop;
        bool healed = false;
        int attempts = 0, received = 0, attempts_at_destroy = -1;
        muduo::net::TcpConnectionPtr connection;
        const int remote_port = UnusedPort();
        muduo::net::TcpServer remote(&loop, muduo::net::InetAddress("127.0.0.1", remote_port), "FlappingPeer");
        const int local_port = UnusedPort();
        remote.setConnectionCallback([&](const auto& conn) {
            if (!conn->connected()) { if (connection == conn) connection.reset(); return; }
            ++attempts;
            if (!healed) { conn->forceClose(); return; }
            connection = conn;
            conn->send(RaftCodec::Encode(RaftMsgType::kAppendEntriesResponse, 1, "healthy"));
        });
        remote.setMessageCallback([](const auto&, auto* buf, auto) { buf->retrieveAll(); });
        std::vector<PeerInfo> peers{{0, "127.0.0.1", local_port}, {1, "127.0.0.1", remote_port}};
        auto manager = std::make_unique<PeerManager>(&loop, 0, local_port, peers);
        manager->SetMessageHandler([&](int peer, RaftMsgType type, const std::string& payload) {
            Check(peer == 1 && type == RaftMsgType::kAppendEntriesResponse && payload == "healthy", "bad healed RPC");
            ++received;
        });
        manager->Start();
        manager->Start();  // Must not replace an active Connector or start twice.
        loop.runAfter(0.7, [&] { remote.start(); }); // Connector must retry initial ECONNREFUSED.
        loop.runAfter(4.0, [&] {
            Check(attempts > 0 && attempts <= 8, "flapping was not exercised or was unbounded");
            healed = true;
        });
        loop.runAfter(6.5, [&] {
            Check(received > 0 && connection && connection->connected(), "failed to heal after backoff");
            connection->forceClose();
        });
        loop.runAfter(6.7, [&] { manager.reset(); attempts_at_destroy = attempts; });
        loop.runAfter(9.0, [&] {
            Check(attempts_at_destroy >= 0 && attempts == attempts_at_destroy, "retry ran after destruction");
            loop.quit();
        });
        loop.loop();
        std::cout << "PASS: initial refusal, bounded flapping, healed RPC and pending-retry destruction\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
