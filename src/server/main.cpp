#include <muduo/net/EventLoop.h>
#include <muduo/net/TcpServer.h>
#include <muduo/net/TcpConnection.h>
#include <muduo/net/InetAddress.h>
#include <muduo/net/Buffer.h>
#include <muduo/net/TimerId.h>
#include <muduo/base/Logging.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <charconv>
#include <cctype>
#include <deque>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>
#include "common/resp_parser.h"
#include "common/command_buffer.h"
#include "common/batch_flush_policy.h"
#include "common/metrics.h"
#include "namespace/namespace_manager.h"
#include "raft/kv_state_machine.h"
#include "raft/peer_manager.h"
#include "raft/raft_node.h"
#include "raft/serial_apply_executor.h"
#include "raft_messages.pb.h"

DEFINE_int32(node_id, 0, "Raft node ID");
DEFINE_int32(client_port, 8080, "Client RESP port");
DEFINE_int32(raft_port, 9080, "Raft peer TCP port");
DEFINE_string(db_path, "/tmp/kv_db", "KV data path (paired with raft_log_path)");
DEFINE_string(raft_log_path, "/tmp/raft_log", "Raft log path");
DEFINE_string(peers, "0:127.0.0.1:9080,1:127.0.0.1:9081,2:127.0.0.1:9082", "id:host:port,...");
DEFINE_bool(leader_only_reads, false, "Restrict local reads to leader (NOT linearizable)");
DEFINE_int32(group_commit_ms, 1, "Partial batch collection window, 0..10 ms; full batches flush next loop turn");
DEFINE_bool(async_apply, true, "Apply committed KV batches on a serial worker; Raft log writes stay synchronous");
DEFINE_int32(max_clients, 1024, "Maximum concurrent client connections");

static KVStateMachine* g_sm = nullptr;
static RaftNode* g_raft = nullptr;
static muduo::net::EventLoop* g_loop = nullptr;
static NamespaceManager g_namespaces;
struct ClientSession {
    CommandBuffer input;
    bool waiting = false;
    bool closing = false;
    bool drain_scheduled = false;
    size_t accounted_output = 0;
};
static std::map<std::string, std::shared_ptr<ClientSession>> g_sessions;
static constexpr size_t kMaxCommandsPerTurn = 128;
static constexpr size_t kMaxTotalInput = 64 * 1024 * 1024;
static constexpr size_t kMaxTotalOutput = 64 * 1024 * 1024;
static constexpr size_t kMaxClientOutput = 4 * 1024 * 1024;
static constexpr size_t kMaxQueuedWrites = 1024;
static constexpr size_t kMaxQueuedWriteBytes = 16 * 1024 * 1024;
static size_t g_input_bytes = 0, g_output_bytes = 0, g_queued_write_bytes = 0;
static uint64_t g_overload_rejections = 0;
static LatencyStats g_write_queue_wait, g_write_completed, g_local_read;
static uint64_t g_flush_immediate = 0, g_flush_delayed = 0, g_flush_promoted = 0;
struct QueuedWrite {
    std::string command;
    std::weak_ptr<muduo::net::TcpConnection> connection;
    std::weak_ptr<ClientSession> session;
    SteadyClock::time_point enqueued_at;
};
static std::deque<QueuedWrite> g_writes;
static BatchFlushPolicy g_flush_policy;
static std::optional<muduo::net::TimerId> g_flush_timer;

static void CloseClient(const muduo::net::TcpConnectionPtr& conn,
                        const std::shared_ptr<ClientSession>& session) {
    session->closing = true;
    conn->forceClose();
}
static void AccountOutput(const muduo::net::TcpConnectionPtr& conn,
                          const std::shared_ptr<ClientSession>& session) {
    g_output_bytes -= session->accounted_output;
    session->accounted_output = conn->outputBuffer()->readableBytes();
    g_output_bytes += session->accounted_output;
    if (session->accounted_output == 0 && conn->outputBuffer()->internalCapacity() > 256 * 1024)
        conn->outputBuffer()->shrink(0);
}
static void SendReply(const muduo::net::TcpConnectionPtr& conn,
                      const std::shared_ptr<ClientSession>& session,
                      const std::string& reply) {
    if (session->closing || !conn->connected()) return;
    AccountOutput(conn, session);
    if (reply.size() > kMaxClientOutput - session->accounted_output ||
        reply.size() > kMaxTotalOutput - g_output_bytes) {
        ++g_overload_rejections;
        CloseClient(conn, session);
        return;
    }
    conn->send(reply);
    AccountOutput(conn, session);
}

static std::string Error(const std::string& message) { return "-ERR " + message + "\r\n"; }
static std::string Bulk(const std::string& value) {
    return "$" + std::to_string(value.size()) + "\r\n" + value + "\r\n";
}
static std::string SerializeCommand(const std::vector<std::string>& args) {
    std::string result = "*" + std::to_string(args.size()) + "\r\n";
    for (const auto& arg : args) result += Bulk(arg);
    return result;
}
static int ParseInteger(std::string_view text) {
    int value = 0;
    auto result = std::from_chars(text.data(), text.data() + text.size(), value);
    if (result.ec != std::errc{} || result.ptr != text.data() + text.size())
        throw std::invalid_argument("invalid integer in peer configuration");
    return value;
}
static std::vector<PeerInfo> ParsePeers(const std::string& text) {
    std::vector<PeerInfo> peers;
    std::istringstream stream(text);
    std::string token;
    while (std::getline(stream, token, ',')) {
        const size_t first = token.find(':');
        const size_t last = token.rfind(':');
        if (first == std::string::npos || first == last)
            throw std::invalid_argument("peer must be id:host:port");
        peers.push_back({ParseInteger(token.substr(0, first)),
                         token.substr(first + 1, last - first - 1),
                         ParseInteger(token.substr(last + 1))});
    }
    ValidatePeers(FLAGS_node_id, peers);
    return peers;
}
static void OnRaftMessage(int from, RaftMsgType type, const std::string& payload) {
    switch (type) {
    case RaftMsgType::kRequestVote: {
        raftcore::RequestVote request;
        if (request.ParseFromString(payload)) g_raft->HandleRequestVote(from, request);
        break;
    }
    case RaftMsgType::kRequestVoteResponse: {
        raftcore::RequestVoteResponse response;
        if (response.ParseFromString(payload)) g_raft->HandleRequestVoteResponse(from, response);
        break;
    }
    case RaftMsgType::kAppendEntries: {
        raftcore::AppendEntries request;
        if (request.ParseFromString(payload)) g_raft->HandleAppendEntries(from, request);
        break;
    }
    case RaftMsgType::kAppendEntriesResponse: {
        raftcore::AppendEntriesResponse response;
        if (response.ParseFromString(payload)) g_raft->HandleAppendEntriesResponse(from, response);
        break;
    }
    }
}
static void DrainClient(const muduo::net::TcpConnectionPtr& conn,
                        const std::shared_ptr<ClientSession>& session);

static void ScheduleDrain(const muduo::net::TcpConnectionPtr& conn,
                          const std::shared_ptr<ClientSession>& session) {
    if (session->drain_scheduled || session->closing || session->waiting ||
        session->input.UnreadBytes() == 0) return;
    session->drain_scheduled = true;
    std::weak_ptr<muduo::net::TcpConnection> weak_conn = conn;
    std::weak_ptr<ClientSession> weak_session = session;
    g_loop->queueInLoop([weak_conn, weak_session]() {
        const auto connection = weak_conn.lock();
        const auto state = weak_session.lock();
        if (!connection || !state) return;
        state->drain_scheduled = false;
        DrainClient(connection, state);
    });
}

static void ScheduleFlush();
static void FlushQueuedWrites() {
    std::vector<RaftNode::Proposal> proposals;
    std::vector<QueuedWrite> owners;
    proposals.reserve(BatchFlushPolicy::kMaxEntries);
    owners.reserve(BatchFlushPolicy::kMaxEntries);
    size_t bytes = 0;
    size_t inspected = 0;
    while (!g_writes.empty() && inspected < BatchFlushPolicy::kMaxEntries) {
        if (!proposals.empty() && g_writes.front().command.size() >
            BatchFlushPolicy::kMaxBytes - bytes) break;
        auto write = std::move(g_writes.front());
        g_writes.pop_front();
        ++inspected; // expired sessions also consume this turn's work budget
        g_queued_write_bytes -= write.command.size();
        const auto connection = write.connection.lock();
        const auto state = write.session.lock();
        if (!connection || !state || !connection->connected() || state->closing) continue;
        bytes += write.command.size();
        const auto weak_conn = write.connection;
        const auto weak_session = write.session;
        const auto enqueued_at = write.enqueued_at;
        g_write_queue_wait.Observe(ElapsedMicros(enqueued_at));
        proposals.push_back({std::move(write.command),
            [weak_conn, weak_session, enqueued_at](bool ok, const std::string& response) {
                // Server completion, including requests whose client disconnected;
                // socket delivery and client/network time are not measured here.
                if (ok) g_write_completed.Observe(ElapsedMicros(enqueued_at));
                const auto conn = weak_conn.lock();
                const auto session = weak_session.lock();
                if (!conn || !session || !conn->connected() || session->closing) return;
                SendReply(conn, session, response);
                session->waiting = false;
                ScheduleDrain(conn, session);
            }});
        owners.push_back(std::move(write));
    }
    if (!proposals.empty()) {
        const auto index = g_raft->ProposeBatch(std::move(proposals));
        if (index < 0) {
            if (index == -2) g_overload_rejections += owners.size();
            for (const auto& owner : owners) {
                const auto conn = owner.connection.lock();
                const auto state = owner.session.lock();
                if (!conn || !state) continue;
                state->waiting = false;
                SendReply(conn, state, index == -1 ?
                    Error("MOVED " + std::to_string(g_raft->GetLeaderId())) :
                    Error("BUSY proposal capacity exhausted"));
                ScheduleDrain(conn, state);
            }
        }
    }
    ScheduleFlush();
}
static void ScheduleFlush() {
    if (g_writes.empty()) return;
    // Preserve the oldest request's collection deadline across partial drains.
    const auto remaining = std::chrono::milliseconds(FLAGS_group_commit_ms) -
                           (SteadyClock::now() - g_writes.front().enqueued_at);
    const auto plan = g_flush_policy.Request(g_writes.size(), g_queued_write_bytes,
                                             remaining <= SteadyClock::duration::zero());
    if (plan.action == BatchFlushPolicy::Action::None) return;
    if (g_flush_timer) {
        g_loop->cancel(*g_flush_timer);
        g_flush_timer.reset();
        ++g_flush_promoted;
    }
    auto flush = [token = plan.token]() {
        // Cancellation cannot retract a timer already copied to an expired list.
        if (!g_flush_policy.Consume(token)) return;
        g_flush_timer.reset();
        FlushQueuedWrites();
    };
    if (plan.action == BatchFlushPolicy::Action::Immediate) {
        ++g_flush_immediate;
        g_loop->queueInLoop(std::move(flush)); // never re-enter Raft from SubmitWrite
    } else {
        ++g_flush_delayed;
        g_flush_timer = g_loop->runAfter(std::chrono::duration<double>(remaining).count(),
            [flush = std::move(flush)]() mutable { g_loop->queueInLoop(std::move(flush)); });
    }
}

static void SubmitWrite(const muduo::net::TcpConnectionPtr& conn,
                        const std::shared_ptr<ClientSession>& session,
                        std::vector<std::string> args) {
    args[1] = g_namespaces.MakeKey(conn->name(), args[1]);
    const std::string command = SerializeCommand(args);
    // Namespace expansion must also fit the state machine's parser limit.
    if (command.size() > RespParser::kMaxCommandBytes) {
        SendReply(conn, session, Error("command too large"));
        return;
    }
    if (!g_raft->IsLeader()) {
        SendReply(conn, session, Error("MOVED " + std::to_string(g_raft->GetLeaderId())));
        return;
    }
    if (g_writes.size() >= kMaxQueuedWrites ||
        command.size() > kMaxQueuedWriteBytes - g_queued_write_bytes) {
        ++g_overload_rejections;
        SendReply(conn, session, Error("BUSY write queue full"));
        return;
    }
    g_writes.push_back({command, conn, session, SteadyClock::now()});
    g_queued_write_bytes += command.size();
    session->waiting = true;
    ScheduleFlush();
}
static void DrainClient(const muduo::net::TcpConnectionPtr& conn,
                        const std::shared_ptr<ClientSession>& session) {
    for (size_t handled = 0; handled < kMaxCommandsPerTurn && conn->connected() &&
         !session->waiting && !session->closing && !session->drain_scheduled; ++handled) {
        auto parsed = session->input.Next();
        if (parsed.state == RespParser::State::NeedMore) return;
        if (parsed.state == RespParser::State::Invalid) {
            conn->stopRead();
            SendReply(conn, session, Error(parsed.error));
            session->closing = true;
            conn->shutdown();
            conn->forceCloseWithDelay(1.0);
            return;
        }
        g_input_bytes -= parsed.consumed;
        auto& args = parsed.args;
        for (char& c : args[0])
            c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
        const std::string& op = args[0];
        if (op == "PING" && args.size() == 1) {
            SendReply(conn, session, "+PONG\r\n");
        } else if (op == "SELECT" && args.size() == 2) {
            if (!NamespaceManager::IsValidName(args[1])) {
                SendReply(conn, session, Error("invalid namespace name"));
                continue;
            }
            g_namespaces.SetNs(conn->name(), args[1]);
            SendReply(conn, session, "+OK\r\n");
        } else if (op == "GET" && args.size() == 2) {
            if (FLAGS_leader_only_reads && !g_raft->IsLeader()) {
                SendReply(conn, session, Error("MOVED " + std::to_string(g_raft->GetLeaderId())));
                continue;
            }
            std::string value;
            const auto key = g_namespaces.MakeKey(conn->name(), args[1]);
            const auto started = SteadyClock::now();
            const bool found = g_sm->Get(key, &value);
            g_local_read.Observe(ElapsedMicros(started));
            SendReply(conn, session, found ? Bulk(value) : "$-1\r\n");
        } else if ((op == "SET" && args.size() == 3) ||
                   (op == "DEL" && args.size() == 2)) {
            SubmitWrite(conn, session, std::move(args));
        } else if (op == "INFO" && args.size() == 1) {
            std::string info = "node_id:" + std::to_string(g_raft->GetNodeId()) + "\r\n";
            info += "state:" + std::string(g_raft->StateName()) + "\r\n";
            info += "leader_id:" + std::to_string(g_raft->GetLeaderId()) + "\r\n";
            info += "term:" + std::to_string(g_raft->GetCurrentTerm()) + "\r\n";
            info += "commit_index:" + std::to_string(g_raft->GetCommitIndex()) + "\r\n";
            info += "last_applied:" + std::to_string(g_raft->GetLastApplied()) + "\r\n";
            info += "namespace:" + g_namespaces.GetNs(conn->name()) + "\r\n";
            info += "connected_clients:" + std::to_string(g_sessions.size()) + "\r\n";
            info += "queued_writes:" + std::to_string(g_writes.size()) + "\r\n";
            info += "queued_write_bytes:" + std::to_string(g_queued_write_bytes) + "\r\n";
            info += "pending_proposals:" + std::to_string(g_raft->PendingProposals()) + "\r\n";
            info += "pending_proposal_bytes:" + std::to_string(g_raft->PendingBytes()) + "\r\n";
            info += "proposal_batches:" + std::to_string(g_raft->ProposalBatches()) + "\r\n";
            info += "apply_batches:" + std::to_string(g_raft->ApplyBatches()) + "\r\n";
            info += "async_apply:" + std::to_string(g_raft->AsyncApplyEnabled()) + "\r\n";
            info += "apply_inflight:" + std::to_string(g_raft->ApplyInFlight()) + "\r\n";
            info += "apply_lag:" + std::to_string(g_raft->GetCommitIndex() - g_raft->GetLastApplied()) + "\r\n";
            info += "client_input_bytes:" + std::to_string(g_input_bytes) + "\r\n";
            info += "client_output_reserved_bytes:" + std::to_string(g_output_bytes) + "\r\n";
            info += "overload_rejections:" + std::to_string(g_overload_rejections) + "\r\n";
            info += "flush_immediate_scheduled:" + std::to_string(g_flush_immediate) + "\r\n";
            info += "flush_delayed_scheduled:" + std::to_string(g_flush_delayed) + "\r\n";
            info += "flush_promotions:" + std::to_string(g_flush_promoted) + "\r\n";
            info += g_write_queue_wait.ToInfo("write_queue_wait");
            info += g_write_completed.ToInfo("write_completed");
            info += g_local_read.ToInfo("local_read");
            info += g_raft->MetricsInfo();
            SendReply(conn, session, Bulk(info));
        } else {
            SendReply(conn, session, Error("unknown command or wrong number of arguments"));
        }
    }
    if (conn->connected()) ScheduleDrain(conn, session);
}
static void OnClientConnection(const muduo::net::TcpConnectionPtr& conn) {
    if (conn->connected()) {
        if (g_sessions.size() >= static_cast<size_t>(FLAGS_max_clients)) {
            ++g_overload_rejections;
            conn->forceClose();
            return;
        }
        g_sessions[conn->name()] = std::make_shared<ClientSession>();
        conn->setWriteCompleteCallback([](const muduo::net::TcpConnectionPtr& connection) {
            const auto found = g_sessions.find(connection->name());
            if (found != g_sessions.end()) AccountOutput(connection, found->second);
        });
    } else {
        const auto found = g_sessions.find(conn->name());
        if (found != g_sessions.end()) {
            g_input_bytes -= found->second->input.UnreadBytes();
            g_output_bytes -= found->second->accounted_output;
            g_sessions.erase(found);
        }
        g_namespaces.Remove(conn->name());
    }
}
static void OnClientMessage(const muduo::net::TcpConnectionPtr& conn,
                            muduo::net::Buffer* buffer, muduo::Timestamp) {
    const auto found = g_sessions.find(conn->name());
    if (found == g_sessions.end()) { conn->forceClose(); return; }
    const auto& session = found->second;
    if (session->closing) return;
    const size_t incoming = buffer->readableBytes();
    if (incoming > kMaxTotalInput - g_input_bytes ||
        !session->input.Append(std::string_view(buffer->peek(), incoming))) {
        ++g_overload_rejections;
        CloseClient(conn, session); // no out-of-order reply while a write is pending
        return;
    }
    g_input_bytes += incoming;
    buffer->retrieveAll();
    if (buffer->internalCapacity() > 256 * 1024) buffer->shrink(0);
    DrainClient(conn, session);
}
static int RunServer() {
    if (FLAGS_group_commit_ms < 0 || FLAGS_group_commit_ms > 10 || FLAGS_max_clients < 1)
        throw std::invalid_argument("invalid group_commit_ms or max_clients");
    const auto peers = ParsePeers(FLAGS_peers);
    if (FLAGS_client_port < 1 || FLAGS_client_port > 65535 ||
        FLAGS_raft_port < 1 || FLAGS_raft_port > 65535)
        throw std::invalid_argument("invalid listening port");
    for (const auto& peer : peers)
        if (peer.id == FLAGS_node_id && peer.raft_port != FLAGS_raft_port)
            throw std::invalid_argument("self peer port differs from raft_port");

    // Raft invalidates queued completions before the worker drains. The state
    // machine and EventLoop remain alive until all accepted work is finished.
    muduo::net::EventLoop loop;
    KVStateMachine state_machine(FLAGS_db_path);
    PeerManager peer_manager(&loop, FLAGS_node_id, FLAGS_raft_port, peers);
    std::unique_ptr<SerialApplyExecutor> apply_executor;
    if (FLAGS_async_apply)
        apply_executor = std::make_unique<SerialApplyExecutor>(
            [&loop](std::function<void()> completion) { loop.queueInLoop(std::move(completion)); });
    RaftNode raft(FLAGS_node_id, peers, &loop, FLAGS_raft_log_path,
                  &state_machine, &peer_manager, apply_executor.get());
    g_loop = &loop;
    g_sm = &state_machine;
    g_raft = &raft;
    peer_manager.SetMessageHandler(OnRaftMessage);
    peer_manager.Start();
    raft.Start();
    loop.runEvery(0.01, [&raft]() { raft.Tick(); });
    muduo::net::TcpServer server(&loop,
        muduo::net::InetAddress(static_cast<uint16_t>(FLAGS_client_port)), "RaftKVClient");
    server.setConnectionCallback(OnClientConnection);
    server.setMessageCallback(OnClientMessage);
    server.start();
    loop.loop();
    return 0;
}
int main(int argc, char* argv[]) {
    gflags::SetUsageMessage("Raft-KV: fixed-membership KV prototype");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);
    try {
        return RunServer();
    } catch (const std::exception& error) {
        std::cerr << "Raft-KV stopped: " << error.what() << '\n';
        return 1; // fail-stop rather than acknowledging uncertain storage state
    }
}
