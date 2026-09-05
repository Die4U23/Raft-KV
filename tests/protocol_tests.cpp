#include "common/resp_parser.h"
#include "common/command_buffer.h"
#include "raft/raft_codec.h"
#include "raft/peers.h"
#include "namespace/namespace_manager.h"
#include <functional>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

static int checks = 0;
static void Check(bool ok, const char* message) {
    ++checks;
    if (!ok) throw std::runtime_error(message);
}
static void Throws(const std::function<void()>& action) {
    bool threw = false;
    try { action(); } catch (const std::exception&) { threw = true; }
    Check(threw, "expected malformed input to throw");
}
static std::vector<std::vector<std::string>> Feed(const std::vector<std::string>& chunks) {
    std::string input;
    std::vector<std::vector<std::string>> commands;
    for (const auto& chunk : chunks) {
        input += chunk;
        while (!input.empty()) {
            auto result = RespParser::TryParseOne(input);
            Check(result.state != RespParser::State::Invalid, "valid stream rejected");
            if (result.state == RespParser::State::NeedMore) break;
            Check(result.consumed > 0, "parser made no progress");
            input.erase(0, result.consumed);
            commands.push_back(std::move(result.args));
        }
    }
    Check(input.empty(), "complete input left unconsumed");
    return commands;
}
static void RespTests() {
    const std::string ping = "*1\r\n$4\r\nPING\r\n";
    for (size_t split = 0; split <= ping.size(); ++split) {
        const auto result = Feed({ping.substr(0, split), ping.substr(split)});
        Check(result.size() == 1 && result[0][0] == "PING", "split command lost");
    }
    std::vector<std::string> bytes;
    for (char c : ping + ping) bytes.emplace_back(1, c);
    Check(Feed(bytes).size() == 2, "byte-by-byte pipeline failed");
    Check(Feed({ping + ping.substr(0, 8), ping.substr(8)}).size() == 2,
          "complete command plus partial suffix lost");

    const std::string binary("a\0b\r\n", 5);
    const auto result = Feed({"*3\r\n$3\r\nSET\r\n$0\r\n\r\n$5\r\n" + binary + "\r\n"});
    Check(result[0][1].empty() && result[0][2] == binary, "binary payload corrupted");
    for (const std::string malformed : {
         "*0\r\n", "*-1\r\n", "*1junk\r\n$4\r\nPING\r\n",
         "*1\r\n$-1\r\n", "*1\r\n$4\r\nPINGxx", "*1\r\n$+4\r\nPING\r\n",
         "*18446744073709551616\r\n", "*1025\r\n", "*1\r\n$1048576\r\n",
         "\r\n", "+PING\r\n"}) {
        Check(RespParser::TryParseOne(malformed).state == RespParser::State::Invalid,
              "invalid RESP accepted");
    }
    Check(RespParser::TryParseOne("*1\r\n$4\r\nPI").state == RespParser::State::NeedMore,
          "incomplete RESP must wait");
    Check(RespParser::TryParseOne(std::string("*") + std::string(30, '9')).state ==
          RespParser::State::Invalid, "unbounded length header accepted");
}
static void CodecTests() {
    const std::string payload("abc\0xyz", 7);
    const auto frame = RaftCodec::Encode(RaftMsgType::kAppendEntries, 42, payload);
    for (size_t length = 0; length < frame.size(); ++length) {
        DecodedRaftMsg message;
        size_t consumed = 100;
        Check(!RaftCodec::TryDecode(frame.data(), length, &message, &consumed),
              "incomplete Raft frame accepted");
        Check(consumed == 0, "incomplete frame consumed bytes");
    }
    DecodedRaftMsg message;
    size_t consumed = 0;
    const auto pipeline = frame + frame;
    Check(RaftCodec::TryDecode(pipeline.data(), pipeline.size(), &message, &consumed),
          "complete frame rejected");
    Check(consumed == frame.size() && message.sender_id == 42 &&
          message.payload == payload, "Raft frame roundtrip mismatch");
    for (uint32_t length = 0; length < 5; ++length) {
        std::string bad(4, '\0');
        RaftCodec::WriteU32(bad.data(), length);
        Throws([&] { RaftCodec::TryDecode(bad.data(), bad.size(), &message, &consumed); });
    }
    auto bad = frame;
    RaftCodec::WriteU32(bad.data(), static_cast<uint32_t>(RaftCodec::kMaxFrameSize + 1));
    Throws([&] { RaftCodec::TryDecode(bad.data(), bad.size(), &message, &consumed); });
    bad = frame;
    bad[4] = static_cast<char>(255);
    Throws([&] { RaftCodec::TryDecode(bad.data(), bad.size(), &message, &consumed); });
    bad = frame;
    RaftCodec::WriteU32(bad.data() + 5, UINT32_MAX);
    Throws([&] { RaftCodec::TryDecode(bad.data(), bad.size(), &message, &consumed); });
    Throws([&] { RaftCodec::Encode(RaftMsgType::kRequestVote, -1, ""); });
}
static void CommandBufferTests() {
    const std::string ping = "*1\r\n$4\r\nPING\r\n";
    for (size_t split = 0; split < ping.size(); ++split) {
        CommandBuffer buffer;
        Check(buffer.Append(std::string_view(ping).substr(0, split)), "fragment rejected");
        Check(buffer.Next().state == RespParser::State::NeedMore &&
              buffer.UnreadBytes() == split, "incomplete command consumed bytes");
        Check(buffer.Append(std::string_view(ping).substr(split)), "suffix rejected");
        const auto result = buffer.Next();
        Check(result.state == RespParser::State::Complete && result.args.size() == 1 &&
              result.args[0] == "PING", "buffer fragmented command corrupted");
        Check(buffer.UnreadBytes() == 0 && buffer.Next().state == RespParser::State::NeedMore,
              "drained buffer did not become empty");
    }

    CommandBuffer pipeline;
    std::string commands;
    for (int i = 0; i < 10000; ++i) commands += ping;
    Check(pipeline.Append(commands), "large valid pipeline rejected");
    bool ordered = true;
    for (int i = 0; i < 10000; ++i) {
        const auto result = pipeline.Next();
        ordered = ordered && result.state == RespParser::State::Complete &&
                  result.args.size() == 1 && result.args[0] == "PING" &&
                  pipeline.UnreadBytes() == static_cast<size_t>(9999 - i) * ping.size();
    }
    Check(ordered && pipeline.UnreadBytes() == 0, "large pipeline lost or repeated commands");

    CommandBuffer partial;
    Check(partial.Append(ping + ping.substr(0, 8)), "partial pipeline rejected");
    Check(partial.Next().state == RespParser::State::Complete && partial.UnreadBytes() == 8,
          "complete prefix did not preserve incomplete suffix");
    Check(partial.Next().state == RespParser::State::NeedMore && partial.UnreadBytes() == 8,
          "partial suffix was consumed");
    const std::string oversized(CommandBuffer::kMaxBufferedBytes + 1, 'x');
    Check(!partial.Append(oversized) && partial.UnreadBytes() == 8,
          "oversized append changed unread bytes");
    Check(partial.Append(ping.substr(8) + ping), "append after prefix consumption failed");
    const auto recovered = partial.Next();
    const auto following = partial.Next();
    Check(recovered.state == RespParser::State::Complete && recovered.args.size() == 1 &&
          recovered.args[0] == "PING" && following.state == RespParser::State::Complete &&
          following.args.size() == 1 && following.args[0] == "PING" &&
          partial.UnreadBytes() == 0, "rejected append corrupted buffered suffix");

    // Eight individually valid commands fill the buffer exactly. Consuming one
    // must free admission capacity even if its storage has not been compacted.
    constexpr size_t slot = CommandBuffer::kMaxBufferedBytes / 8;
    const auto large = [](char value) {
        const std::string payload(slot - 15, value);
        return "*1\r\n$" + std::to_string(payload.size()) + "\r\n" + payload + "\r\n";
    };
    CommandBuffer full;
    for (int i = 0; i < 8; ++i)
        Check(full.Append(large(static_cast<char>('A' + i))), "exact-limit command rejected");
    Check(full.UnreadBytes() == CommandBuffer::kMaxBufferedBytes && !full.Append("x") &&
          full.UnreadBytes() == CommandBuffer::kMaxBufferedBytes,
          "full buffer limit or rejection atomicity failed");
    const auto first = full.Next();
    Check(first.state == RespParser::State::Complete && first.args.size() == 1 &&
          first.args[0] == std::string(slot - 15, 'A'), "full buffer prefix corrupted");
    Check(full.Append(large('Z')) && full.UnreadBytes() == CommandBuffer::kMaxBufferedBytes,
          "consumed prefix still counted against admission limit");
    bool intact = true;
    for (int i = 1; i <= 8; ++i) {
        const auto result = full.Next();
        const char expected = i == 8 ? 'Z' : static_cast<char>('A' + i);
        intact = intact && result.state == RespParser::State::Complete &&
                 result.args.size() == 1 && result.args[0] == std::string(slot - 15, expected);
    }
    Check(intact && full.UnreadBytes() == 0, "limit handling changed pending command bytes");

    CommandBuffer invalid;
    Check(invalid.Append("*0\r\n" + ping), "malformed buffer append rejected prematurely");
    const size_t unread = invalid.UnreadBytes();
    Check(invalid.Next().state == RespParser::State::Invalid && invalid.UnreadBytes() == unread,
          "malformed command was consumed");
    Check(invalid.Next().state == RespParser::State::Invalid && invalid.UnreadBytes() == unread,
          "malformed prefix was skipped on retry");
}
static void MembershipTests() {
    ValidatePeers(10, {{10, "127.0.0.1", 9000}, {30, "127.0.0.1", 9001}});
    Throws([] { ValidatePeers(0, {{0, "x", 1}, {0, "x", 2}}); });
    Throws([] { ValidatePeers(0, {{1, "x", 1}}); });
    Throws([] { ValidatePeers(0, {{0, "x", 65536}}); });
    NamespaceManager manager;
    manager.SetNs("a", "tenant_a");
    Check(manager.MakeKey("a", "x:y") == "tenant_a:x:y", "namespace encoding failed");
    Check(manager.GetNs("b") == "default", "namespace leaked to another connection");
    manager.Remove("a");
    Check(manager.GetNs("a") == "default", "namespace survived disconnect");
    Check(!NamespaceManager::IsValidName(std::string(1, static_cast<char>(0xff))),
          "non-ASCII namespace accepted");
    Check(!NamespaceManager::IsValidName("a:b"), "separator accepted in namespace");
    Check(NamespaceManager::IsValidName("Ab_0-9"), "valid namespace rejected");
}
int main() {
    try {
        RespTests(); CodecTests(); MembershipTests(); CommandBufferTests();
        std::cout << "PASS: " << checks << " protocol and namespace checks\n";
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "FAIL: " << e.what() << '\n';
        return 1;
    }
}
