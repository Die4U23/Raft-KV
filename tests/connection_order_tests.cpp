// Production DrainClient/ExecuteNextCommand queue. Homemade FIFO is not CTest.
#include <chrono>
#include "common/command_type.h"
#include "common/config_cache.h"
#include "common/frame_mac.h"
#include "common/session_queue.h"
#include "common/shard.h"
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

static int checks = 0;

static void Check(bool ok, const char* message) {
    ++checks;
    if (!ok) {
        std::cerr << "FAIL: " << message << std::endl;
        throw std::runtime_error(message);
    }
}

static std::string Resp(std::initializer_list<std::string> args) {
    std::string out = "*" + std::to_string(args.size()) + "\r\n";
    for (const auto& arg : args)
        out += "$" + std::to_string(arg.size()) + "\r\n" + arg + "\r\n";
    return out;
}

static void TestClassifier() {
    auto set = ClassifyCommand({"SET", "k", "v"});
    Check(set.type == CommandClass::WRITE && set.error.empty(), "SET 3 args is WRITE");
    auto del = ClassifyCommand({"DEL", "k"});
    Check(del.type == CommandClass::WRITE && del.error.empty(), "DEL 2 args is WRITE");
    auto get = ClassifyCommand({"GET", "k"});
    Check(get.type == CommandClass::READ && get.error.empty(), "GET 2 args is READ");
    auto ping = ClassifyCommand({"PING"});
    Check(ping.type == CommandClass::READ, "PING is READ");
    auto info = ClassifyCommand({"INFO"});
    Check(info.type == CommandClass::READ, "INFO is READ");
    auto select = ClassifyCommand({"SELECT", "ns"});
    Check(select.type == CommandClass::READ, "SELECT is READ");

    auto bogus = ClassifyCommand({"BOGUS"});
    Check(bogus.type == CommandClass::ERROR, "unknown command is ERROR");
    Check(bogus.error == "ERR unknown command 'BOGUS'", "unknown command text");
    auto short_set = ClassifyCommand({"SET", "k"});
    Check(short_set.type == CommandClass::ERROR, "SET missing value is ERROR");
    Check(short_set.error.find("wrong number of arguments") != std::string::npos,
          "SET arity error text");
    auto bad_get = ClassifyCommand({"GET"});
    Check(bad_get.type == CommandClass::ERROR, "GET missing key is ERROR");
    auto empty = ClassifyCommand({});
    Check(empty.type == CommandClass::ERROR && empty.error == "ERR empty command",
          "empty command text");

    auto ping_args = ClassifyCommand({"PING", "x"});
    Check(ping_args.type == CommandClass::ERROR &&
          ping_args.error == "ERR wrong number of arguments for 'PING' command",
          "PING arity");
    auto info_args = ClassifyCommand({"INFO", "all"});
    Check(info_args.type == CommandClass::ERROR, "INFO extra arg is ERROR");
    auto select_short = ClassifyCommand({"SELECT"});
    Check(select_short.type == CommandClass::ERROR, "SELECT missing ns is ERROR");
    auto select_extra = ClassifyCommand({"SELECT", "a", "b"});
    Check(select_extra.type == CommandClass::ERROR, "SELECT extra arg is ERROR");
    auto get_extra = ClassifyCommand({"GET", "k", "extra"});
    Check(get_extra.type == CommandClass::ERROR, "GET extra arg is ERROR");
    auto set_extra = ClassifyCommand({"SET", "k", "v", "nx"});
    Check(set_extra.type == CommandClass::ERROR &&
          set_extra.error.find("wrong number of arguments") != std::string::npos,
          "SET extra arg is ERROR");
    auto del_extra = ClassifyCommand({"DEL", "k", "k2"});
    Check(del_extra.type == CommandClass::ERROR, "DEL extra arg is ERROR");
    auto del_short = ClassifyCommand({"DEL"});
    Check(del_short.type == CommandClass::ERROR, "DEL missing key is ERROR");
    auto lower = ClassifyCommand({"set", "k", "v"});
    Check(lower.type == CommandClass::ERROR &&
          lower.error == "ERR unknown command 'set'",
          "classifier is case-sensitive; DrainClient must uppercase first");

    auto idem_set = ClassifyCommand({"SET", "k", "v", "c1", "1"});
    Check(idem_set.type == CommandClass::WRITE && idem_set.error.empty(),
          "SET with client id and request id is WRITE");
    auto idem_del = ClassifyCommand({"DEL", "k", "c1", "2"});
    Check(idem_del.type == CommandClass::WRITE && idem_del.error.empty(),
          "DEL with client id and request id is WRITE");
    auto bad_seq = ClassifyCommand({"SET", "k", "v", "c1", "01"});
    Check(bad_seq.type == CommandClass::ERROR &&
          bad_seq.error == "ERR invalid client id or request id",
          "leading zero request id was accepted");
    auto zero_seq = ClassifyCommand({"DEL", "k", "c1", "0"});
    Check(zero_seq.type == CommandClass::ERROR, "request id 0 was accepted");
    auto empty_client = ClassifyCommand({"SET", "k", "v", "", "1"});
    Check(empty_client.type == CommandClass::ERROR, "empty client id was accepted");
    auto huge = ClassifyCommand({"SET", "k", "v", std::string(129, 'a'), "1"});
    Check(huge.type == CommandClass::ERROR, "oversized client id was accepted");
    auto cfg = ClassifyCommand({"CFGSET", "policy", "on"});
    Check(cfg.type == CommandClass::WRITE, "CFGSET is WRITE");
    auto cfg_get = ClassifyCommand({"CFGGET", "policy"});
    Check(cfg_get.type == CommandClass::READ, "CFGGET is READ");
    auto cache = ClassifyCommand({"CFGCACHE", "policy", "1000"});
    Check(cache.type == CommandClass::LOCAL, "CFGCACHE is LOCAL");
    auto auth = ClassifyCommand({"AUTH", "secret"});
    Check(auth.type == CommandClass::LOCAL, "AUTH is LOCAL");
    auto join = ClassifyCommand({"MEMBER", "JOIN", "10"});
    Check(join.type == CommandClass::WRITE, "MEMBER JOIN is WRITE");
    auto bad_peer = ClassifyCommand({"MEMBER", "LEAVE", "01"});
    Check(bad_peer.type == CommandClass::ERROR, "leading-zero peer id was accepted");
    auto reserved = ClassifyCommand({"SET", std::string("\0k", 2), "v"});
    Check(reserved.type == CommandClass::ERROR && reserved.error == "ERR reserved key",
          "reserved key was accepted");
}

static void TestCacheShardAndMac() {
    ConfigCache cache;
    const auto now = SteadyClock::now();
    uint64_t version = 0;
    std::string value;
    Check(!cache.Fresh("policy", now, 1000, &version, &value), "empty cache was fresh");
    cache.Store("policy", 3, "on", now);
    Check(cache.Fresh("policy", now, 0, &version, &value) && version == 3 && value == "on",
          "zero max-age missed a just-stored value");
    Check(!cache.Fresh("policy", now + std::chrono::milliseconds(2), 1, &version, &value),
          "stale cache was served");
    Check(cache.Fresh("policy", now + std::chrono::milliseconds(2), 2, &version, &value),
          "entry inside max-age was a miss");
    cache.Invalidate("policy");
    Check(!cache.Fresh("policy", now, 1000, &version, &value), "invalidate left the entry");

    Check(ShardOf("any", 1) == 0, "one shard hashed the key");
    int left = -1, right = -1;
    for (int i = 0; i < 32 && left == right; ++i) {
        left = ShardOf("key-" + std::to_string(i), 4);
        right = ShardOf("key-" + std::to_string(i + 1), 4);
    }
    Check(left != right, "four shards put every sample key on the same group");
    int shard = -1;
    std::string payload;
    const auto prefixed = PrefixShardPayload(4, 2, "abc");
    Check(StripShardPayload(4, prefixed, &shard, &payload) && shard == 2 && payload == "abc",
          "shard prefix did not round-trip");
    Check(PrefixShardPayload(1, 0, "abc") == "abc", "one shard changed the payload");

    const std::string frame = RaftCodec::Encode(RaftMsgType::kAppendEntries, 1, "payload");
    Check(SealFrame("", frame) == frame, "empty token changed the frame");
    const auto sealed = SealFrame("secret", frame);
    Check(sealed != frame && sealed.compare(0, 4, "MAC1") == 0, "token did not seal the frame");
    std::string opened;
    size_t used = 0;
    Check(UnsealFrame("secret", sealed.data(), sealed.size(), &opened, &used) == FrameSealStatus::Ok &&
          opened == frame && used == sealed.size(), "sealed frame did not open");
    auto tampered = sealed;
    tampered.back() = static_cast<char>(tampered.back() ^ 0x1);
    Check(UnsealFrame("secret", tampered.data(), tampered.size(), &opened, &used) == FrameSealStatus::Reject,
          "tampered MAC was accepted");
    const std::string key(20, '\x0b');
    const auto mac = frame_mac::HmacSha256(key, "Hi There");
    const char expect[] = "\xb0\x34\x4c\x61\xd8\xdb\x38\x53\x5c\xa8\xaf\xce\xaf\x0b\xf1\x2b"
                          "\x88\x1d\xc2\x00\xc9\x83\x3d\xa7\x26\xe9\x37\x6c\x2e\x32\xcf\xf7";
    Check(frame_mac::MacEqual(mac.data(), reinterpret_cast<const uint8_t*>(expect), 32),
          "HMAC-SHA256 test vector mismatch");
}

static void TestReadIndexRedirectErrors() {
    Check(IsReadIndexRedirectError("not leader"), "not leader is a redirect");
    Check(IsReadIndexRedirectError("no leader"), "no leader is a redirect");
    Check(IsReadIndexRedirectError("leadership lost"), "step-down is a redirect");
    Check(IsReadIndexRedirectError("server stopped"), "stop is a redirect");
    Check(!IsReadIndexRedirectError("read index timeout"), "timeout stays a local error");
    Check(!IsReadIndexRedirectError("read index queue full"), "overload stays a local error");
    Check(!IsReadIndexRedirectError("waiting for leader to commit no-op"),
          "pre-noop wait is not MOVED");
    Check(DecideLinearizableGet(true, true, "") == LinearizableGetAction::Serve,
          "quorum plus leadership serves the local value");
    Check(DecideLinearizableGet(true, false, "") == LinearizableGetAction::Redirect,
          "successful quorum after step-down must not serve the local value");
    Check(DecideLinearizableGet(false, false, "leadership lost") == LinearizableGetAction::Redirect,
          "step-down failure redirects");
    Check(DecideLinearizableGet(false, true, "read index timeout") == LinearizableGetAction::Fail,
          "timeout while still leader stays a local error");
    Check(DecideLinearizableGet(false, false, "read index timeout") == LinearizableGetAction::Redirect,
          "timeout after step-down redirects");
    Check(DecideLinearizableGet(false, true, "read index queue full") == LinearizableGetAction::Fail,
          "overload while still leader stays a local error");
}

static void TestSessionQueueLimits() {
    Check(SessionQueueLimits::kMaxCommands == 1000, "F3 command limit drifted");
    Check(SessionQueueLimits::kMaxBytes == 4 * 1024 * 1024, "F3 byte limit drifted");
    Check(SessionQueueLimits::kMaxCommandsPerTurn == 128, "drain turn limit drifted");
    Check(!SessionQueueLimits::IsFull(0, 0), "empty queue reported full");
    Check(!SessionQueueLimits::IsFull(999, SessionQueueLimits::kMaxBytes - 1),
          "999 commands just under 4MiB reported full");
    Check(SessionQueueLimits::IsFull(1000, 0), "1000 queued commands were still admitted");
    Check(SessionQueueLimits::IsFull(0, SessionQueueLimits::kMaxBytes),
          "exactly 4MiB queued was still admitted");
}

static DrainOutcome Feed(SessionCommandQueue& queue, const std::string& wire,
                         size_t max_per_turn = SessionQueueLimits::kMaxCommandsPerTurn) {
    CommandBuffer buffer;
    Check(buffer.Append(wire), "RESP append rejected");
    return DrainCommands(buffer, queue, max_per_turn, true);
}

struct SessionDriver {
    SessionCommandQueue queue;
    std::vector<std::string> replies;
    bool executing = false;
    QueuedCommand inflight;

    void Drain(const std::string& wire) {
        const auto drained = Feed(queue, wire);
        Check(!drained.invalid, drained.invalid_error);
        Process();
    }

    void Process() {
        if (executing || queue.Empty())
            return;
        executing = true;
        inflight = queue.PopFront();
        if (inflight.type == CommandClass::ERROR) {
            replies.push_back("-ERR " + inflight.error_message);
            Complete();
        } else if (inflight.type == CommandClass::READ) {
            replies.push_back("OK: " + inflight.args[0]);
            Complete();
        }
    }

    void FinishWrite() {
        Check(executing && inflight.type == CommandClass::WRITE, "no in-flight write");
        replies.push_back("OK: " + inflight.args[0]);
        Complete();
    }

    void Complete() {
        executing = false;
        inflight = {};
        Process();
    }
};

static void TestErrorDoesNotOvertakeWrite() {
    SessionDriver q;
    q.Drain(Resp({"SET", "k", "v"}) + Resp({"BOGUS"}) + Resp({"GET", "k"}));
    Check(q.replies.empty(), "WRITE must not reply before commit");
    Check(q.executing, "WRITE holds executing");
    Check(q.queue.size() == 2, "ERROR and GET remain queued");
    q.FinishWrite();
    Check(q.replies.size() == 3, "three replies");
    Check(q.replies[0] == "OK: SET", "SET first");
    Check(q.replies[1] == "-ERR ERR unknown command 'BOGUS'", "ERROR does not overtake SET");
    Check(q.replies[2] == "OK: GET", "GET last");
    Check(!q.executing && q.queue.Empty() && q.queue.queued_bytes == 0,
          "idle after drain leaked accounting");
}

static void TestMixedPipelineAndLowercase() {
    SessionDriver q;
    q.Drain(Resp({"set", "k", "v"}) + Resp({"PING"}) + Resp({"GET", "k"}) +
            Resp({"SET", "k2", "v2"}) + Resp({"GET", "k2"}));
    Check(q.inflight.args[0] == "SET", "DrainClient did not uppercase SET");
    q.FinishWrite();
    q.FinishWrite();
    Check(q.replies.size() == 5, "pipeline size");
    Check(q.replies[0] == "OK: SET" && q.replies[1] == "OK: PING" &&
          q.replies[2] == "OK: GET" && q.replies[3] == "OK: SET" &&
          q.replies[4] == "OK: GET", "pipeline RESP order");
}

static void TestQueueStopsAtCommandLimit() {
    SessionCommandQueue queue;
    Check(queue.Enqueue({"SET", "k", "v"}, 14), "first write rejected");
    Check(!queue.Empty() && queue.PopFront().type == CommandClass::WRITE, "write pop");
    for (size_t i = 0; i < SessionQueueLimits::kMaxCommands; ++i)
        Check(queue.Enqueue({"PING"}, 14), "PING rejected before the production cap");
    Check(queue.IsFull() && !queue.Enqueue({"PING"}, 14),
          "queue still admitted after 1000 waiting commands");
    while (!queue.Empty())
        queue.PopFront();
    Check(!queue.IsFull() && queue.queued_bytes == 0, "pop did not restore admission");
}

static void TestQueueStopsAtByteLimitAndDoesNotLeak() {
    SessionDriver q;
    const auto ping = Resp({"PING"});
    std::string many;
    for (int i = 0; i < 32; ++i)
        many += ping;
    q.Drain(many);
    Check(q.replies.size() == 32 && q.queue.Empty() && q.queue.queued_bytes == 0,
          "completed reads leaked queued_bytes");

    const std::string payload(64 * 1024, 'x');
    const auto frame = Resp({"SET", "k", payload});
    const size_t args_only = std::string("SET").size() + 1 + payload.size();
    Check(SessionQueueLimits::AccountedBytes(frame.size(), {"SET", "k", payload}) == frame.size(),
          "wire frame must be counted once, not added to the payload");
    Check(SessionQueueLimits::AccountedBytes(frame.size(), {"SET", "k", payload}) <
              frame.size() + args_only,
          "payload was charged a second time");

    CommandBuffer first;
    std::string chunk;
    while (chunk.size() + frame.size() <= CommandBuffer::kMaxBufferedBytes)
        chunk += frame;
    Check(!chunk.empty() && first.Append(chunk), "first wire chunk rejected");
    SessionCommandQueue queue;
    const auto drained = DrainCommands(
        first, queue, SessionQueueLimits::kMaxCommandsPerTurn, true);
    Check(!drained.invalid && drained.enqueued > 0 && first.UnreadBytes() == 0,
          "first wire chunk did not drain");
    Check(queue.queued_bytes == drained.enqueued * frame.size(),
          "queued_bytes is not one wire frame per admitted command");
    Check(!queue.IsFull(), "one input buffer was double-counted past 4MiB");

    const auto before = queue.queued_bytes;
    CommandBuffer extra;
    Check(extra.Append(frame + frame), "crossing frames rejected");
    const auto crossed = DrainCommands(
        extra, queue, SessionQueueLimits::kMaxCommandsPerTurn, true);
    Check(crossed.enqueued == 1 && crossed.stop_read && !crossed.invalid,
          "byte cap did not admit exactly the crossing command");
    Check(extra.UnreadBytes() == frame.size(),
          "drain consumed the frame past the byte cap");
    Check(queue.IsFull() && queue.queued_bytes == before + frame.size(),
          "crossing command was not counted once");
    auto first_cmd = queue.PopFront();
    Check(first_cmd.bytes == frame.size() &&
          queue.queued_bytes == before + frame.size() - first_cmd.bytes,
          "pop subtracted a different size than the wire frame");
}

static void TestDrainTurnLimitAndStopRead() {
    std::string wire;
    for (int i = 0; i < 129; ++i)
        wire += Resp({"PING"});
    CommandBuffer buffer;
    Check(buffer.Append(wire), "129 PINGs rejected");
    SessionCommandQueue queue;
    const auto first = DrainCommands(buffer, queue, SessionQueueLimits::kMaxCommandsPerTurn, true);
    Check(first.enqueued == 128 && !first.stop_read && !first.invalid,
          "first drain turn did not stop at 128");
    Check(buffer.UnreadBytes() == Resp({"PING"}).size(), "turn limit consumed the 129th command");
    const auto second = DrainCommands(buffer, queue, SessionQueueLimits::kMaxCommandsPerTurn, true);
    Check(second.enqueued == 1 && queue.size() == 129, "leftover command was dropped");

    SessionCommandQueue full;
    for (size_t i = 0; i < SessionQueueLimits::kMaxCommands; ++i)
        Check(full.Enqueue({"PING"}, 14), "fill");
    CommandBuffer extra;
    extra.Append(Resp({"PING"}));
    const auto blocked = DrainCommands(extra, full, SessionQueueLimits::kMaxCommandsPerTurn, true);
    Check(blocked.stop_read && blocked.enqueued == 0 && extra.UnreadBytes() == Resp({"PING"}).size(),
          "full queue consumed more input instead of stopping reads");
}

static void TestInvalidStopsWithoutEnqueue() {
    SessionCommandQueue queue;
    CommandBuffer buffer;
    buffer.Append("*0\r\n" + Resp({"PING"}));
    const auto drained = DrainCommands(buffer, queue, SessionQueueLimits::kMaxCommandsPerTurn, true);
    Check(drained.invalid && drained.stop_read && drained.enqueued == 0 && queue.Empty(),
          "invalid RESP was enqueued or skipped");
    Check(ShouldReplyInvalidFrame(false, false, true),
          "an idle connection should report the broken frame");
    Check(!ShouldReplyInvalidFrame(true, false, true),
          "an in-flight command must not receive the broken-frame error");
    Check(!ShouldReplyInvalidFrame(false, true, true),
          "a write waiting on Raft must not receive the broken-frame error");
    Check(!ShouldReplyInvalidFrame(false, false, false),
          "queued commands must not receive the broken-frame error");
    Check(CanDeliverClientReply(true, false), "connected session can take a reply");
    Check(!CanDeliverClientReply(false, false), "disconnected session must drop the reply");
    Check(!CanDeliverClientReply(true, true), "closing session must drop the reply");
}

int main() {
    try {
        TestClassifier();
        TestCacheShardAndMac();
        TestReadIndexRedirectErrors();
        TestSessionQueueLimits();
        TestErrorDoesNotOvertakeWrite();
        TestMixedPipelineAndLowercase();
        TestQueueStopsAtCommandLimit();
        TestQueueStopsAtByteLimitAndDoesNotLeak();
        TestDrainTurnLimitAndStopRead();
        TestInvalidStopsWithoutEnqueue();
        Check(checks >= 50, "too few assertions");
        std::cout << "PASS: production DrainClient queue (" << checks << " checks)\n";
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "FAIL: " << e.what() << " after " << checks << " checks\n";
        return 1;
    }
}
