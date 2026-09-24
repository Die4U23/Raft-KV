// Production KVStateMachine semantics. Batching is covered by storage_batch_tests;
// these fail if Apply/Get/DEL replies, lastApplied, or command validation change.
#include "raft/kv_state_machine.h"
#include "common/resp_parser.h"
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

static int checks = 0;
static void Check(bool ok, const char* message) {
    ++checks;
    if (!ok) throw std::runtime_error(message);
}
template<class F> static void Throws(F call) {
    bool threw = false;
    try { call(); } catch (const std::runtime_error&) { threw = true; }
    Check(threw, "expected state-machine failure");
}
static std::string Command(std::initializer_list<std::string> args) {
    std::string out = "*" + std::to_string(args.size()) + "\r\n";
    for (const auto& arg : args)
        out += "$" + std::to_string(arg.size()) + "\r\n" + arg + "\r\n";
    return out;
}

static void SetGetOverwriteAndDelete() {
    KVStateMachine machine("kv-sm/basic");
    Check(machine.LastApplied() == 0, "empty store must start at lastApplied 0");
    std::string value;
    Check(!machine.Get("default:k", &value), "missing key reported present");
    Check(machine.Apply(1, Command({"SET", "default:k", "one"})) == "+OK\r\n", "SET reply");
    Check(machine.LastApplied() == 1 && machine.Get("default:k", &value) && value == "one",
          "SET did not persist");
    Check(machine.Apply(2, Command({"SET", "default:k", "two"})) == "+OK\r\n", "overwrite reply");
    Check(machine.Get("default:k", &value) && value == "two" && machine.LastApplied() == 2,
          "overwrite kept the old value");
    Check(machine.Apply(3, Command({"DEL", "default:k"})) == ":1\r\n", "DEL existing");
    Check(!machine.Get("default:k", &value) && machine.LastApplied() == 3,
          "DEL left the key in the store");
    Check(machine.Apply(4, Command({"DEL", "default:k"})) == ":0\r\n", "DEL missing");
    Check(machine.LastApplied() == 4, "missing DEL did not advance lastApplied");
}

static void EmptyNoopAndCaseFold() {
    KVStateMachine machine("kv-sm/noop");
    Check(machine.Apply(1, "") == "+OK\r\n" && machine.LastApplied() == 1,
          "empty committed command is not an internal no-op");
    std::string value;
    Check(!machine.Get("", &value), "no-op invented a key");
    Check(machine.Apply(2, Command({"set", "default:k", "v"})) == "+OK\r\n",
          "committed lowercase SET rejected");
    Check(machine.Get("default:k", &value) && value == "v", "lowercase SET did not write");
    Check(machine.Apply(3, Command({"del", "default:k"})) == ":1\r\n",
          "committed lowercase DEL rejected");
    Check(!machine.Get("default:k", &value), "lowercase DEL did not delete");
}

static void BinaryAndNamespacedKeys() {
    KVStateMachine machine("kv-sm/binary");
    const std::string key("tenant:a\0b\r\n", 12);
    const std::string payload("x\0y\r\n", 5);
    Check(machine.Apply(1, Command({"SET", key, payload})) == "+OK\r\n", "binary SET");
    std::string value;
    Check(machine.Get(key, &value) && value == payload, "binary GET mismatch");
    Check(!machine.Get("tenant:a", &value), "namespace prefix leaked");
    Check(machine.Apply(2, Command({"DEL", key})) == ":1\r\n", "binary DEL");
}

static void RejectUnsupportedCommittedCommands() {
    KVStateMachine machine("kv-sm/invalid");
    machine.Apply(1, Command({"SET", "default:k", "v"}));
    const auto applied = machine.LastApplied();
    std::string value;
    Throws([&] { machine.Apply(2, Command({"GET", "default:k"})); });
    Throws([&] { machine.Apply(2, Command({"PING"})); });
    Throws([&] { machine.Apply(2, Command({"BOGUS", "default:k"})); });
    Throws([&] { machine.Apply(2, Command({"SET", "default:k"})); });
    Throws([&] { machine.Apply(2, "*2\r\n$3\r\nSET\r\n$1\r\nk"); });
    Throws([&] { machine.Apply(2, Command({"SET", "default:k", "v"}) + "x"); });
    Throws([&] { machine.Apply(3, Command({"SET", "default:skip", "x"})); });
    Check(machine.LastApplied() == applied && machine.Get("default:k", &value) && value == "v",
          "invalid command mutated lastApplied or existing keys");
}

static void ApplyMatchesApplyBatchAndReopen() {
    KVStateMachine machine("kv-sm/reopen");
    const auto single = machine.Apply(1, Command({"SET", "default:a", "1"}));
    const auto batch = machine.ApplyBatch(2, {Command({"SET", "default:b", "2"})});
    Check(single == "+OK\r\n" && batch.size() == 1 && batch[0] == "+OK\r\n",
          "Apply and ApplyBatch replies diverged");
    Check(machine.LastApplied() == 2, "two applies did not land at index 2");
    KVStateMachine reopened("kv-sm/reopen");
    std::string a, b;
    Check(reopened.LastApplied() == 2 && reopened.Get("default:a", &a) && a == "1" &&
          reopened.Get("default:b", &b) && b == "2",
          "reopened machine lost applied keys");
}

static std::string SnapshotWithKey(const std::string& key, const std::string& value) {
    std::string out(1, '\x01');
    auto append_u32 = [&](uint32_t n) {
        for (int i = 3; i >= 0; --i) out.push_back(static_cast<char>((n >> (8 * i)) & 0xff));
    };
    append_u32(1);
    append_u32(static_cast<uint32_t>(key.size()));
    out += key;
    append_u32(static_cast<uint32_t>(value.size()));
    out += value;
    return out;
}

static void SnapshotExportInstallRoundTrip() {
    KVStateMachine source("kv-sm/snap-src");
    Check(source.Apply(1, Command({"SET", "default:a", "1"})) == "+OK\r\n", "snapshot seed");
    const std::string binary("x\0y", 3);
    Check(source.Apply(2, Command({"SET", std::string("default:b\0c", 11), binary})) == "+OK\r\n",
          "binary snapshot seed");
    Check(source.Apply(3, Command({"DEL", "default:a"})) == ":1\r\n", "snapshot delete");
    std::string blob;
    Check(source.TryExportSnapshot(&blob) && source.IsSnapshot(blob) && blob.size() >= 9 &&
          blob[0] == 2, "export did not produce a version-2 snapshot");
    KVStateMachine empty("kv-sm/snap-empty");
    std::string empty_blob;
    Check(empty.TryExportSnapshot(&empty_blob) && empty_blob.size() == 9 &&
          empty.IsSnapshot(empty_blob), "empty snapshot is not version 2 with zero sessions");

    KVStateMachine restored("kv-sm/snap-dst");
    Check(restored.Apply(1, Command({"SET", "default:stale", "x"})) == "+OK\r\n", "stale seed");
    Check(restored.TryInstallSnapshot(3, blob), "install rejected a snapshot just exported");
    std::string value;
    Check(restored.LastApplied() == 3 && !restored.Get("default:stale", &value) &&
          !restored.Get("default:a", &value) &&
          restored.Get(std::string("default:b\0c", 11), &value) && value == binary,
          "install did not replace user keys or dropped a binary value");
    KVStateMachine reopened("kv-sm/snap-dst");
    Check(reopened.LastApplied() == 3 && reopened.Get(std::string("default:b\0c", 11), &value) &&
          value == binary, "reopened store lost the installed snapshot");

    Check(!restored.IsSnapshot("junk") && !restored.IsSnapshot(empty_blob + "x") &&
          !restored.TryInstallSnapshot(4, "junk"), "malformed snapshot was accepted");
    Check(restored.LastApplied() == 3, "malformed snapshot advanced lastApplied");
    Throws([&] { restored.InstallSnapshot(4, "junk"); });
    const auto reserved = SnapshotWithKey(std::string("\0secret", 7), "v");
    Check(!restored.IsSnapshot(reserved) && !restored.TryInstallSnapshot(4, reserved),
          "reserved key was accepted in a snapshot");
    Throws([&] { restored.TryInstallSnapshot(2, blob); });
    Throws([&] { restored.TryInstallSnapshot(0, empty_blob); });
    Check(restored.LastApplied() == 3 && restored.Get(std::string("default:b\0c", 11), &value) &&
          value == binary, "rejected snapshot rewound applied state");
}

static void IdempotentRetryDoesNotApplyTwice() {
    KVStateMachine plain("kv-sm/plain-del");
    Check(plain.Apply(1, Command({"SET", "default:k", "v"})) == "+OK\r\n", "plain SET");
    Check(plain.Apply(2, Command({"DEL", "default:k"})) == ":1\r\n", "plain DEL existing");
    Check(plain.Apply(3, Command({"DEL", "default:k"})) == ":0\r\n",
          "plain DEL without a request id ran a second time");

    KVStateMachine machine("kv-sm/dedup");
    Check(machine.Apply(1, Command({"SET", "default:k", "v"})) == "+OK\r\n", "seed SET");
    Check(machine.Apply(2, "") == "+OK\r\n", "no-op before the idempotent DEL");
    Check(machine.Apply(3, Command({"SET", "default:k", "v"})) == "+OK\r\n", "plain SET");
    Check(machine.Apply(4, Command({"DEL", "default:k", "app", "1"})) == ":1\r\n",
          "idempotent DEL missed the key");
    Check(machine.Apply(5, Command({"DEL", "default:k", "other", "1"})) == ":0\r\n",
          "a second client shared the first session");
    std::string value;
    Check(!machine.Get("default:k", &value), "DEL left the key");
    Check(machine.Apply(6, Command({"DEL", "default:k", "app", "1"})) == ":1\r\n",
          "retry did not replay the original DEL reply");
    Check(machine.Apply(7, Command({"SET", "default:k", "again", "app", "1"})) == ":1\r\n" &&
          !machine.Get("default:k", &value),
          "same request id with a different command was executed");
    Check(machine.Apply(8, Command({"SET", "default:k", "v2", "app", "3"})) == "-ERR stale request id\r\n" &&
          !machine.Get("default:k", &value) && machine.LastApplied() == 8,
          "a gap wrote the key or skipped the index");
    Check(machine.Apply(9, Command({"SET", "default:k", "v2", "app", "2"})) == "+OK\r\n" &&
          machine.Get("default:k", &value) && value == "v2",
          "the next request id did not apply");
    const auto batch = machine.ApplyBatch(10, {
        Command({"SET", "default:k", "batch", "app", "3"}),
        Command({"SET", "default:k", "ignored", "app", "3"}),
        Command({"DEL", "default:k", "app", "2"}),
    });
    Check(batch.size() == 3 && batch[0] == "+OK\r\n" && batch[1] == "+OK\r\n" &&
          batch[2] == "-ERR stale request id\r\n" &&
          machine.Get("default:k", &value) && value == "batch",
          "same-batch duplicate or stale request changed the key");
    Check(machine.Apply(13, Command({"set", "default:k", "lower", "app", "4"})) == "+OK\r\n" &&
          machine.Get("default:k", &value) && value == "lower",
          "lowercase idempotent SET was rejected");
    Throws([&] { machine.Apply(14, Command({"SET", "default:k", "v", "app", "01"})); });
    Throws([&] { machine.Apply(14, Command({"SET", "default:k", "v", std::string("a\0b", 3), "1"})); });
    Check(machine.LastApplied() == 13 && machine.Get("default:k", &value) && value == "lower",
          "invalid request id mutated the store");

    KVStateMachine reopened("kv-sm/dedup");
    Check(reopened.Apply(14, Command({"SET", "default:k", "nope", "app", "4"})) == "+OK\r\n" &&
          reopened.Get("default:k", &value) && value == "lower",
          "reopened session executed the retry");

    std::string blob;
    Check(reopened.TryExportSnapshot(&blob) && reopened.IsSnapshot(blob), "session export failed");
    KVStateMachine restored("kv-sm/dedup-dst");
    Check(restored.Apply(1, Command({"SET", "default:other", "x", "stranger", "1"})) == "+OK\r\n",
          "pre-install session");
    Check(restored.TryInstallSnapshot(14, blob), "session snapshot rejected");
    Check(!restored.Get("default:other", &value) && restored.Get("default:k", &value) &&
          value == "lower", "install did not replace keys");
    Check(restored.Apply(15, Command({"SET", "default:k", "nope", "app", "4"})) == "+OK\r\n" &&
          restored.Get("default:k", &value) && value == "lower",
          "installed session executed the retry");
    Check(restored.Apply(16, Command({"SET", "default:other", "y", "stranger", "1"})) == "+OK\r\n" &&
          restored.Get("default:other", &value) && value == "y",
          "install kept a session that was not in the snapshot");

    KVStateMachine legacy("kv-sm/dedup-v1");
    Check(legacy.Apply(1, Command({"SET", "default:k", "v", "c", "1"})) == "+OK\r\n", "v1 seed");
    Check(legacy.TryInstallSnapshot(2, SnapshotWithKey("default:n", "1")), "version 1 snapshot rejected");
    Check(legacy.Get("default:n", &value) && value == "1" && !legacy.Get("default:k", &value),
          "version 1 snapshot did not replace user keys");
    Check(legacy.Apply(3, Command({"SET", "default:k", "again", "c", "1"})) == "+OK\r\n" &&
          legacy.Get("default:k", &value) && value == "again",
          "version 1 snapshot kept a session and blocked the same request id");
}

static void LegacyStoreWithoutMarkerIsRejected() {
    const std::string path = "kv-sm/legacy";
    auto state = rocksdb::testing::StateFor(path);
    state->data["default:old"] = "value";
    Throws([&] { KVStateMachine machine(path); });
}

int main() {
    try {
        SetGetOverwriteAndDelete();
        EmptyNoopAndCaseFold();
        BinaryAndNamespacedKeys();
        RejectUnsupportedCommittedCommands();
        ApplyMatchesApplyBatchAndReopen();
        SnapshotExportInstallRoundTrip();
        IdempotentRetryDoesNotApplyTwice();
        LegacyStoreWithoutMarkerIsRejected();
        Check(checks >= 20, "too few KV assertions");
        std::cout << "PASS: production KVStateMachine (" << checks << " checks)\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL: " << error.what() << '\n';
        return 1;
    }
}
