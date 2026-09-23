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
        LegacyStoreWithoutMarkerIsRejected();
        Check(checks >= 20, "too few KV assertions");
        std::cout << "PASS: production KVStateMachine (" << checks << " checks)\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL: " << error.what() << '\n';
        return 1;
    }
}
