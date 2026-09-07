"""Check the supplied smoke/build archive; never execute its commands or patches."""
import hashlib
import json
import re
import tarfile
from pathlib import Path, PurePosixPath

from verify_ubuntu_abba import require


ROOT = Path(__file__).resolve().parent
ARCHIVE = ROOT / "evidence/build-evidence-jQOIcr.tar.gz"
EXPECTED_SHA256 = "c0e5217b5e0e2bc6aede875ff2c4759b1b023b65a6f1a7a11b7c361570601f3b"
PREFIX = "build-evidence-jQOIcr/"
BUILD_FILES = {
    "python-version.txt", "packages.txt", "protobuf-version.txt",
    "project-CMakeLists.txt", "kernel.txt", "git-status.txt", "cpu.txt",
    "server-sha256.txt", "flags.make", "muduo-HttpResponse.cc", "CMakeCache.txt",
    "os-release.txt", "provenance.txt", "cmake-version.txt", "packages-query.log",
    "compiler-version.txt", "server-stat.txt", "link.txt", "collected-at.txt",
    "tracked-code-changes.patch", "git-head.txt",
}
STEPS = [
    "three nodes agree on one leader",
    "fragmented PING and binary SET/GET pipeline",
    "pipeline spans multiple 128-command event-loop turns",
    "namespace isolation and switching on one persistent connection",
    "DEL absent=0, present=1, then absent=0",
    "follower rejects writes with a RESP error",
    "32 concurrent connections complete ordered writes and reads",
    "leader killed abruptly",
    "survivors elect a new leader and acknowledge a new write",
    "old leader restarts from paired directories; all three nodes converge",
]


def verify_smoke(report):
    require(report["status"] == "PASS" and report["steps"] == STEPS, "Smoke steps/status")
    require(report["initial_leader"] == 0 and report["failover_leader"] == report["final_leader"] == 2,
            "Smoke leader transition")
    require(report["minimum_applied_index"] == 42, "Smoke applied index")
    require(set(report["last_info"]) == {"0", "1", "2"}, "Smoke node snapshots")
    for node in range(3):
        info = report["last_info"][str(node)]
        require(info["node_id"] == node and info["leader_id"] == 2 and info["term"] == 3,
                "Smoke node identity/term")
        require(info["state"] == ("leader" if node == 2 else "follower"), "Smoke role")
        require(info["commit_index"] == info["last_applied"] == 42, "Smoke convergence")
        require(int(info["apply_lag"]) == int(info["apply_inflight"]) == 0,
                "Smoke endpoint backlog")
        require(int(info["async_apply"]) == 1, "Smoke async mode")
    nodes = {n["id"]: n for n in report["nodes"]}
    require(set(nodes) == {0, 1, 2} and len(report["nodes"]) == 3, "Smoke process inventory")
    require([s["exit_code"] for s in nodes[0]["starts"]] == [-9, -15], "Leader kill/restart exits")
    for node in (1, 2):
        require([s["exit_code"] for s in nodes[node]["starts"]] == [-15], "Cleanup exits")
    require(nodes[0]["starts"][0]["command"] == nodes[0]["starts"][1]["command"],
            "Restart must reuse paired paths and arguments")
    return {"status": "PASS", "step_count": len(STEPS), "initial_leader": 0,
            "final_leader": 2, "final_term": 3, "final_commit_and_applied_index": 42,
            "elapsed_seconds": report["elapsed_seconds"],
            "elapsed_semantics": "Entire smoke test, not failover duration",
            "expected_exit_codes": {"0": [-9, -15], "1": [-15], "2": [-15]}}


def verify():
    digest = hashlib.sha256(ARCHIVE.read_bytes()).hexdigest()
    require(digest == EXPECTED_SHA256, "Build archive SHA256 mismatch")
    expected = {"build/" + n for n in BUILD_FILES} | {
        "smoke/report.json", "smoke/node-0.log", "smoke/node-1.log", "smoke/node-2.log"}
    blobs = {}
    manifest = []
    with tarfile.open(ARCHIVE, "r:gz") as archive:
        members = archive.getmembers()
        require(len(members) == 28 and len({m.name for m in members}) == 28, "Member inventory")
        for member in members:
            path = PurePosixPath(member.name)
            require(not path.is_absolute() and ".." not in path.parts and "\\" not in member.name,
                    "Unsafe archive path")
            if member.isdir():
                require(member.name.rstrip("/") in {PREFIX.rstrip("/"), PREFIX + "build", PREFIX + "smoke"},
                        "Unexpected directory")
                continue
            require(member.name.startswith(PREFIX), "Unexpected root")
            name = member.name[len(PREFIX):]
            require(member.isfile() and name in expected and 0 <= member.size <= 2_000_000,
                    "Unexpected archive member")
            with archive.extractfile(member) as stream:
                payload = stream.read()
            blobs[name] = payload
            manifest.append({"path": member.name, "bytes": len(payload),
                             "sha256": hashlib.sha256(payload).hexdigest()})
    require(set(blobs) == expected, "Missing evidence file")
    def text(name):
        return blobs["build/" + name].decode("utf-8").replace("\r\n", "\n")
    smoke = verify_smoke(json.loads(blobs["smoke/report.json"]))
    sha = text("git-head.txt").strip()
    require(sha == "64f60a783bb9de0bfd3bf5b5068bbb7d79fe3ee0", "Unexpected current Git HEAD")
    binary_sha = text("server-sha256.txt").split()[0]
    require(re.fullmatch(r"[0-9a-f]{64}", binary_sha) is not None, "Malformed recorded binary hash")
    cache = text("CMakeCache.txt")
    require("CMAKE_BUILD_TYPE:STRING=RelWithDebInfo\n" in cache, "Build type")
    require("CXX_FLAGS = -O2 -g -DNDEBUG -std=gnu++17" in text("flags.make"), "Compiler flags")
    require("find_package(Boost COMPONENTS thread REQUIRED)" in text("project-CMakeLists.txt"),
            "Boost compatibility adjustment")
    require("char buf[64];" in text("muduo-HttpResponse.cc")
            and "Content-Length: %zu" in text("muduo-HttpResponse.cc"), "Muduo adjustment")
    require("Post-test collection" in text("provenance.txt"), "Collection provenance")
    packages = {}
    for line in text("packages.txt").splitlines():
        parts = line.split("\t")
        if len(parts) == 3 and parts[2].strip() == "ii":
            packages[parts[0]] = parts[1]
    require(packages.get("librocksdb-dev") == "9.11.2-1", "RocksDB package")
    return {"status": "PASS", "scope": "Original smoke report consistency and post-test build records; no live rerun",
            "archive_sha256": digest, "archive_bytes": ARCHIVE.stat().st_size,
            "file_count": len(manifest), "file_manifest": sorted(manifest, key=lambda m: m["path"]),
            "smoke": smoke, "build": {
                "collected_at": text("collected-at.txt").strip(), "current_git_head": sha,
                "current_git_status": text("git-status.txt").splitlines(),
                "recorded_current_binary_sha256": binary_sha, "binary_in_archive": False,
                "test_time_binary_identity_proven": False, "build_type": "RelWithDebInfo",
                "cxx_flags": "-O2 -g -DNDEBUG -std=gnu++17",
                "compiler": text("compiler-version.txt").splitlines()[0],
                "cmake": text("cmake-version.txt").splitlines()[0],
                "protoc": text("protobuf-version.txt").strip(),
                "installed_packages": packages}}


if __name__ == "__main__":
    print(json.dumps(verify(), ensure_ascii=False, indent=2))
