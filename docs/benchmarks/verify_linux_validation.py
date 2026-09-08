"""Verify this archived Linux workflow run against its recorded Git source revision."""
import hashlib
import io
import json
import subprocess
import tarfile
import zipfile
from datetime import datetime
from pathlib import Path, PurePosixPath

from verify_ubuntu_abba import require
from verify_ubuntu_build import STEPS

ROOT = Path(__file__).resolve().parent
REPO = ROOT.parents[1]
ARCHIVE = ROOT / "evidence/linux-validation-1WQbP1.tar.gz"
EXPECTED_SHA256 = "20841bcfbf9bb69d471728c7e3964bb9122cd84f6d2a365e13388770bc00627a"
COMMIT = "35a348d951b43470d1dd6491a7d6522a5a83af96"
PREFIX = "run-gnt4rq_1/"
CLUSTER = "cluster/run-yg5iv6kc/"
COMMANDS = ["git-head", "git-status", "tracked-code-changes", "cmake-version",
            "ctest-version", "c++-version", "protoc-version", "packages", "muduo-configure",
            "muduo-build", "muduo-install", "server-configure", "server-build", "ctest", "smoke"]


def digest(data):
    return hashlib.sha256(data).hexdigest()


def verify(archive_path=ARCHIVE, expected_sha256=EXPECTED_SHA256, prefix=PREFIX,
           cluster=CLUSTER, initial_leader=1, final_leader=2, final_term=4, fresh=False):
    archive_bytes = archive_path.read_bytes()
    require(digest(archive_bytes) == expected_sha256, "Archive hash mismatch")
    expected = {name + ".log" for name in COMMANDS} | {
        "muduo-CMakeCache.txt", "server-CMakeCache.txt", "flags.make", "link.txt",
        "os-release.txt", "build-report.json", cluster + "report.json",
        *(cluster + "node-{}.log".format(i) for i in range(3))}
    blobs, manifest = {}, []
    with tarfile.open(archive_path, "r:gz") as archive:
        members = archive.getmembers()
        require(len(members) == len({m.name for m in members}) == 28, "Archive inventory")
        for member in members:
            path = PurePosixPath(member.name)
            require(not path.is_absolute() and ".." not in path.parts and "\\" not in member.name,
                    "Unexpected archive path")
            if member.isdir():
                require(member.name.rstrip("/") in {prefix.rstrip("/"), prefix + "cluster",
                        (prefix + cluster).rstrip("/")}, "Unexpected directory")
                continue
            require(member.name.startswith(prefix), "Unexpected archive root")
            name = member.name[len(prefix):]
            require(member.isfile() and name in expected and 0 <= member.size < 2_000_000,
                    "Unexpected file/type/size")
            with archive.extractfile(member) as stream:
                data = stream.read()
            blobs[name] = data
            manifest.append({"path": member.name, "bytes": len(data), "sha256": digest(data)})
    require(set(blobs) == expected, "Missing evidence")
    report = json.loads(blobs["build-report.json"])
    require(all(report[k] == "PASS" for k in ("status", "linux_server_build", "ctest", "smoke")),
            "Workflow result")
    require(blobs["git-head.log"].decode().strip() == COMMIT, "Git revision")
    require(not blobs["tracked-code-changes.log"], "Unexpected tracked modifications")
    commands = report["commands"]
    require([c["step"] for c in commands] == COMMANDS, "Command sequence")
    last = datetime.fromisoformat(report["started_at"])
    for command in commands:
        require(command["returncode"] == 0 and command["log"] == command["step"] + ".log",
                "Failed command or invalid log reference")
        started, finished = (datetime.fromisoformat(command[k]) for k in ("started_at", "finished_at"))
        require(last <= started <= finished, "Command timestamps")
        last = finished
    require(last <= datetime.fromisoformat(report["finished_at"]), "Run timestamps")
    require(report["binary"]["sha256"] == report["smoke_binary_sha256"], "Binary fingerprint mismatch")
    recorded_at = datetime.fromisoformat(report["binary"]["recorded_at"])
    require(datetime.fromisoformat(commands[12]["finished_at"]) <= recorded_at
            <= datetime.fromisoformat(commands[14]["started_at"]), "Binary identity timing")
    smoke_command = commands[-1]["command"]
    require(smoke_command[smoke_command.index("--binary") + 1] == report["binary"]["path"],
            "Smoke binary path")
    require(b"100% tests passed, 0 tests failed out of 5" in blobs["ctest.log"], "CTest result")
    smoke = json.loads(blobs[cluster + "report.json"])
    require(smoke["status"] == "PASS" and smoke["steps"] == STEPS, "Smoke steps")
    require(all(("PASS: " + step).encode() in blobs["smoke.log"] for step in STEPS), "Smoke stdout")
    require(smoke["initial_leader"] == initial_leader and smoke["failover_leader"] == smoke["final_leader"] == final_leader,
            "Smoke leadership")
    require(set(smoke["last_info"]) == {"0", "1", "2"}, "Smoke snapshot inventory")
    for node in range(3):
        info = smoke["last_info"][str(node)]
        require(info["node_id"] == node and info["leader_id"] == final_leader and info["term"] == final_term,
                "Smoke node/term")
        require(info["state"] == ("leader" if node == final_leader else "follower") and int(info["async_apply"]) == 1,
                "Smoke mode/role")
        require(info["commit_index"] == info["last_applied"] == 42, "Smoke convergence")
        require(int(info["apply_lag"]) == int(info["apply_inflight"]) == 0, "Smoke final backlog")
    require(len(smoke["nodes"]) == 3 and {n["id"] for n in smoke["nodes"]} == {0, 1, 2}, "Smoke processes")
    for node in smoke["nodes"]:
        starts = node["starts"]
        require([s["exit_code"] for s in starts] == ([-9, -15] if node["id"] == initial_leader else [-15]),
                "Smoke process lifecycle")
        require(all(s["command"][0] == report["binary"]["path"] for s in starts), "Node binary")
        if node["id"] == initial_leader:
            require(starts[0]["command"] == starts[1]["command"], "Restart paths")
    files = report["source_manifest_before"]
    require(digest(json.dumps(files, sort_keys=True).encode()) == report["source_manifest_sha256"],
            "Source manifest hash")
    tracked = subprocess.check_output(["git", "ls-tree", "-r", "--name-only", COMMIT], cwd=REPO).decode().splitlines()
    tracked = [p for p in tracked if p in ("CMakeLists.txt", "third_party/muduo.zip")
               or p.startswith(("src/", "proto/", "tests/", "scripts/"))]
    extra = set(files) - set(tracked)
    require(set(tracked) <= set(files) and extra == {"src/server/main.cpp.bak"}, "Source inventory")
    output = subprocess.check_output(["git", "cat-file", "--batch"], cwd=REPO,
                                     input="".join(COMMIT + ":" + p + "\n" for p in tracked).encode())
    stream, source_blobs = io.BytesIO(output), {}
    for path in tracked:
        header = stream.readline().split()
        require(len(header) == 3 and header[1] == b"blob", "Missing Git object")
        data = stream.read(int(header[2]))
        require(stream.read(1) == b"\n" and digest(data) == files[path], "Source differs: " + path)
        source_blobs[path] = data
    muduo_zip = source_blobs["third_party/muduo.zip"]
    require(digest(muduo_zip) == report["muduo"]["archive_sha256"], "Muduo archive identity")
    prepared = {}
    with zipfile.ZipFile(io.BytesIO(muduo_zip)) as archive:
        for name in archive.namelist():
            if name.endswith("/"):
                continue
            path, data = name[len("muduo-master/"):], archive.read(name)
            if path == "muduo/net/http/HttpResponse.cc":
                data = data.replace(b"char buf[32];", b"char buf[64];").replace(b"Content-Length: %zd", b"Content-Length: %zu")
            prepared[path] = digest(data)
    require(prepared == report["muduo"]["source_manifest"], "Prepared Muduo source differs")
    install_prefix = report["build_directory"] + "/deps/install/"
    for library in ("libmuduo_net.a", "libmuduo_base.a"):
        require((install_prefix + "lib/" + library).encode() in blobs["link.txt"], "Private Muduo link")
    if fresh:
        require(blobs["muduo-build.log"].count(b"Building CXX object") == 41
                and blobs["muduo-build.log"].count(b"Linking CXX static library") == 4,
                "Muduo full compile/link evidence")
        require(blobs["server-build.log"].count(b"Building CXX object") == 23
                and blobs["server-build.log"].count(b"Linking CXX executable") == 6
                and b"Running cpp protocol buffer compiler" in blobs["server-build.log"],
                "Server full compile/link evidence")
        require(b"Linking CXX executable raft_kv_server" in blobs["server-build.log"], "Server link evidence")
    scope = ("Archived fresh-directory Linux build and tests; not a clean OS or a local live rerun" if fresh else
             "Archived incremental Linux workflow validation; not a clean rebuild or a local live rerun")
    result = {"status": "PASS", "scope": scope,
            "archive_sha256": expected_sha256, "archive_bytes": len(archive_bytes), "file_count": len(blobs),
            "file_manifest": sorted(manifest, key=lambda m: m["path"]), "source_commit": COMMIT,
            "matched_tracked_source_files": len(tracked), "matched_prepared_muduo_files": len(prepared),
            "extra_source_hashes_not_verified_from_content": {p: files[p] for p in sorted(extra)},
            "binary_sha256_recorded_at_test_time": report["binary"]["sha256"], "binary_in_archive": False,
            "ctest_passed": 5, "smoke_steps_passed": 10, "smoke_final_term": final_term,
            "smoke_final_commit_and_applied_index": 42, "platform": report["platform"],
            "started_at": report["started_at"], "finished_at": report["finished_at"],
            "clean_build_proven": False}
    if fresh:
        del result["clean_build_proven"]
        result.update(fresh_directory_build_verified=True, clean_os_reproduction_proven=False,
                      muduo_compilation_units=41, server_and_test_compilation_units=23)
    return result


if __name__ == "__main__":
    print(json.dumps(verify(), ensure_ascii=False, indent=2))
