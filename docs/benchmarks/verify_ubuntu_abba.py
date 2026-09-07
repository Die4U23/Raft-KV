"""Verify the archived ABBA evidence without extracting or executing its contents."""
import hashlib
import json
import math
import tarfile
from pathlib import Path, PurePosixPath
from statistics import mean


ROOT = Path(__file__).resolve().parent
SUMMARY = ROOT / "ubuntu-2cpu-abba-summary.json"
ARCHIVE = ROOT / "evidence/abba-evidence-1szk2s.tar.gz"
EXPECTED_SHA256 = "ffd6ee7ea83db1e4b8a771be5c399e1e9be782ae9eccc4d2b9abd6c9a2e04088"
FILES = {
    "result.json", "warmup.json", "info-before.json", "info-after.json",
    "vmstat.log", "threads.log", "benchmark.log", "warmup.log",
    "node-0.log", "node-1.log", "node-2.log",
}


def require(condition, label):
    if not condition:
        raise ValueError(label)


def verify():
    archive_bytes = ARCHIVE.read_bytes()
    digest = hashlib.sha256(archive_bytes).hexdigest()
    require(digest == EXPECTED_SHA256, "Archive SHA256 mismatch")
    summary = json.loads(SUMMARY.read_text(encoding="utf-8"))
    runs = summary["runs"]
    require([r["async_apply"] for r in runs] == [True, False, False, True], "Run order")
    names = [PurePosixPath(r["vm_report_directory"]).name for r in runs]
    expected_files = {f"{name}/{filename}" for name in names for filename in FILES}
    blobs = {}
    manifest = []
    with tarfile.open(ARCHIVE, "r:gz") as archive:
        members = archive.getmembers()
        require(len(members) == 48, "Expected four directories and 44 files")
        require(len({m.name for m in members}) == len(members), "Duplicate members")
        for member in members:
            path = PurePosixPath(member.name)
            require(not path.is_absolute() and ".." not in path.parts
                    and "\\" not in member.name, "Unexpected member path")
            if member.isdir():
                require(member.name.rstrip("/") in names, "Unexpected directory")
                continue
            require(member.isfile() and member.name in expected_files,
                    "Unexpected member type or filename")
            require(0 <= member.size <= 2_000_000, "Unexpected member size")
            with archive.extractfile(member) as stream:
                payload = stream.read()
            blobs[member.name] = payload
            manifest.append({"path": member.name, "bytes": len(payload),
                             "sha256": hashlib.sha256(payload).hexdigest()})
    require(set(blobs) == expected_files, "Missing evidence files")
    reports = []
    run_ids = set()
    for run, name in zip(runs, names):
        def read(filename):
            return json.loads(blobs[f"{name}/{filename}"])

        raw = read("result.json")
        warmup = read("warmup.json")
        before = {int(s["node_id"]): s for s in read("info-before.json")}
        after = {int(s["node_id"]): s for s in read("info-after.json")}
        require(set(before) == set(after) == {0, 1, 2}, f"{name}: node identities")
        require(raw["run_id"] not in run_ids, "Duplicate run ID")
        run_ids.add(raw["run_id"])
        for key in ("status", "success", "errors", "elapsed_seconds",
                    "goodput_ops_per_second", "batch_latency_ms"):
            require(raw[key] == run[key], f"{name}: {key} transcript mismatch")
        for result, count in ((raw, 100000), (warmup, 10000)):
            require(result["status"] == "COMPLETED" and result["errors"] == 0,
                    f"{name}: unsuccessful benchmark")
            for key in ("success", "attempted", "requested", "completed_batches",
                        "latency_sample_count"):
                require(result[key] == count, f"{name}: {key} request count")
            require(all(v == 0 for v in result["errors_by_category"].values()),
                    f"{name}: error categories")
            require(result["reads_attempted"] == result["writes_attempted"] == count // 2,
                    f"{name}: operation mix")
            require(math.isclose(result["success"] / result["elapsed_seconds"],
                                 result["goodput_ops_per_second"], rel_tol=1e-12),
                    f"{name}: throughput arithmetic")
            config = result["configuration"]
            expected = {"host": "127.0.0.1", "connections": 32, "pipeline": 1,
                        "requests": count, "value_size": 128, "write_ratio": 0.5,
                        "timeout": 5.0, "namespace": "bench"}
            require(all(config[k] == v for k, v in expected.items()), f"{name}: workload")
            require(result["effective_connections"] == 32, f"{name}: connections")
            require(int(result["server_info_before"]["async_apply"]) == int(run["async_apply"]),
                    f"{name}: benchmark target mode")
        require(raw["configuration"]["port"] == warmup["configuration"]["port"],
                f"{name}: warmup target")
        for a, b in (("result.json", "benchmark.log"), ("warmup.json", "warmup.log")):
            require(blobs[f"{name}/{a}"] == blobs[f"{name}/{b}"], f"{name}: stdout mismatch")
        states = []
        for node in range(3):
            a, b = before[node], after[node]
            for info in (a, b):
                require(int(info["async_apply"]) == int(run["async_apply"]), f"{name}: node mode")
                require(info["leader_id"] == run["leader_id"], f"{name}: leader agreement")
                require(info["state"] == ("leader" if node == run["leader_id"] else "follower"),
                        f"{name}: role")
                require(info["commit_index"] == info["last_applied"], f"{name}: endpoint apply")
                require(all(int(info[k]) == 0 for k in ("apply_lag", "apply_inflight",
                        "pending_proposals", "queued_writes", "overload_rejections")),
                        f"{name}: endpoint backlog")
            require(a["term"] == b["term"] == before[run["leader_id"]]["term"], f"{name}: term")
            require(b["commit_index"] - a["commit_index"] == 50032, f"{name}: committed entries")
            retries = int(b["replication_retry_attempts"]) - int(a["replication_retry_attempts"])
            require(retries == 0, f"{name}: replication retry delta")
            states.append({"node_id": node, "term_before": a["term"], "term_after": b["term"],
                           "commit_index_after": b["commit_index"],
                           "last_applied_after": b["last_applied"],
                           "replication_retries_before": int(a["replication_retry_attempts"]),
                           "replication_retry_delta": retries})
        a, b = before[run["leader_id"]], after[run["leader_id"]]
        for key, stage in run["leader_stage_delta"].items():
            count = int(b[key + "_count"]) - int(a[key + "_count"])
            total = int(b[key + "_total_us"]) - int(a[key + "_total_us"])
            require(count == stage["count"] and count > 0 and total >= 0,
                    f"{name}: stage count/total")
            require(round(total / count, 1) == stage["mean_us"], f"{name}: stage mean")
        rows = []
        for line in blobs[f"{name}/vmstat.log"].decode().splitlines():
            parts = line.split()
            if len(parts) >= 17 and all(v.isdigit() for v in parts[:17]):
                rows.append(list(map(int, parts[:17])))
        cpu = {"samples": len(rows),
               "samples_idle_below_10_percent": sum(r[14] < 10 for r in rows),
               "samples_with_swap_io": sum(r[6] > 0 or r[7] > 0 for r in rows)}
        for key, index in (("user_percent_mean", 12), ("system_percent_mean", 13),
                           ("idle_percent_mean", 14), ("iowait_percent_mean", 15),
                           ("runnable_mean", 0)):
            cpu[key] = round(mean(r[index] for r in rows), 2)
        require(cpu == run["cpu"], f"{name}: CPU transcript mismatch")
        require(blobs[f"{name}/threads.log"].count(b"top - ") >= 3, f"{name}: thread samples")
        reports.append({"order": run["order"], "directory": name, "run_id": raw["run_id"],
                        "status": "PASS", "endpoint_state": states})
    combined = {}
    for mode, label in ((False, "sync"), (True, "async")):
        selected = [r for r in runs if r["async_apply"] == mode]
        combined[label] = sum(r["success"] for r in selected) / sum(r["elapsed_seconds"] for r in selected)
    return {"status": "PASS", "verification_scope": "Archived files versus transcript; not a live server rerun",
            "archive_sha256": digest, "archive_bytes": len(archive_bytes),
            "file_count": len(manifest), "file_manifest": sorted(manifest, key=lambda m: m["path"]),
            "runs": reports, "combined_goodput_ops_per_second": combined,
            "async_difference_percent": (combined["async"] / combined["sync"] - 1) * 100}


if __name__ == "__main__":
    print(json.dumps(verify(), ensure_ascii=False, indent=2))
