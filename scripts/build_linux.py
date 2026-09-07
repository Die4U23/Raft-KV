"""Build private Muduo dependencies and the Linux server; record each validation run."""
import argparse
import datetime
import hashlib
import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from prepare_muduo import prepare, sha256


ROOT = Path(__file__).resolve().parents[1]


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def file_hash(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def source_manifest():
    paths = [ROOT / "CMakeLists.txt", ROOT / "third_party/muduo.zip"]
    for directory in ("src", "proto", "tests", "scripts"):
        paths.extend(p for p in (ROOT / directory).rglob("*")
                     if p.is_file() and "__pycache__" not in p.parts and p.suffix != ".pyc")
    return {p.relative_to(ROOT).as_posix(): file_hash(p) for p in sorted(paths)}


def positive(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be positive")
    return number


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-dir", type=Path, default=ROOT / "build-linux-repro")
    parser.add_argument("--jobs", type=positive, default=2)
    parser.add_argument("--prepare-only", action="store_true", help="verify and prepare Muduo; works on Windows")
    parser.add_argument("--smoke", action="store_true", help="run the real three-node smoke test after CTest")
    args = parser.parse_args()
    if args.prepare_only and args.smoke:
        parser.error("--prepare-only and --smoke cannot be combined")
    if not args.prepare_only and platform.system() != "Linux":
        parser.error("Full builds require Linux; use --prepare-only to check Muduo preparation here")
    build = args.build_dir.resolve()
    if build == ROOT or build in ROOT.parents:
        parser.error("Build directory must not be the repository or its ancestor")
    for directory in ("src", "proto", "tests", "scripts", "third_party"):
        protected = ROOT / directory
        if build == protected or protected in build.parents:
            parser.error("Build directory must be outside source and dependency archive directories")
    build.mkdir(parents=True, exist_ok=True)
    reports = build / "reports"
    reports.mkdir(exist_ok=True)
    report_dir = Path(tempfile.mkdtemp(prefix="run-", dir=str(reports)))
    record = {"status": "RUNNING", "started_at": now(), "build_directory": str(build),
              "commands": [], "linux_server_build": "UNRUN", "ctest": "UNRUN", "smoke": "UNRUN"}
    env = dict(os.environ, LC_ALL="C")

    def save():
        (report_dir / "build-report.json").write_text(
            json.dumps(record, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    def run(label, command, cwd=ROOT):
        command = [str(part) for part in command]
        entry = {"step": label, "command": command, "cwd": str(cwd), "started_at": now(), "log": label + ".log"}
        record["commands"].append(entry)
        save()
        print("Running: " + label, flush=True)
        with (report_dir / entry["log"]).open("wb") as output:
            result = subprocess.run(command, cwd=cwd, env=env, stdout=output, stderr=subprocess.STDOUT)
        entry.update(returncode=result.returncode, finished_at=now())
        save()
        if result.returncode:
            raise RuntimeError(label + " failed; see " + str(report_dir / entry["log"]))

    try:
        record["source_manifest_before"] = source_manifest()
        record["source_manifest_sha256"] = sha256(json.dumps(
            record["source_manifest_before"], sort_keys=True).encode("utf-8"))
        if shutil.which("git"):
            run("git-head", ["git", "rev-parse", "HEAD"])
            run("git-status", ["git", "status", "--porcelain=v1"])
            run("tracked-code-changes", ["git", "diff", "--no-ext-diff", "--no-textconv", "HEAD",
                                          "--", "CMakeLists.txt", "src", "proto", "tests", "scripts"])
        else:
            record["git"] = "unavailable; source file hashes recorded"
        source = build / "deps/muduo-source"
        record["muduo"] = prepare(ROOT / "third_party/muduo.zip", source)
        if args.prepare_only:
            record["status"] = "PREPARED"
        else:
            for tool in ("cmake", "ctest", "c++", "protoc"):
                run(tool + "-version", [tool, "--version"])
            record["platform"] = platform.platform()
            record["python"] = sys.version
            if Path("/etc/os-release").exists():
                shutil.copyfile("/etc/os-release", report_dir / "os-release.txt")
            if shutil.which("dpkg-query"):
                run("packages", ["dpkg-query", "-W", "-f=${binary:Package}\t${Version}\t${db:Status-Abbrev}\n",
                                 "libboost*", "librocksdb*", "libprotobuf*", "protobuf-compiler",
                                 "libgoogle-glog*", "libgflags*"])
            deps_build = build / "deps/muduo-build"
            prefix = build / "deps/install"
            run("muduo-configure", ["cmake", "-S", source, "-B", deps_build,
                                    "-DCMAKE_BUILD_TYPE=Release", "-DCMAKE_POLICY_VERSION_MINIMUM=3.5",
                                    "-DMUDUO_BUILD_EXAMPLES=OFF", "-DCMAKE_DISABLE_FIND_PACKAGE_Protobuf=TRUE",
                                    "-DBUILD_SHARED_LIBS=OFF", "-DCMAKE_INSTALL_PREFIX=" + str(prefix)])
            run("muduo-build", ["cmake", "--build", deps_build, "--parallel", args.jobs])
            run("muduo-install", ["cmake", "--install", deps_build])
            server = build / "server"
            run("server-configure", ["cmake", "-S", ROOT, "-B", server,
                                    "-DCMAKE_BUILD_TYPE=RelWithDebInfo", "-DRAFTKV_BUILD_SERVER=ON",
                                    "-DBUILD_TESTING=ON", "-DMUDUO_INCLUDE_DIR=" + str(prefix / "include"),
                                    "-DMUDUO_NET_LIB=" + str(prefix / "lib/libmuduo_net.a"),
                                    "-DMUDUO_BASE_LIB=" + str(prefix / "lib/libmuduo_base.a")])
            run("server-build", ["cmake", "--build", server, "--parallel", args.jobs])
            record["linux_server_build"] = "PASS"
            binary = server / "raft_kv_server"
            record["binary"] = {"path": str(binary), "sha256": file_hash(binary), "recorded_at": now()}
            record["muduo_libraries"] = {p.name: file_hash(p) for p in (
                prefix / "lib/libmuduo_net.a", prefix / "lib/libmuduo_base.a")}
            shutil.copyfile(server / "CMakeCache.txt", report_dir / "server-CMakeCache.txt")
            shutil.copyfile(deps_build / "CMakeCache.txt", report_dir / "muduo-CMakeCache.txt")
            for name in ("flags.make", "link.txt"):
                path = server / "CMakeFiles/raft_kv_server.dir" / name
                if path.exists():
                    shutil.copyfile(path, report_dir / name)
            run("ctest", ["ctest", "--output-on-failure"], cwd=server)
            record["ctest"] = "PASS"
            if args.smoke:
                if file_hash(binary) != record["binary"]["sha256"]:
                    raise RuntimeError("Server binary changed before smoke test")
                record["smoke_binary_sha256"] = file_hash(binary)
                run("smoke", [sys.executable, ROOT / "tests/cluster_smoke.py", "--binary", binary,
                              "--timeout", "180", "--artifacts", report_dir / "cluster"])
                record["smoke"] = "PASS"
            if file_hash(binary) != record["binary"]["sha256"]:
                raise RuntimeError("Server binary changed during validation")
            record["status"] = "PASS"
        if source_manifest() != record["source_manifest_before"]:
            raise RuntimeError("Source files changed during this run; use a new validation run")
        return 0
    except (OSError, ValueError, RuntimeError) as error:
        record.update(status="FAILED", error=str(error))
        print(str(error), file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        record.update(status="INTERRUPTED", error="Interrupted by user")
        return 130
    finally:
        record["finished_at"] = now()
        save()
        print("Status: " + record["status"])
        print("Report: " + str(report_dir / "build-report.json"), flush=True)


if __name__ == "__main__":
    sys.exit(main())
