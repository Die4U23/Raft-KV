"""Portable checks for the real pinned archive and source-preserving preparation."""
import hashlib
import json
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import prepare_muduo
import build_linux


class BuildWorkflowTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.destination = Path(self.temporary.name) / "muduo"
        self.archive = ROOT / "third_party/muduo.zip"

    def test_real_patch_matches_previously_tested_vm_file_and_is_repeatable(self):
        before = hashlib.sha256(self.archive.read_bytes()).hexdigest()
        result = prepare_muduo.prepare(self.archive, self.destination)
        target = self.destination / prepare_muduo.HTTP_SOURCE
        with tarfile.open(ROOT / "docs/benchmarks/evidence/build-evidence-jQOIcr.tar.gz") as evidence:
            original = evidence.extractfile("build-evidence-jQOIcr/build/muduo-HttpResponse.cc").read()
        self.assertEqual(target.read_bytes(), original)
        modified = target.stat().st_mtime_ns
        self.assertEqual(prepare_muduo.prepare(self.archive, self.destination), result)
        self.assertEqual(target.stat().st_mtime_ns, modified)
        self.assertEqual(hashlib.sha256(self.archive.read_bytes()).hexdigest(), before)

    def test_existing_local_edit_is_preserved(self):
        prepare_muduo.prepare(self.archive, self.destination)
        target = self.destination / prepare_muduo.HTTP_SOURCE
        target.write_bytes(b"local edit that must not be overwritten\n")
        with self.assertRaisesRegex(ValueError, "Prepared source changed"):
            prepare_muduo.prepare(self.archive, self.destination)
        self.assertEqual(target.read_bytes(), b"local edit that must not be overwritten\n")

    def test_unexpected_existing_file_is_preserved(self):
        self.destination.mkdir()
        extra = self.destination / "user-notes.txt"
        extra.write_text("keep this", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "Prepared source changed"):
            prepare_muduo.prepare(self.archive, self.destination)
        self.assertEqual(list(self.destination.iterdir()), [extra])

    def test_archive_mismatch_fails_before_creating_sources(self):
        changed = Path(self.temporary.name) / "changed.zip"
        changed.write_bytes(self.archive.read_bytes() + b"changed")
        with self.assertRaisesRegex(ValueError, "SHA256 mismatch"):
            prepare_muduo.prepare(changed, self.destination)
        self.assertFalse(self.destination.exists())

    def test_directory_cannot_replace_an_expected_source_file(self):
        target = self.destination / "CMakeLists.txt"
        target.mkdir(parents=True)
        with self.assertRaisesRegex(ValueError, "unexpected file type"):
            prepare_muduo.prepare(self.archive, self.destination)
        self.assertTrue(target.is_dir())
        self.assertEqual(list(self.destination.iterdir()), [target])

    def test_prepare_only_records_identity_without_claiming_linux_tests(self):
        build = Path(self.temporary.name) / "build"
        result = subprocess.run([sys.executable, str(ROOT / "scripts/build_linux.py"),
                                 "--prepare-only", "--build-dir", str(build)],
                                cwd=ROOT, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        reports = list((build / "reports").glob("run-*/build-report.json"))
        self.assertEqual(len(reports), 1)
        report = json.loads(reports[0].read_text(encoding="utf-8"))
        self.assertEqual(report["status"], "PREPARED")
        self.assertEqual([report[k] for k in ("linux_server_build", "ctest", "smoke")], ["UNRUN"] * 3)
        self.assertIn("CMakeLists.txt", report["source_manifest_before"])
        self.assertEqual(report["muduo"]["archive_sha256"], prepare_muduo.ARCHIVE_SHA256)

    def test_failed_tool_retains_failed_report_and_does_not_run_tests(self):
        build = Path(self.temporary.name) / "failed-build"
        with patch.object(sys, "argv", ["build_linux.py", "--build-dir", str(build)]), \
             patch.object(build_linux.platform, "system", return_value="Linux"), \
             patch.object(build_linux.shutil, "which", return_value=None), \
             patch.object(build_linux.subprocess, "run", return_value=subprocess.CompletedProcess([], 9)) as tool:
            self.assertEqual(build_linux.main(), 1)
        self.assertEqual(tool.call_count, 1)
        report = json.loads(next((build / "reports").glob("run-*/build-report.json")).read_text(encoding="utf-8"))
        self.assertEqual(report["status"], "FAILED")
        self.assertEqual(report["commands"][0]["returncode"], 9)
        self.assertEqual(report["ctest"], "UNRUN")
        self.assertEqual(report["smoke"], "UNRUN")


if __name__ == "__main__":
    unittest.main()
