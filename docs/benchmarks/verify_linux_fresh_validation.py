"""Verify the archived fresh-directory Linux build using the shared evidence checks."""
import json

from verify_linux_validation import ROOT, verify as verify_workflow


def verify():
    return verify_workflow(
        archive_path=ROOT / "evidence/linux-fresh-validation-wjgjNs.tar.gz",
        expected_sha256="60f0772a8a149e71ffb99d18e63046b3141bea80678c95580e9e33faaf6951ff",
        prefix="run-a3q01rgw/", cluster="cluster/run-7s79tl9i/",
        initial_leader=2, final_leader=0, final_term=3, fresh=True)


if __name__ == "__main__":
    print(json.dumps(verify(), ensure_ascii=False, indent=2))
