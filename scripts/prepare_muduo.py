"""Prepare the pinned Muduo sources without changing the original archive."""
import argparse
import hashlib
import io
import json
import stat
import zipfile
from pathlib import Path, PurePosixPath


ARCHIVE_SHA256 = "910d21d1343164e9517f32a5b77025cfeed0f20aae2ae86dd1ed399a7b4d2192"
HTTP_SOURCE = "muduo/net/http/HttpResponse.cc"


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def prepare(archive, destination):
    payload = Path(archive).read_bytes()
    if sha256(payload) != ARCHIVE_SHA256:
        raise ValueError("Muduo archive SHA256 mismatch; original archive was not modified")
    expected = {}
    with zipfile.ZipFile(io.BytesIO(payload)) as bundle:
        entries = bundle.infolist()
        if sum(e.file_size for e in entries) > 32 * 1024 * 1024:
            raise ValueError("Unexpected Muduo archive size")
        for entry in entries:
            path = PurePosixPath(entry.filename)
            if (path.is_absolute() or ".." in path.parts or "\\" in entry.filename
                    or not path.parts or path.parts[0] != "muduo-master"
                    or stat.S_ISLNK(entry.external_attr >> 16)):
                raise ValueError("Unexpected Muduo archive path/type")
            if entry.is_dir():
                continue
            relative = PurePosixPath(*path.parts[1:]).as_posix()
            if relative == "." or relative in expected:
                raise ValueError("Duplicate or empty Muduo archive filename")
            expected[relative] = bundle.read(entry)
    http = expected[HTTP_SOURCE]
    for old, new in ((b"char buf[32];", b"char buf[64];"),
                     (b"Content-Length: %zd", b"Content-Length: %zu")):
        if http.count(old) != 1:
            raise ValueError("Unexpected HttpResponse.cc patch context")
        http = http.replace(old, new, 1)
    expected[HTTP_SOURCE] = http
    destination = Path(destination)
    if destination.is_symlink():
        raise ValueError("Prepared source root must not be a symlink")
    destination = destination.resolve()
    # Check all existing content before writing. Preserve edits rather than overwrite them.
    if destination.exists():
        for path in destination.rglob("*"):
            if path.is_symlink():
                raise ValueError("Prepared source contains a symlink: " + str(path))
            if path.is_file():
                relative = path.relative_to(destination).as_posix()
                if relative not in expected or path.read_bytes() != expected[relative]:
                    raise ValueError("Prepared source changed; use a new destination: " + str(path))
            elif not path.is_dir() or path.relative_to(destination).as_posix() in expected:
                raise ValueError("Prepared source has an unexpected file type: " + str(path))
    destination.mkdir(parents=True, exist_ok=True)
    for relative, data in expected.items():
        path = destination / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            with path.open("xb") as stream:
                stream.write(data)
    return {"archive_sha256": ARCHIVE_SHA256, "patch_revision": 1,
            "file_count": len(expected), "source_directory": str(destination),
            "patched_http_sha256": sha256(http),
            "source_manifest": {name: sha256(data) for name, data in sorted(expected.items())}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", required=True, type=Path)
    parser.add_argument("--destination", required=True, type=Path)
    args = parser.parse_args()
    try:
        result = prepare(args.archive, args.destination)
    except (OSError, ValueError, zipfile.BadZipFile) as error:
        parser.exit(1, str(error) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
