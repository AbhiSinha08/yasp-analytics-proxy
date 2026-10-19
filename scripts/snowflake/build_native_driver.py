#!/usr/bin/env python3
"""Build the pinned Apache Snowflake driver with bounded stream prefetch.

This native build is separate from the gateway. It needs Linux x86-64, a C/C++
compiler and patch. Go, upstream sources, dependencies and output stay under target.
"""

import argparse
import hashlib
import json
import os
import platform
from pathlib import Path
import shutil
import subprocess
import tarfile
import urllib.request

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
GO_VERSION = "1.26.1"
GO_SHA = "031f088e5d955bab8657ede27ad4e3bc5b7c1ba281f05f245bcc304f327c987a"
SOURCE_URL = "https://codeload.github.com/apache/arrow-adbc/tar.gz/refs/tags/go/adbc/v1.11.0"
SOURCE_SHA = "79e57e8916fafcf25fdd13d0873526b6478ccaf6d5c2a284cba6129fbd0bab37"
SOURCE_DIR = "arrow-adbc-go-adbc-v1.11.0"
READER_SHA = "5a28e44ac3bf9ca7e1a8af465186a65971c35429bdec6c8c0ee86bc0501f2604"
PATCHED_READER_SHA = "8cb54c7ad91a69135681e82ad0752cc0b85568cbde2060a7f50a0b0e712b10fc"


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def download(url, path, expected):
    if not path.exists():
        temporary = path.with_suffix(path.suffix + ".partial")
        with urllib.request.urlopen(url, timeout=60) as response, temporary.open("wb") as output:
            shutil.copyfileobj(response, output)
        if digest(temporary) != expected:
            temporary.unlink()
            raise RuntimeError("download digest did not match the pinned artifact")
        temporary.replace(path)
    if digest(path) != expected:
        raise RuntimeError("cached archive digest did not match the pinned artifact")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=ROOT / "target/compatibility/native-build")
    args = parser.parse_args()
    if platform.system() != "Linux" or platform.machine() not in ("x86_64", "AMD64"):
        raise RuntimeError("the pinned native build supports Linux x86-64, including WSL")
    directory = args.output_dir.resolve()
    directory.mkdir(parents=True, exist_ok=True)
    go_archive = directory / f"go{GO_VERSION}.linux-amd64.tar.gz"
    download(f"https://go.dev/dl/go{GO_VERSION}.linux-amd64.tar.gz", go_archive, GO_SHA)
    if not (directory / "go/bin/go").is_file():
        with tarfile.open(go_archive) as archive:
            archive.extractall(directory, filter="data")
    source_archive = directory / "arrow-adbc-v1.11.0.tar.gz"
    download(SOURCE_URL, source_archive, SOURCE_SHA)
    source = directory / SOURCE_DIR
    if not source.exists():
        with tarfile.open(source_archive) as archive:
            archive.extractall(directory, filter="data")
    reader = source / "go/adbc/driver/snowflake/record_reader.go"
    patch = HERE / "snowflake-v1.11.0-prefetch.patch"
    if digest(reader) == READER_SHA:
        subprocess.run(["patch", "-p1", "--forward", "--batch", "-i", str(patch)], cwd=source, check=True)
        subprocess.run([str(directory / "go/bin/gofmt"), "-w", str(reader)], check=True)
    if digest(reader) != PATCHED_READER_SHA:
        raise RuntimeError("native source differs from the pinned, formatted prefetch patch")
    library = directory / "libadbc_driver_snowflake.so"
    environment = dict(os.environ, GOTOOLCHAIN="local", GOPATH=str(directory / "gopath"),
                       GOCACHE=str(directory / "gocache"), CGO_ENABLED="1", GOMAXPROCS="2")
    subprocess.run([str(directory / "go/bin/go"), "build", "-mod=readonly", "-p", "2", "-trimpath",
                    "-tags", "driverlib", "-buildmode=c-shared", "-ldflags=-s -w", "-o", str(library),
                    "./pkg/snowflake"], cwd=source / "go/adbc", env=environment, check=True)
    for name in ("LICENSE.txt", "NOTICE.txt"):
        shutil.copyfile(source / name, directory / name)
    result = {"upstream": "apache/arrow-adbc go/adbc/v1.11.0", "source_sha256": SOURCE_SHA,
              "go_version": GO_VERSION, "patch_sha256": digest(patch), "reader_sha256": digest(reader),
              "library": str(library), "library_sha256": digest(library)}
    (directory / "build.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
