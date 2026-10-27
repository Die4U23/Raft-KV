#!/bin/bash
# Portable production components and standalone models; no live Linux cluster.
set -euo pipefail
if [ ! -f CMakeLists.txt ]; then
    echo "Run this script from the repository root" >&2
    exit 1
fi
cmake -S . -B build-new-tests -G Ninja \
    -DRAFTKV_BUILD_SERVER=OFF -DCMAKE_BUILD_TYPE=Debug
cmake --build build-new-tests --parallel 2
ctest --test-dir build-new-tests --output-on-failure
python3 -m unittest discover -s tests -p '*_tests.py'
echo "PASS: portable checks; real TCP/connection tests require RAFTKV_BUILD_SERVER=ON"
