# Learning runtime image. Base tag and apt package names match docs/linux-build.md.
# Package versions are whatever Ubuntu 26.04 serves at build time; this is not
# a frozen multi-machine distribution.
FROM ubuntu:26.04 AS build

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential cmake python3 unzip ca-certificates \
        libboost-dev libboost-thread-dev \
        libprotobuf-dev protobuf-compiler \
        librocksdb-dev libgflags-dev libgoogle-glog-dev libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY CMakeLists.txt /src/CMakeLists.txt
COPY src /src/src
COPY proto /src/proto
COPY scripts/prepare_muduo.py /src/scripts/prepare_muduo.py
COPY third_party/muduo.zip /src/third_party/muduo.zip

RUN python3 scripts/prepare_muduo.py \
        --archive third_party/muduo.zip \
        --destination /tmp/muduo-source \
    && cmake -S /tmp/muduo-source -B /tmp/muduo-build \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
        -DMUDUO_BUILD_EXAMPLES=OFF \
        -DCMAKE_DISABLE_FIND_PACKAGE_Protobuf=TRUE \
        -DBUILD_SHARED_LIBS=OFF \
        -DCMAKE_INSTALL_PREFIX=/tmp/muduo-install \
    && cmake --build /tmp/muduo-build --parallel 2 \
    && cmake --install /tmp/muduo-build \
    && cmake -S /src -B /tmp/server \
        -DCMAKE_BUILD_TYPE=RelWithDebInfo \
        -DRAFTKV_BUILD_SERVER=ON \
        -DBUILD_TESTING=OFF \
        -DMUDUO_INCLUDE_DIR=/tmp/muduo-install/include \
        -DMUDUO_NET_LIB=/tmp/muduo-install/lib/libmuduo_net.a \
        -DMUDUO_BASE_LIB=/tmp/muduo-install/lib/libmuduo_base.a \
    && cmake --build /tmp/server --parallel 2 --target raft_kv_server

FROM ubuntu:26.04

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates curl \
        libboost-thread-dev \
        libprotobuf-dev \
        librocksdb-dev libgflags-dev libgoogle-glog-dev libssl-dev \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /tmp/server/raft_kv_server /usr/local/bin/raft_kv_server
COPY docker/raft-kv-node.sh /usr/local/bin/raft-kv-node.sh
RUN chmod 755 /usr/local/bin/raft-kv-node.sh \
    && mkdir -p /data/kv /data/raft-log

EXPOSE 8080 9080 9090
HEALTHCHECK --interval=5s --timeout=2s --start-period=20s --retries=3 \
    CMD curl -fsS http://127.0.0.1:9090/health
ENTRYPOINT ["/usr/local/bin/raft-kv-node.sh"]
