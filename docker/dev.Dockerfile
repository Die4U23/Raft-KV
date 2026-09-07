FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive

RUN sed -i 's/archive.ubuntu.com/mirrors.aliyun.com/g' /etc/apt/sources.list

RUN apt-get update && apt-get install -y \
    build-essential cmake gdb git \
    wget curl gnupg ca-certificates \
    pkg-config autoconf automake libtool \
    python3 python3-pip unzip \
    g++-10 \
    libgflags-dev libprotobuf-dev protobuf-compiler \
    libgtest-dev libgoogle-glog-dev libssl-dev \
    zlib1g-dev libsnappy-dev liblz4-dev \
    default-libmysqlclient-dev libhiredis-dev \
    libleveldb-dev \
    libprotoc-dev \
    libboost-system-dev libboost-thread-dev libboost-date-time-dev libboost-regex-dev \
    vim less net-tools iputils-ping \
    && rm -rf /var/lib/apt/lists/*

RUN update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-10 100 \
    && update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-10 100 \
    && update-alternatives --install /usr/bin/cc cc /usr/bin/gcc-10 100 \
    && update-alternatives --install /usr/bin/c++ c++ /usr/bin/g++-10 100

COPY third_party/ /tmp/archives/
COPY scripts/prepare_muduo.py /tmp/prepare_muduo.py

RUN cd /tmp && \
    unzip -q /tmp/archives/rocksdb.zip -d . && \
    cd rocksdb-7.10.2 && \
    make -j1 shared_lib && make install-shared && \
    cd / && rm -rf /tmp/rocksdb-7.10.2

RUN cd /tmp && \
    unzip -q /tmp/archives/brpc.zip -d . && \
    cd brpc-1.15.0 && \
    mkdir build && cd build && \
    cmake -DCMAKE_BUILD_TYPE=Release .. && \
    make -j1 && make install && \
    cd / && rm -rf /tmp/brpc-1.15.0

RUN cd /tmp && \
    unzip -q /tmp/archives/braft.zip -d . && \
    cd braft-1.1.2 && \
    mkdir build && cd build && \
    cmake -DCMAKE_BUILD_TYPE=Release .. && \
    make -j1 && make install && \
    cd / && rm -rf /tmp/braft-1.1.2

RUN cd /tmp && \
    python3 /tmp/prepare_muduo.py --archive /tmp/archives/muduo.zip --destination /tmp/muduo-master > /tmp/muduo-preparation.json && \
    cd muduo-master && \
    mkdir build && cd build && \
    cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_POLICY_VERSION_MINIMUM=3.5 -DMUDUO_BUILD_EXAMPLES=OFF -DCMAKE_DISABLE_FIND_PACKAGE_Protobuf=TRUE .. && make -j1 && make install && \
    cd / && rm -rf /tmp/muduo-master

RUN cd /tmp && \
    unzip -q /tmp/archives/redis-plus-plus.zip -d . && \
    cd redis-plus-plus-master && \
    mkdir build && cd build && \
    cmake -DCMAKE_BUILD_TYPE=Release .. && make -j1 && make install && \
    cd / && rm -rf /tmp/redis-plus-plus-master

RUN rm -rf /tmp/archives

WORKDIR /workspace
CMD ["/bin/bash"]
