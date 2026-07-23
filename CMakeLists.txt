cmake_minimum_required(VERSION 3.16)
project(raft-kv LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 查找已安装的库
find_package(muduo REQUIRED)
find_package(brpc REQUIRED)
find_package(braft REQUIRED)
find_package(RocksDB REQUIRED)
find_package(redis++ REQUIRED)

# 系统库
find_package(Threads REQUIRED)
find_package(Protobuf REQUIRED)
find_package(gflags REQUIRED)
find_package(glog REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(Boost COMPONENTS system thread REQUIRED)

# MySQL C API
find_library(MYSQL_LIB mysqlclient)

# 收集源码
file(GLOB_RECURSE SOURCES src/*.cpp src/*.h)

# 可执行文件：KV 服务器
add_executable(raft_kv_server ${SOURCES})

target_include_directories(raft_kv_server PRIVATE
    ${CMAKE_SOURCE_DIR}/src
    ${MUDAO_INCLUDE_DIRS}
    ${BRPC_INCLUDE_DIRS}
    ${BRAFT_INCLUDE_DIRS}
    ${ROCKSDB_INCLUDE_DIRS}
    ${REDIS++_INCLUDE_DIRS}
    ${PROTOBUF_INCLUDE_DIRS}
    ${GFLAGS_INCLUDE_DIRS}
    ${GLOG_INCLUDE_DIRS}
)

target_link_libraries(raft_kv_server
    muduo_net muduo_base
    brpc braft
    rocksdb
    redis++
    ${PROTOBUF_LIBRARIES}
    ${GFLAGS_LIBRARIES}
    ${GLOG_LIBRARIES}
    ${MYSQL_LIB}
    ${OPENSSL_LIBRARIES}
    ${Boost_LIBRARIES}
    Threads::Threads
    dl
)
