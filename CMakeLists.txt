cmake_minimum_required(VERSION 3.16)
project(raft-kv LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
find_package(Threads REQUIRED)

option(RAFTKV_BUILD_SERVER "Build Linux server with Muduo/RocksDB/Protobuf" ON)
include(CTest)
if(BUILD_TESTING)
    add_subdirectory(tests)
endif()
if(NOT RAFTKV_BUILD_SERVER)
    return()
endif()

# ----- protobuf 生成（Raft 消息 + 业务命令）-----
find_package(Protobuf REQUIRED)
set(PROTO_DIR ${CMAKE_SOURCE_DIR}/proto)
file(GLOB_RECURSE PROTO_FILES ${PROTO_DIR}/*.proto)
PROTOBUF_GENERATE_CPP(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})

# ----- 手动库查找 -----
set(INSTALL_PREFIX /usr/local CACHE PATH "Additional dependency installation prefix")
set(INCLUDE_DIR ${INSTALL_PREFIX}/include)
set(LIBRARY_DIR ${INSTALL_PREFIX}/lib)

find_path(MUDUO_INCLUDE_DIR muduo/net/EventLoop.h PATHS ${INCLUDE_DIR})
find_library(MUDUO_NET_LIB muduo_net PATHS ${LIBRARY_DIR})
find_library(MUDUO_BASE_LIB muduo_base PATHS ${LIBRARY_DIR})

find_path(ROCKSDB_INCLUDE_DIR rocksdb/db.h PATHS ${INCLUDE_DIR})
find_library(ROCKSDB_LIB rocksdb PATHS ${LIBRARY_DIR})

find_path(GFLAGS_INCLUDE_DIR gflags/gflags.h PATHS ${INCLUDE_DIR})
find_library(GFLAGS_LIB gflags PATHS ${LIBRARY_DIR})

find_path(GLOG_INCLUDE_DIR glog/logging.h PATHS ${INCLUDE_DIR})
find_library(GLOG_LIB glog PATHS ${LIBRARY_DIR})

find_package(OpenSSL REQUIRED)
# Boost.System is header-only since 1.69; recent Boost has no system library.
find_package(Boost 1.69 COMPONENTS thread REQUIRED)
find_library(DL_LIB dl)

# ----- 收集所有源文件（包括新增的 raftcore 目录）-----
file(GLOB_RECURSE SRCS src/*.cpp src/*.h src/*.cc)
list(APPEND SRCS ${PROTO_SRCS})

# ----- 可执行文件 -----
add_executable(raft_kv_server ${SRCS})

target_include_directories(raft_kv_server PRIVATE
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_BINARY_DIR}          # 让生成的 pb.h 能被找到
    ${MUDUO_INCLUDE_DIR}
    ${ROCKSDB_INCLUDE_DIR}
    ${GFLAGS_INCLUDE_DIR}
    ${GLOG_INCLUDE_DIR}
    ${PROTOBUF_INCLUDE_DIRS}
)

target_link_libraries(raft_kv_server
    ${MUDUO_NET_LIB}
    ${MUDUO_BASE_LIB}
    ${ROCKSDB_LIB}
    ${GFLAGS_LIB}
    ${GLOG_LIB}
    ${OPENSSL_LIBRARIES}
    ${Boost_LIBRARIES}
    Threads::Threads
    ${DL_LIB}
    ${PROTOBUF_LIBRARIES}
)
