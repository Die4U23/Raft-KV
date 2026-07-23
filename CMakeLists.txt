cmake_minimum_required(VERSION 3.16)
project(raft-kv LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# ------ protobuf generation ------
find_package(Protobuf REQUIRED)
set(PROTO_DIR ${CMAKE_SOURCE_DIR}/proto)
file(GLOB_RECURSE PROTO_FILES ${PROTO_DIR}/*.proto)
PROTOBUF_GENERATE_CPP(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})
include_directories(${PROTO_SRCS} ${PROTO_HDRS})

# ------ manual library paths (same as before) ------
set(INSTALL_PREFIX /usr/local)
set(INCLUDE_DIR  ${INSTALL_PREFIX}/include)
set(LIBRARY_DIR  ${INSTALL_PREFIX}/lib)

find_path(MUDUO_INCLUDE_DIR  muduo/net/EventLoop.h  PATHS ${INCLUDE_DIR})
find_library(MUDUO_NET_LIB    muduo_net              PATHS ${LIBRARY_DIR})
find_library(MUDUO_BASE_LIB   muduo_base             PATHS ${LIBRARY_DIR})

#find_path(BRPC_INCLUDE_DIR    brpc/server.h          PATHS ${INCLUDE_DIR})
#find_library(BRPC_LIB         brpc                   PATHS ${LIBRARY_DIR})

#find_path(BRAFT_INCLUDE_DIR   braft/raft.h           PATHS ${INCLUDE_DIR})
#find_library(BRAFT_LIB        braft                  PATHS ${LIBRARY_DIR})

find_path(ROCKSDB_INCLUDE_DIR rocksdb/db.h           PATHS ${INCLUDE_DIR})
find_library(ROCKSDB_LIB      rocksdb                PATHS ${LIBRARY_DIR})

# redis++ (optional now, but keep)
find_path(REDIS++_INCLUDE_DIR redis++.h PATHS ${INCLUDE_DIR}/sw/redis++)
if(NOT REDIS++_INCLUDE_DIR)
    set(REDIS++_INCLUDE_DIR ${INCLUDE_DIR}/sw/redis++)
endif()
find_library(REDIS++_LIB      redis++                PATHS ${LIBRARY_DIR})

# gflags, glog
find_path(GFLAGS_INCLUDE_DIR  gflags/gflags.h        PATHS ${INCLUDE_DIR})
find_library(GFLAGS_LIB       gflags                 PATHS ${LIBRARY_DIR})
find_path(GLOG_INCLUDE_DIR    glog/logging.h         PATHS ${INCLUDE_DIR})
find_library(GLOG_LIB         glog                   PATHS ${LIBRARY_DIR})

# OpenSSL
find_package(OpenSSL REQUIRED)
# Boost
find_package(Boost COMPONENTS system thread REQUIRED)
# MySQL C API
find_library(MYSQL_LIB        mysqlclient            PATHS /usr/lib/x86_64-linux-gnu)
# pthread, dl
find_library(PTHREAD_LIB      pthread)
find_library(DL_LIB           dl)

# ------ collect sources ------
file(GLOB_RECURSE SRCS src/*.cpp src/*.h)
list(APPEND SRCS ${PROTO_SRCS})
file(GLOB_RECURSE HDRS src/*.h)
list(APPEND HDRS ${PROTO_HDRS})

# ------ executable ------
add_executable(raft_kv_server ${SRCS} ${HDRS})

target_include_directories(raft_kv_server PRIVATE
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_BINARY_DIR}  # for generated proto headers
    ${MUDUO_INCLUDE_DIR}
    ${BRPC_INCLUDE_DIR}
    ${BRAFT_INCLUDE_DIR}
    ${ROCKSDB_INCLUDE_DIR}
    ${REDIS++_INCLUDE_DIR}
    ${PROTOBUF_INCLUDE_DIRS}
    ${GFLAGS_INCLUDE_DIR}
    ${GLOG_INCLUDE_DIR}
)

target_link_libraries(raft_kv_server
    ${MUDUO_NET_LIB}
    ${MUDUO_BASE_LIB}
#    ${BRPC_LIB}
#    ${BRAFT_LIB}
    ${ROCKSDB_LIB}
    ${REDIS++_LIB}
    ${PROTOBUF_LIBRARIES}
    ${GFLAGS_LIB}
    ${GLOG_LIB}
    ${MYSQL_LIB}
    ${OPENSSL_LIBRARIES}
    ${Boost_LIBRARIES}
    ${PTHREAD_LIB}
    ${DL_LIB}
)
