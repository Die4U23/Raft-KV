cmake_minimum_required(VERSION 3.16)
project(raft-kv LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(INSTALL_PREFIX /usr/local)
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
find_package(Boost COMPONENTS system thread REQUIRED)
find_library(PTHREAD_LIB pthread)
find_library(DL_LIB dl)

file(GLOB_RECURSE SRCS src/*.cpp src/*.h)

add_executable(raft_kv_server ${SRCS})

target_include_directories(raft_kv_server PRIVATE
    ${CMAKE_SOURCE_DIR}/src
    ${MUDUO_INCLUDE_DIR}
    ${ROCKSDB_INCLUDE_DIR}
    ${GFLAGS_INCLUDE_DIR}
    ${GLOG_INCLUDE_DIR}
)

target_link_libraries(raft_kv_server
    ${MUDUO_NET_LIB}
    ${MUDUO_BASE_LIB}
    ${ROCKSDB_LIB}
    ${GFLAGS_LIB}
    ${GLOG_LIB}
    ${OPENSSL_LIBRARIES}
    ${Boost_LIBRARIES}
    ${PTHREAD_LIB}
    ${DL_LIB}
)
