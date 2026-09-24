add_executable(protocol_tests protocol_tests.cpp
    ${PROJECT_SOURCE_DIR}/src/namespace/namespace_manager.cc)
target_include_directories(protocol_tests PRIVATE ${PROJECT_SOURCE_DIR}/src)
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(protocol_tests PRIVATE -Wall -Wextra -Wpedantic)
endif()
add_test(NAME protocol_tests COMMAND protocol_tests)

# Run the production consensus/storage sources without Linux-only dependencies.
# These test doubles model API behavior; they do not certify real transport,
# protobuf interoperability, or RocksDB crash/power-loss durability.
# Replication ACK tests that compile the production RaftNode.
set(RAFT_PORTABLE_SRCS
    ${PROJECT_SOURCE_DIR}/src/raft/raft_node.cc
    ${PROJECT_SOURCE_DIR}/src/raft/kv_state_machine.cc
    ${PROJECT_SOURCE_DIR}/src/raftcore/raft_log.cc
    ${PROJECT_SOURCE_DIR}/src/storage/rocksdb_store.cpp)

function(add_portable_raft_test name)
    add_executable(${name} ${name}.cpp ${RAFT_PORTABLE_SRCS})
    target_include_directories(${name} PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/test_support
        ${PROJECT_SOURCE_DIR}/src)
    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        target_compile_options(${name} PRIVATE -Wall -Wextra -Wpedantic)
    endif()
    add_test(NAME ${name} COMMAND ${name})
endfunction()

add_portable_raft_test(core_tests)
add_portable_raft_test(storage_failure_tests)
add_portable_raft_test(replication_logic_tests)
add_portable_raft_test(replication_partition_tests)
add_portable_raft_test(readindex_tests)
add_portable_raft_test(replication_edge_cases_unit)
add_portable_raft_test(kv_state_machine_tests)
add_portable_raft_test(raft_log_tests)
add_portable_raft_test(raft_node_coverage_tests)
add_portable_raft_test(cluster_features_tests)
set_tests_properties(readindex_tests PROPERTIES TIMEOUT 20)
set_tests_properties(raft_node_coverage_tests PROPERTIES TIMEOUT 20)
set_tests_properties(cluster_features_tests PROPERTIES TIMEOUT 20)

# Production ClassifyCommand and FIFO ERROR ordering (no RaftNode).
add_executable(connection_order_tests connection_order_tests.cpp)
target_include_directories(connection_order_tests PRIVATE ${PROJECT_SOURCE_DIR}/src)
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(connection_order_tests PRIVATE -Wall -Wextra -Wpedantic)
endif()
add_test(NAME connection_order_tests COMMAND connection_order_tests)

add_executable(storage_batch_tests storage_batch_tests.cpp
    ${PROJECT_SOURCE_DIR}/src/raft/kv_state_machine.cc
    ${PROJECT_SOURCE_DIR}/src/raftcore/raft_log.cc
    ${PROJECT_SOURCE_DIR}/src/storage/rocksdb_store.cpp)
target_include_directories(storage_batch_tests PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/test_support ${PROJECT_SOURCE_DIR}/src)
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(storage_batch_tests PRIVATE -Wall -Wextra -Wpedantic)
endif()
add_test(NAME storage_batch_tests COMMAND storage_batch_tests)

# Real std::thread executor tests; no RocksDB/Muduo/protobuf doubles are involved.
add_executable(async_executor_tests async_executor_tests.cpp
    ${PROJECT_SOURCE_DIR}/src/raft/serial_apply_executor.cc)
target_include_directories(async_executor_tests PRIVATE ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(async_executor_tests PRIVATE Threads::Threads)
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(async_executor_tests PRIVATE -Wall -Wextra -Wpedantic)
endif()
add_test(NAME async_executor_tests COMMAND async_executor_tests)
set_tests_properties(async_executor_tests PROPERTIES TIMEOUT 15)

# Real scheduling policy with a deterministic fake event queue; no Muduo timers.
add_executable(batch_flush_tests batch_flush_tests.cpp)
target_include_directories(batch_flush_tests PRIVATE ${PROJECT_SOURCE_DIR}/src)
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(batch_flush_tests PRIVATE -Wall -Wextra -Wpedantic)
endif()
add_test(NAME batch_flush_tests COMMAND batch_flush_tests)

# Production backoff policy only; live Muduo lifecycle is checked on Linux.
add_executable(peer_retry_tests peer_retry_tests.cpp)
target_include_directories(peer_retry_tests PRIVATE ${PROJECT_SOURCE_DIR}/src)
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(peer_retry_tests PRIVATE -Wall -Wextra -Wpedantic)
endif()
add_test(NAME peer_retry_tests COMMAND peer_retry_tests)

