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
add_executable(core_tests core_tests.cpp
    ${PROJECT_SOURCE_DIR}/src/raft/raft_node.cc
    ${PROJECT_SOURCE_DIR}/src/raft/kv_state_machine.cc
    ${PROJECT_SOURCE_DIR}/src/raftcore/raft_log.cc
    ${PROJECT_SOURCE_DIR}/src/storage/rocksdb_store.cpp)
target_include_directories(core_tests PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/test_support
    ${PROJECT_SOURCE_DIR}/src)
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(core_tests PRIVATE -Wall -Wextra -Wpedantic)
endif()
add_test(NAME core_tests COMMAND core_tests)

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
