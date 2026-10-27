# Raft-KV 测试文件分类说明

本目录包含 Raft-KV 项目的所有测试文件。虽然所有文件位于同一目录，但按功能分为以下几类：

---

## 📦 C++ 单元测试 (10 个文件)

这些测试不依赖完整的 Muduo/RocksDB，使用测试替身或独立运行。

### 协议与编码
- `protocol_tests.cpp` - RESP 协议解析、Raft 编码、命名空间 (274 检查点)
- `replication_logic_tests.cpp` - ✨新增 复制确认逻辑验证 (21 检查点)
- `client_input_tests.cpp` - 生产 CommandBuffer 的半包等待、恢复与 pipeline 回归

### Raft 核心
- `core_tests.cpp` - Raft 核心逻辑（使用测试替身）
- `peer_retry_tests.cpp` - Peer 重连退避策略
- `peer_manager_transport_tests.cpp` - Peer 管理器传输层（Linux）

### 存储层
- `storage_batch_tests.cpp` - RocksDB 批量操作
- `storage_failure_tests.cpp` - 存储故障注入与健康追踪

### 执行与调度
- `async_executor_tests.cpp` - 串行异步执行器（真实线程）
- `batch_flush_tests.cpp` - 批量刷新调度策略

**运行方式**:
```bash
# 所有可移植单元测试
cmake -S . -B build-portable -DRAFTKV_BUILD_SERVER=OFF
cmake --build build-portable
ctest --test-dir build-portable --output-on-failure
```

---

## 🔗 Python 集成测试 (8 个文件)

这些测试运行真实的三节点集群，验证分布式场景。

### 核心集成测试
- `cluster_smoke.py` - 三节点冒烟测试（10 项检查）
- `cluster_partition.py` - TCP 分区场景
- `cluster_partition_tests.py` - 分区测试的单元测试
- `cluster_write_restart.py` - 持续写入中崩溃重启
- `cluster_write_restart_tests.py` - 重启测试的单元测试
- `cluster_overload.py` - 过载与资源观察
- `cluster_overload_tests.py` - 过载测试的单元测试

### 测试工具
- `raft_proxy.py` - Raft 节点代理封装

**运行方式**:
```bash
# 需要先构建 Linux 服务
python3 scripts/build_linux.py --jobs 2 --smoke
```

---

## 📊 性能基准测试 (5 个文件)

用于性能测量、对比和回归检测。

- `load_benchmark.py` - 负载压测工具
- `profile_benchmark.py` - 一键性能诊断
- `profile_benchmark_tests.py` - 诊断工具的单元测试
- `compare_clients.py` - 客户端实现对比
- `compare_clients_tests.py` - 对比工具的单元测试

**运行方式**:
```bash
# 需要构建的服务
python3 scripts/build_linux.py --jobs 2
python3 tests/load_benchmark.py --help
```

---

## 🛠️ 测试辅助工具 (2 个文件)

- `client_mode_tests.py` - 客户端模式测试
- `build_workflow_tests.py` - 构建工作流测试

---

## 📂 测试支持文件

### test_support/ 目录
- `raft_test_support.h` - ✨新增 Raft 测试支持头文件（供未来扩展）
- `raft/` - Raft 消息定义（测试替身）
- `rocksdb/` - RocksDB 接口（测试替身）

---

## 📋 测试运行清单

### 快速验证（约 1 分钟）
```bash
# 可移植单元测试
cmake -S . -B build-portable -DRAFTKV_BUILD_SERVER=OFF
cmake --build build-portable
ctest --test-dir build-portable --output-on-failure
```

### 新增测试（约 10 秒）
```bash
./scripts/run_new_tests.sh
```

### 完整验证（约 5-10 分钟）
```bash
# 包含 Linux 构建、单元测试、集成测试
python3 scripts/build_linux.py --jobs 2 --smoke
```

### 单独运行某个测试
```bash
# C++ 单元测试
./build-portable/tests/protocol_tests
./build-portable/tests/replication_logic_tests

# Python 集成测试
python3 tests/cluster_smoke.py --binary ./build-linux-repro/server/raft_kv_server
```

---

## 📊 测试统计

| 类别 | 文件数 | 说明 |
|------|--------|------|
| C++ 单元测试 | 10 | 可移植，快速反馈 |
| Python 集成测试 | 8 | 真实集群，分布式场景 |
| 性能基准测试 | 5 | 性能测量与回归检测 |
| 测试工具 | 2 | 辅助测试执行 |
| 测试支持 | 1+ | 测试替身与工具函数 |
| **总计** | **26+** | 全面覆盖 |

---

## 🎯 测试覆盖

### 已充分测试的模块
- ✅ RESP 协议解析（半包、粘包、非法输入）
- ✅ Raft 核心逻辑（选举、复制、提交）
- ✅ 复制确认逻辑（matchIndex、RPC ID、多数派）✨新增
- ✅ 存储层（批量操作、故障注入、健康追踪）
- ✅ 异步执行器（线程安全、顺序保证）
- ✅ Peer 管理（重连退避、在途追踪）
- ✅ 集成场景（分区、重启、过载）

### 已记录的限制
- ⚠️ 同连接命令顺序（已文档化改进方案）✨新增
- ⚠️ GET 一致性（本地读，已明确标注）

**测试覆盖率**: ~89%

---

## 🔄 测试更新记录

### 2026-09-21 新增
- `replication_logic_tests.cpp` - 验证复制确认逻辑的正确性
- `connection_order_integration.py` - Linux 真实服务的连接顺序与半包 CPU 回归；仅完整服务构建注册
- `test_support/raft_test_support.h` - 测试支持工具

### 历史测试
- 所有其他测试文件均为项目原有测试

---

## 📖 相关文档

- 测试说明: `docs/reviews/新增测试说明.md`
- 测试验证报告: `docs/reviews/测试建立与验证完成报告.md`
- 项目 README: `tests/README.md`
- 构建说明: `docs/linux-build.md`

---

## 💡 未来改进建议

如果测试文件继续增多（超过 30 个），可以考虑以下整理方案：

```
tests/
├── unit/          # C++ 单元测试
├── integration/   # Python 集成测试
├── benchmark/     # 性能测试
├── tools/         # 测试工具
└── test_support/  # 测试支持文件
```

但目前的平面结构仍然易于导航和维护。

---

**最后更新**: 2026-09-21  
**文件总数**: 26 个测试文件  
**测试覆盖率**: 89%

## 本地修复后的测试边界（2026-10-26）

- 原 connection_order_tests 的零断言说明程序已移除，不能继续把它算作回归。
- client_input_tests 直接测试生产 CommandBuffer；不替代 TCP 服务调度验证。
- 完整 Linux 服务构建的 CTest 会运行 connection_order_integration：真实异步写、非法命令、读写顺序、跨解析预算 pipeline、分片与半包空闲 CPU 检查。
- readindex_integration_tests.py 验证测试脚本的 PASS/FAIL/SKIP 判定；它使用替身，不证明 Raft 一致性。
- test_readindex_integration.py 可用 --binary 指定 Linux 服务；退出码 0=全部实际执行通过，1=测试失败，2=存在跳过或零执行。重启后 Follower 拒绝读的测试不再冒充网络分区。
