# raft-kv

基于 Raft 共识协议的固定成员 KV 存储学习项目，使用 C++17、Muduo、Protobuf 和 RocksDB。默认配置为三个节点。

当前本地优化的实现范围和验证结果见 [LOCAL_REVIEW_STATUS.md](LOCAL_REVIEW_STATUS.md)，测试方法见 [tests/README.md](tests/README.md)。完整 Linux 服务的验证状态以该记录为准。

面向 C++ 后端 / 基础架构实习的改进顺序、验收标准和面试准备见 [实习项目升级路线](docs/internship-roadmap.md)。路线中的待办不代表已经实现的能力。

并发处理支持满批立即调度、状态机异步应用、队列限额、慢连接保护，以及 INFO 阶段耗时与批量大小统计。默认 `--async_apply=true` 将已提交 KV 批次交给串行工作线程，Raft 日志仍同步落盘；详见 [并发处理说明](docs/concurrency.md) 与 [压测说明](docs/benchmark.md)。真实吞吐和尾延迟尚未测量。

## 架构

```
┌──────────────────────────────────────────────┐
│              main.cpp (Server)               │
│  Client TCP :808x    Raft RPC TCP :908x      │
│  SET/DEL → RaftNode.Propose()                │
│  GET → KVStateMachine.Get()                  │
│  Peer msg → RaftNode.Handle*()                  │
└──────┬───────────────────┬───────────────────┘
       │                   │
┌──────▼──────┐   ┌───────▼──────────────┐
│ RaftNode    │   │ PeerManager          │
│ - 选举/复制 │   │ - muduo TcpClient x N│
│ - 日志管理  │   │ - 断线重连           │
│ - 提交应用  │   │ - 异步发送           │
└──────┬──────┘   └──────────────────────┘
       │
┌──────▼──────────┐
│ KVStateMachine  │
│ (封装RocksDB)   │
└─────────────────┘
```

### 组件一览

| 文件 | 功能 |
|------|------|
| `src/raft/raft_node.h/.cc` | Raft 共识核心 — Leader 选举、日志复制、提交应用 |
| `src/raft/peer_manager.h/.cc` | 对端 TCP 连接管理 + 二进制帧编解码 |
| `src/raft/kv_state_machine.h/.cc` | KV 状态机（封装 RocksDBStore） |
| `src/raft/raft_codec.h` | 二进制帧编解码器 |
| `src/raftcore/raft_log.h/.cc` | RocksDB 持久化的 Raft 日志 + 硬状态 |
| `src/storage/rocksdb_store.h/.cpp` | RocksDB KV 存储引擎 |
| `src/common/resp_parser.h` | Redis RESP 协议解析器 |
| `src/server/main.cpp` | 服务入口 |

### 支持的客户端命令

| 命令 | 说明 |
|------|------|
| `PING` | 测试连通性，返回 `+PONG` |
| `SET key value` | 写入（Leader 复制到多数节点后返回 `+OK`） |
| `GET key` | 本地读取，可能落后于 Leader，不保证线性一致性 |
| `DEL key` | 提交并应用后，存在的键返回 `:1`，不存在返回 `:0` |
| `SELECT namespace` | 为当前 TCP 连接选择命名空间；新连接默认 `default` |
| `INFO` | 查看角色、任期、Leader、commit_index、last_applied、namespace |

写入非 Leader 节点会返回 `-ERR MOVED <leader_id>`。这是项目自定义错误，并非 Redis Cluster 的完整重定向协议。

`SELECT` 与后续操作必须使用同一连接；分别运行两次 `redis-cli` 不会保留命名空间。连接断开或领导权变化时，已进入日志的请求结果可能未知；当前没有请求去重机制。

## 依赖

- C++17 (GCC 10+)
- CMake ≥ 3.16
- [muduo](https://github.com/chenshuo/muduo) — 网络库
- [RocksDB](https://github.com/facebook/rocksdb) — 持久化存储
- [Protobuf](https://github.com/protocolbuffers/protobuf) — RPC 消息序列化
- [gflags](https://github.com/gflags/gflags) — 命令行参数
- [glog](https://github.com/google/glog) — 日志

## 编译

```bash
mkdir -p build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
```

产物为 `build/raft_kv_server`。

如果依赖库安装在非标准路径，请修改 `CMakeLists.txt` 中的 `INSTALL_PREFIX`。

## 启动集群

验证新版本时，请为各节点使用全新的、配套的 KV 和 Raft 日志目录。旧版非空 KV 数据库没有 lastApplied 标记，当前会拒绝启动；请保留旧数据，不要直接清空。所有节点须以同一版本重新构建，暂不支持混合版本滚动升级。

分别在三个终端中启动三个节点：

**终端 1 — 节点 0**
```bash
./build/raft_kv_server \
    --node_id=0 --client_port=8080 --raft_port=9080 \
    --db_path=/tmp/kv_db_0 --raft_log_path=/tmp/raft_log_0
```

**终端 2 — 节点 1**
```bash
./build/raft_kv_server \
    --node_id=1 --client_port=8081 --raft_port=9081 \
    --db_path=/tmp/kv_db_1 --raft_log_path=/tmp/raft_log_1
```

**终端 3 — 节点 2**
```bash
./build/raft_kv_server \
    --node_id=2 --client_port=8082 --raft_port=9082 \
    --db_path=/tmp/kv_db_2 --raft_log_path=/tmp/raft_log_2
```

启动后等待 2-3 秒，三个节点会自动完成 Leader 选举。

### 命令行参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--node_id` | 0 | 非负节点 ID，支持不连续 ID，必须在 peers 中且唯一 |
| `--client_port` | 8080 | 客户端 RESP 端口 |
| `--raft_port` | 9080 | Raft 对端 RPC 端口 |
| `--db_path` | `/tmp/kv_db` | 状态机 RocksDB 路径 |
| `--raft_log_path` | `/tmp/raft_log` | Raft 日志 RocksDB 路径 |
| `--peers` | `0:127.0.0.1:9080,1:127.0.0.1:9081,2:127.0.0.1:9082` | 集群拓扑 |
| `--leader_only_reads` | false | 仅 Leader 服务本地读；仍不保证线性一致性 |
| `--group_commit_ms` | 1 | 收集窗口目标，0–10 ms；0 表示本轮回调之后处理 |
| `--max_clients` | 1024 | 最大客户端连接数 |

## 测试

协议与核心回归测试可独立构建；真实集群测试需要 Linux 服务及实际依赖。完整命令与测试边界见 [测试说明](tests/README.md)。以下命令用于手工检查。

### 1. 验证 Leader 选举

查看各节点状态（应有一个 state:leader，其余 state:follower）：

```bash
redis-cli -p 8080 INFO
redis-cli -p 8081 INFO
redis-cli -p 8082 INFO
```

### 2. 测试写入和一致性复制

```bash
# 对 Leader 写入（假设 8080 是 Leader）
redis-cli -p 8080 SET foo bar
# → +OK

# 等待各节点应用日志后再读取（Follower 可能暂时返回旧值）
redis-cli -p 8080 GET foo
redis-cli -p 8081 GET foo
redis-cli -p 8082 GET foo
```

### 3. 测试 MOVED 重定向

```bash
# 对 Follower 写入会返回错误
redis-cli -p 8081 SET key val
# → -ERR MOVED 0
```

### 4. 测试 Leader 故障转移

```bash
# Kill Leader 进程（Ctrl+C 或 kill）
kill <leader_pid>

# 等待 3-5 秒，剩余节点将选举新 Leader
redis-cli -p 8081 INFO   # 应该是 leader
redis-cli -p 8082 INFO   # 应该是 follower

# 数据不丢失
redis-cli -p 8081 GET foo   # → bar
```

### 5. 自动化验证

无需服务依赖即可运行可移植测试：

```bash
cmake -S . -B build-portable -DRAFTKV_BUILD_SERVER=OFF
cmake --build build-portable
ctest --test-dir build-portable --output-on-failure
```

编译真实 Linux 服务后，运行隔离的三节点测试；脚本自动发现 Leader，使用独立临时目录，不清理你的现有数据库：

```bash
python3 tests/cluster_smoke.py --server ./build/raft_kv_server
```

## Docker 编译与测试

现有开发镜像包含多种历史依赖，首次构建可能较慢。容器内的三个测试节点通过回环地址通信，无需开放主机端口：

```bash
docker build -t raft-kv-dev -f docker/dev.Dockerfile .
docker run --rm -v "$PWD":/workspace -w /workspace raft-kv-dev \
    bash -lc 'cmake -S . -B build-linux -DCMAKE_BUILD_TYPE=Release && cmake --build build-linux -j2 && ctest --test-dir build-linux --output-on-failure && python3 tests/cluster_smoke.py --server ./build-linux/raft_kv_server'
```

上述真实环境流程尚需实际执行，不能用可移植测试结果替代。

## 目录结构

```
raft-kv/
├── CMakeLists.txt              # 构建配置
├── proto/
│   └── raft_messages.proto     # Raft RPC 消息定义（含 RaftMessage oneof 包装）
├── src/
│   ├── common/
│   │   └── resp_parser.h       # RESP 协议解析
│   ├── storage/
│   │   ├── rocksdb_store.h
│   │   └── rocksdb_store.cpp   # RocksDB KV 存储
│   ├── raftcore/
│   │   ├── raft_log.h
│   │   └── raft_log.cc         # Raft 日志 + 硬状态持久化
│   ├── raft/
│   │   ├── raft_node.h/.cc     # Raft 核心状态机
│   │   ├── peer_manager.h/.cc  # 对端 TCP 管理
│   │   ├── kv_state_machine.h/.cc  # KV 状态机
│   │   └── raft_codec.h        # 帧编解码
│   └── server/
│       └── main.cpp            # 入口
├── docker/
│   └── dev.Dockerfile          # 开发容器
└── third_party/                # 依赖源码归档
```

## TODO

- [ ] 日志快照（Snapshot）与日志压缩
- [ ] 成员变更（Add/Remove Server）
- [x] RESP 半包保留、输入上限与同连接命令顺序控制
- [ ] ReadIndex / 线性一致读
- [ ] 客户端请求去重与请求超时
- [ ] 真实集群故障测试与可复现性能基线
- [ ] gRPC 或 HTTP API
- [ ] 监控指标导出（Prometheus）
- [x] 可移植协议回归测试
- [ ] 覆盖真实依赖和崩溃恢复的持续集成
