# raft-kv

基于 Raft 共识协议的三节点分布式 KV 存储系统。

## 架构

```
┌──────────────────────────────────────────────┐
│              main.cpp (Server)               │
│  Client TCP :808x    Raft RPC TCP :908x      │
│  SET/DEL → RaftNode.Propose()                │
│  GET → KVStateMachine.Get()                  │
│  Peer msg → RaftNode.Step()                  │
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
| `GET key` | 读取（任意节点均可服务） |
| `DEL key` | 删除（Leader 复制到多数节点后返回 `:1`） |
| `INFO` | 查看节点状态（id / state / leader / term / commit_index） |

写入非 Leader 节点会返回 `-ERR MOVED <leader_id>`。

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
| `--node_id` | 0 | 节点 ID（0、1、2） |
| `--client_port` | 8080 | 客户端 RESP 端口 |
| `--raft_port` | 9080 | Raft 对端 RPC 端口 |
| `--db_path` | `/tmp/kv_db` | 状态机 RocksDB 路径 |
| `--raft_log_path` | `/tmp/raft_log` | Raft 日志 RocksDB 路径 |
| `--peers` | `0:127.0.0.1:9080,1:127.0.0.1:9081,2:127.0.0.1:9082` | 集群拓扑 |
| `--leader_only_reads` | false | 仅 Leader 服务读请求 |

## 测试

所有测试可用 `redis-cli` 或 `nc` 完成。

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

# 从任意节点读取（均返回 bar）
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

### 5. 一键自动化测试

也可以用 Python 快速验证：

```bash
python3 << 'EOF'
import socket, time

def cmd(port, *args):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(3)
    s.connect(('127.0.0.1', port))
    resp = f"*{len(args)}\r\n" + "".join(f"${len(a)}\r\n{a}\r\n" for a in args)
    s.sendall(resp.encode())
    result = s.recv(1024).decode().strip()
    s.close()
    return result.replace('\r\n', '\\r\\n')

# 检查 INFO + SET + GET
for p in [8080, 8081, 8082]:
    print(f"Port {p} INFO:", cmd(p, "INFO"))
time.sleep(0.5)
print("SET:", cmd(8080, "SET", "hello", "world"))
for p in [8080, 8081, 8082]:
    print(f"Port {p} GET:", cmd(p, "GET", "hello"))
EOF
```

### 6. 预期输出示例

```
Port 8080 INFO: $62\r\nnode_id:0\r\nstate:leader\r\nleader_id:0\r\nterm:2\r\ncommit_index:1
Port 8081 INFO: $64\r\nnode_id:1\r\nstate:follower\r\nleader_id:0\r\nterm:2\r\ncommit_index:1
Port 8082 INFO: $64\r\nnode_id:2\r\nstate:follower\r\nleader_id:0\r\nterm:2\r\ncommit_index:1
SET: +OK
Port 8080 GET: $5\r\nworld
Port 8081 GET: $5\r\nworld
Port 8082 GET: $5\r\nworld
```

## Docker 编译与测试

项目提供了 Docker 开发环境。如果本地缺少依赖，可以直接用 Docker：

```bash
# 构建镜像
docker build -t raft-kv-dev -f docker/dev.Dockerfile .

# 编译
docker run --rm -v "$PWD":/workspace -w /workspace raft-kv-dev \
    bash -c "mkdir -p build2 && cd build2 && cmake .. -DCMAKE_BUILD_TYPE=Release && make -j\$(nproc)"

# 运行测试（三节点集群）
docker run --rm -v "$PWD":/workspace -w /workspace --network host raft-kv-dev \
    bash -c '
rm -rf /tmp/kv_db_{0,1,2} /tmp/raft_log_{0,1,2}
PEERS="0:127.0.0.1:9080,1:127.0.0.1:9081,2:127.0.0.1:9082"
for i in 0 1 2; do
    build2/raft_kv_server --node_id=$i --client_port=$((8080+i)) --raft_port=$((9080+i)) \
        --db_path=/tmp/kv_db_$i --raft_log_path=/tmp/raft_log_$i \
        --peers="$PEERS" --logtostderr=true 2>/dev/null &
done
sleep 3
# 用 python 测试 ...
'
```

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
- [ ] 客户端 RESP 管线化优化
- [ ] gRPC 或 HTTP API
- [ ] 监控指标导出（Prometheus）
- [ ] 单元测试（gtest）

## License

MIT
