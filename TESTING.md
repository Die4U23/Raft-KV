# Raft-KV 测试验证指南

本文档覆盖 Raft 共识集群和 Namespace 数据隔离两部分的测试方法。

## 准备工作

### 编译

```bash
mkdir -p build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
```

产物为 `build/raft_kv_server`。

如果本地缺依赖，也可以用 Docker 编译（见 README.md）。

### 测试工具

以下测试使用 `redis-cli`，也可以用 Python 脚本。

```bash
# 安装 redis-cli（如果还没有）
sudo apt install redis-tools          # Ubuntu/Debian
brew install redis                    # macOS
```

---

## 一、Raft 共识集群测试

### 1.1 启动三节点集群

分别在三个终端启动：

**节点 0（终端 1）**
```bash
./build/raft_kv_server \
    --node_id=0 --client_port=8080 --raft_port=9080 \
    --db_path=/tmp/kv_db_0 --raft_log_path=/tmp/raft_log_0
```

**节点 1（终端 2）**
```bash
./build/raft_kv_server \
    --node_id=1 --client_port=8081 --raft_port=9081 \
    --db_path=/tmp/kv_db_1 --raft_log_path=/tmp/raft_log_1
```

**节点 2（终端 3）**
```bash
./build/raft_kv_server \
    --node_id=2 --client_port=8082 --raft_port=9082 \
    --db_path=/tmp/kv_db_2 --raft_log_path=/tmp/raft_log_2
```

启动后等待约 3 秒，三节点会自动完成 Leader 选举。

---

### 1.2 验证 Leader 选举

```bash
# 查看集群状态，其中有一个 state:leader，其余 state:follower
redis-cli -p 8080 INFO
redis-cli -p 8081 INFO
redis-cli -p 8082 INFO
```

**预期输出**：

```
node_id:0
state:leader          ← 这个节点是 Leader
leader_id:0
term:2
commit_index:0

node_id:1
state:follower        ← Follower
leader_id:0
term:2
commit_index:0

node_id:2
state:follower        ← Follower
leader_id:0
term:2
commit_index:0
```

> 三个节点的 `term` 和 `commit_index` 保持一致说明集群状态正常。

---

### 1.3 写入数据并验证复制

假设节点 0 是 Leader（根据 1.2 的结果判断，如果 Leader 不是 8080，替换对应端口）。

```bash
# 写入
redis-cli -p 8080 SET hello world
# → OK

redis-cli -p 8080 SET foo bar
# → OK
```

**从任意节点读取，数据应一致**：

```bash
redis-cli -p 8080 GET hello   # → "world"
redis-cli -p 8081 GET hello   # → "world"  （从 follower 读到）
redis-cli -p 8082 GET hello   # → "world"  （从 follower 读到）

redis-cli -p 8080 GET foo     # → "bar"
redis-cli -p 8081 GET foo     # → "bar"
redis-cli -p 8082 GET foo     # → "bar"
```

**验证**：写入一次，三个节点全都返回相同值 → 日志复制正常 ✅

---

### 1.4 向非 Leader 写入 → MOVED 错误

```bash
# 对 Follower 执行 SET（假设 8080 是 Leader，8081 是 Follower）
redis-cli -p 8081 SET key value
# → (error) ERR MOVED 0

redis-cli -p 8082 SET key value
# → (error) ERR MOVED 0
```

**验证**：非 Leader 拒绝写入并返回 `MOVED <leader_id>` ✅

---

### 1.5 Leader 故障转移

这是最关键的测试——验证集群在 Leader 宕机后能够自动恢复。

**步骤 1：确认当前 Leader 和数据**

```bash
redis-cli -p 8080 INFO | grep state     # → state:leader
redis-cli -p 8080 SET survive yes        # → OK
redis-cli -p 8080 GET survive            # → "yes"
```

**步骤 2：Kill Leader**

```
在节点 0 的终端按 Ctrl+C，或 kill <pid>
```

**步骤 3：等待约 3-5 秒，检查剩余节点的状态**

```bash
redis-cli -p 8081 INFO | grep state     # → state:leader  （新 Leader）
redis-cli -p 8082 INFO | grep state     # → state:follower
```

**步骤 4：验证数据不丢**

```bash
redis-cli -p 8081 GET survive           # → "yes"  ✅
redis-cli -p 8082 GET survive           # → "yes"  ✅
```

**步骤 5：新 Leader 可以正常写入**

```bash
redis-cli -p 8081 SET after_failover ok
# → OK
redis-cli -p 8082 GET after_failover    # → "ok"  ✅
```

**验证**：
- ✅ Leader 宕机后自动选举新 Leader
- ✅ commit_index 保持，数据不丢失
- ✅ 新 Leader 可正常接收写入

---

### 1.6 DELETE 操作

```bash
redis-cli -p 8081 DEL survive           # → (integer) 1
redis-cli -p 8081 GET survive           # → (nil)
redis-cli -p 8082 GET survive           # → (nil)  （从 follower 读也删了）
```

---

## 二、Namespace 命名空间测试

Namespace 允许同一个集群中不同业务/租户的数据完全隔离，互不干扰。

### 2.1 基本用法：SELECT 切换命名空间

```bash
# 默认在 "default" 命名空间
redis-cli -p 8080 SET foo 100
# → OK

# 切换到 ns1
redis-cli -p 8080 SELECT ns1
# → OK

# 在 ns1 中，foo 是一个新的键空间
redis-cli -p 8080 GET foo               # → (nil)  （ns1 里没有 foo）
redis-cli -p 8080 SET foo 200           # → OK
redis-cli -p 8080 GET foo               # → "200"

# 切回 default，数据隔离
redis-cli -p 8080 SELECT default
# → OK
redis-cli -p 8080 GET foo               # → "100"  （default 的 foo 仍是 100）
```

**验证**：不同 namespace 同名 key 互不影响 ✅

---

### 2.2 不同连接独立 namespace

```bash
# 终端 A（端口 8080）：使用 ns_a
redis-cli -p 8080 SELECT ns_a           # → OK
redis-cli -p 8080 SET user alice        # → OK
redis-cli -p 8080 GET user              # → "alice"

# 终端 B（同样端口 8080，新连接）：使用 ns_b
redis-cli -p 8080 SELECT ns_b           # → OK
redis-cli -p 8080 SET user bob          # → OK
redis-cli -p 8080 GET user              # → "bob"

# 终端 A 的 user 不受影响
# （在终端A继续）GET user → 仍是 "alice"
```

**验证**：两个独立连接使用不同 namespace，相同 key 互不干扰 ✅

---

### 2.3 跨节点 namespace 一致性

```bash
# Leader 上操作
redis-cli -p 8080 SELECT ns_prod        # → OK
redis-cli -p 8080 SET api_key "sk-12345"
# → OK

# Follower 上可以读到
redis-cli -p 8081 SELECT ns_prod        # → OK
redis-cli -p 8081 GET api_key           # → "sk-12345"  ✅
```

**验证**：命名空间数据通过 Raft 日志复制到所有节点 ✅

---

### 2.4 DELETE 在命名空间中

```bash
redis-cli -p 8080 SELECT ns1            # → OK
redis-cli -p 8080 DEL foo               # → (integer) 1
redis-cli -p 8080 GET foo               # → (nil)

# 不影响其他命名空间
redis-cli -p 8080 SELECT default        # → OK
redis-cli -p 8080 GET foo               # → "100"  （default 的 foo 还在）
```

---

### 2.5 INFO 显示当前命名空间

```bash
redis-cli -p 8080 SELECT ns1
redis-cli -p 8080 INFO
```

**预期输出包含**：
```
namespace:ns1
```

---

### 2.6 非法命名空间名称

```bash
redis-cli -p 8080 SELECT "bad ns!"      # → (error) ERR invalid namespace name: bad ns!
redis-cli -p 8080 SELECT ""             # → (error) ERR invalid namespace name:
redis-cli -p 8080 SELECT "aaaa...aaa"   # > 63 个字符 → (error) ERR invalid namespace name: ...
```

**命名空间名称规则**：
- 允许字符：字母、数字、下划线、连字符（`[a-zA-Z0-9_-]+`）
- 长度：1~63 字符
- 默认值：`default`

---

## 三、完整场景模拟（推荐流程）

如果你只想快速验证全部功能，按以下顺序做一遍即可：

```
# 1. 启动三节点（三个终端）
Terminal 1: ./raft_kv_server --node_id=0 --client_port=8080 --raft_port=9080 ...
Terminal 2: ./raft_kv_server --node_id=1 --client_port=8081 --raft_port=9081 ...
Terminal 3: ./raft_kv_server --node_id=2 --client_port=8082 --raft_port=9082 ...

# 2. 等待 3 秒

# 3. 确认 Leader
redis-cli -p 8080 INFO | grep state   # 找到 leader 端口，下面假设是 8080

# 4. 默认 namespace 写数据
redis-cli -p 8080 SET app_name "RaftKV"
redis-cli -p 8080 SET version "1.0"

# 5. 创建两个 namespace 并分别写入
redis-cli -p 8080 SELECT tenant_a
redis-cli -p 8080 SET db_host "10.0.0.1"
redis-cli -p 8080 SET db_port "5432"

redis-cli -p 8080 SELECT tenant_b
redis-cli -p 8080 SET db_host "10.0.0.2"

# 6. 验证：两个 tenant 的 db_host 不同
redis-cli -p 8080 SELECT tenant_a
redis-cli -p 8080 GET db_host          # → "10.0.0.1"

redis-cli -p 8080 SELECT tenant_b
redis-cli -p 8080 GET db_host          # → "10.0.0.2"

# 7. 验证：数据在 Follower 上也可见
redis-cli -p 8081 SELECT tenant_a
redis-cli -p 8081 GET db_host          # → "10.0.0.1"

# 8. 验证：default 命名空间未被污染
redis-cli -p 8081 SELECT default
redis-cli -p 8081 GET db_host          # → (nil)
redis-cli -p 8081 GET app_name         # → "RaftKV"

# 9. 验证故障转移
kill <leader_pid>                      # Ctrl+C 在 Leader 终端
sleep 5
redis-cli -p 8081 INFO | grep state    # → leader (已选举新 Leader)
redis-cli -p 8081 SELECT tenant_a
redis-cli -p 8081 GET db_host          # → "10.0.0.1" （数据没有丢）

# 10. 新 Leader 继续接受写入
redis-cli -p 8081 SELECT tenant_a
redis-cli -p 8081 SET db_name "mydb"   # → OK
redis-cli -p 8082 SELECT tenant_a
redis-cli -p 8082 GET db_name          # → "mydb"
```

---

## 四、Python 自动化测试

如果不想手工操作，可以用 Docker 一键跑全部测试：

```bash
docker build -t raft-kv-dev docker/

docker run --rm -v "$PWD":/workspace -w /workspace --network host raft-kv-dev bash -c '
set -e

# Build
mkdir -p build2 && cd build2 && cmake .. -DCMAKE_BUILD_TYPE=Release && make -j$(nproc)

# Purge old data
rm -rf /tmp/kv_db_0 /tmp/kv_db_1 /tmp/kv_db_2
rm -rf /tmp/raft_log_0 /tmp/raft_log_1 /tmp/raft_log_2

PEERS="0:127.0.0.1:9080,1:127.0.0.1:9081,2:127.0.0.1:9082"

# Launch cluster
for i in 0 1 2; do
    build2/raft_kv_server \
        --node_id=$i --client_port=$((8080+i)) --raft_port=$((9080+i)) \
        --db_path=/tmp/kv_db_$i --raft_log_path=/tmp/raft_log_$i \
        --peers="$PEERS" --logtostderr=true --minloglevel=1 2>/dev/null &
done
sleep 4

# Helper: RESP client
send() {
    python3 -c "
import socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.settimeout(3)
s.connect(('127.0.0.1', $1))
s.sendall($2)
print(repr(s.recv(1024)))
s.close()
"
}

echo "=== Raft: leader election ==="
for p in 8080 8081 8082; do
    echo -n "Port $p: "; send $p "b\"*1\r\n\$4\r\nINFO\r\n\""
done

echo "=== Raft: write + replicate ==="
send 8080 "b\"*3\r\n\$3\r\nSET\r\n\$4\r\ntest\r\n\$5\r\nvalue\r\n\""
sleep 0.5
for p in 8080 8081 8082; do
    echo -n "Port $p GET: "; send $p "b\"*2\r\n\$3\r\nGET\r\n\$4\r\ntest\r\n\""
done

echo "=== Namespace: isolation ==="
send 8080 "b\"*2\r\n\$6\r\nSELECT\r\n\$3\r\nns1\r\n\""
send 8080 "b\"*3\r\n\$3\r\nSET\r\n\$3\r\nfoo\r\n\$2\r\nv1\r\n\""
send 8080 "b\"*2\r\n\$6\r\nSELECT\r\n\$7\r\ndefault\r\n\""
send 8080 "b\"*3\r\n\$3\r\nSET\r\n\$3\r\nfoo\r\n\$2\r\nv2\r\n\""
sleep 0.5
echo -n "ns1 GET foo: "
send 8080 "b\"*2\r\n\$6\r\nSELECT\r\n\$3\r\nns1\r\n\""
send 8080 "b\"*2\r\n\$3\r\nGET\r\n\$3\r\nfoo\r\n\""
echo -n "default GET foo: "
send 8080 "b\"*2\r\n\$6\r\nSELECT\r\n\$7\r\ndefault\r\n\""
send 8080 "b\"*2\r\n\$3\r\nGET\r\n\$3\r\nfoo\r\n\""

echo "=== Namespace: cross-node ==="
echo -n "Follower ns1 GET foo: "
send 8081 "b\"*2\r\n\$6\r\nSELECT\r\n\$3\r\nns1\r\n\""
send 8081 "b\"*2\r\n\$3\r\nGET\r\n\$3\r\nfoo\r\n\""

echo "=== Done ==="
'
```

---

## 关键概念总结

| 概念 | 说明 |
|------|------|
| Leader 选举 | 三节点通过 RequestVote 选出 Leader，term 递增保证唯一性 |
| 日志复制 | 写命令追加到 Raft Log → 广播 AppendEntries → 多数确认 → 提交 |
| 状态机应用 | 已提交日志按序应用到 RocksDB |
| MOVED 重定向 | 非 Leader 写操作返回 `-ERR MOVED <leader_id>` |
| 故障转移 | Leader 宕机后 ~300ms 内选举新 Leader，数据不丢 |
| Namespace | 通过 `SELECT <ns>` 切换命名空间，不同 namespace 的 key 带前缀存储 |
| 数据隔离 | `foo` 在 `ns1` 和 `default` 中是完全独立的两条记录 |
| 跨节点一致 | namespace 信息隐含在 Raft 日志中（key 前缀），自动复制到所有节点 |
