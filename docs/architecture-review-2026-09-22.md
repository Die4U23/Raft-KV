# Raft-KV 架构检查（2026-09-22）

检查版本：`831a91cf259d77a48267e9b1c66e4ae6dc6e3da9`，本地 `main` 与本次获取的 `origin/main` 一致。

原分支 `security-fixes-phase1` 的远端分支已删除；其本地 HEAD 是主分支祖先，没有独有提交，工作区原先干净。因此保留旧分支，切换到本地 main 并快进更新。本次只新增检查报告与被 Git 忽略的复现程序，没有修改生产代码或提交、推送。

## 结论

项目具备清楚的单 Raft 组 KV 服务骨架，持久化应用位置、串行异步应用、复制 ACK 校验和故障证据归档是已有基础。但最新 ReadIndex 与连接队列改造存在正确性和资源边界缺陷，当前不能按 README 的表述认定线性一致读已可靠完成。应先修复这些缺陷，再做性能或快照扩展。

这是对生产模块、协议、构建、测试及文档一致性的架构检查，不是完整的 Raft 正确性证明。历史 Linux 归档未在本轮重新执行或逐份核验。

## 模块与运行链路

| 模块 | 当前职责 | 架构观察 |
| --- | --- | --- |
| `src/server/main.cpp` | 参数、依赖装配、RESP 接入、会话、命名空间、命令排队、跨连接组批、限流、INFO | 564 行集中承担装配和服务逻辑；全局指针及状态较多，是本轮主要回归所在 |
| `src/common` | RESP/输入缓存、组批策略、指标、重连退避 | 多数策略能独立测试；队列内存尚未贯通计量 |
| `src/namespace` | TCP 连接的命名空间选择，生成带前缀的存储键 | 是逻辑分区，不是租户认证或授权机制 |
| `src/raft/raft_node.*` | 选举、复制、提交、应用调度、ReadIndex | 由单个 EventLoop 所有；读屏障与复制状态的关联不完整 |
| `src/raft/peer_manager.*` | 节点 TCP 连接、身份字段绑定、帧分发、重连和输出限制 | 直接依赖 Muduo；sender_id 检查不等于密码学身份认证 |
| `src/raftcore/raft_log.*` | Raft 日志、term/voted_for、日志尾缓存 | 独立 RocksDB；启动全量扫描；无快照、截断前缀或压缩机制 |
| `src/raft/kv_state_machine.*` | 将复制命令转换为状态变更、生成响应 | 提交后按索引连续应用 |
| `src/storage` | RocksDB 封装、错误处理、KV 与 lastApplied 原子批写 | 避免只持久化 KV 而丢失应用进度；不等于两个数据库的跨库事务 |
| `src/raft/serial_apply_executor.*` | 单工作线程执行状态机批次，完成结果回到所有者线程 | 一批在途，顺序与生命周期边界较清楚 |
| `tests / scripts / docs/benchmarks` | 可移植测试、Linux 构建、集群故障及证据核验 | 验证分层已有基础，但新增测试存在生产代码覆盖空洞 |

写路径：RESP → 连接队列 → 全局写队列组批 → Leader 同步写 Raft 日志 → PeerManager 复制 → 多数派提交 → 串行应用 KV 与 lastApplied → 回调回复。

读路径：默认直接读取本地 KV；开启 ReadIndex 时，Leader 尝试多数派确认后等应用位置追上再读取；Follower 入口目前存在错误回退，见 F2。

线程模型：客户端接入、共识和节点网络共用所有者 EventLoop；KV 应用默认走串行工作线程。Raft 日志同步落盘仍在 EventLoop 上，因此存储延迟会阻塞其他连接、心跳和读屏障；异步应用不代表整个持久化链路已异步化。

复制模型：固定成员、单 Raft 组，每个 peer 一条在途 AppendEntries，超时重发；吞吐受批大小、同步落盘、网络往返和串行应用共同限制。目前没有动态成员、多分片、请求去重、快照恢复或 CheckQuorum/PreVote 实现。

## 需要优先修复的问题

### F1 · P1：ReadIndex 接受读请求之前生成的旧 ACK

位置：`src/raft/raft_node.cc:296`，以及 `RequestReadIndex` / `StartHeartbeatRound`。

处理 AppendEntriesResponse 时，先把当前任期的响应加入所有未确认读轮次，之后才在 321 行起检查复制 RPC 的关联关系。读轮次没有将确认绑定到读请求之后发出的探测；存在旧在途 RPC 时，BroadcastAppendEntries 甚至可能不发送新请求。仅凭回复到达时间不足以证明它发生在读请求之后。

本轮使用真实 RaftNode 和已有存储/网络测试替身复现：先产生并扣住一个真实成功 ACK，再发起读、丢弃新轮次所有消息，只投递旧 ACK，读回调仍成功。输出为 `Read accepted with only a response generated BEFORE request: YES (BUG)`。

影响：隔离的旧 Leader 可能凭延迟回复越过读屏障；这破坏“新读请求获得多数派确认”的前提。读请求追加到已经发送的轮次也需要相同的先后关系约束。

建议：读请求先进入待发批次，再为该批次发送带关联标识的确认请求；只接纳能证明属于该批次的 ACK，后来到达的读进入后续批次。复制 RPC 的关联检查和读屏障确认应有明确共享协议，不能依赖收到回复的时间。

### F2 · P1：开启线性一致读后，Follower 仍返回本地数据

位置：`src/server/main.cpp:350`、382 行附近。

ReadIndex 分支要求 `linearizable_reads && IsLeader()`；Follower 会进入本地读分支，而该分支只根据 `leader_only_reads` 拒绝请求。后者默认 false，README 的启动示例只开启前者。因此启用 linearizable_reads 并不能阻止 Follower 返回落后副本的数据。

建议：先按一致性模式分流；线性一致模式下非 Leader 应明确拒绝或重定向，不能静默降级为本地读。增加直接读取落后 Follower 的真实服务测试。

### F3 · P1：连接命令队列绕开内存限额

位置：`src/server/main.cpp:431`，特别是 446、467 行。

解析后立即从 g_input_bytes 扣除原始输入，但 args 被移动到 command_queue；队列没有条数或字节上限。OnClientMessage 在当前命令等待提交时仍继续解析新输入。失去多数派或持续慢请求时，一个连接可持续把数据迁移到未计量队列中，绕过输入缓存和全局写队列上限。

建议：对原始输入、已解析队列、已提交请求分别记账，设置连接级和全局字节/条数预算；达到高水位暂停读取，低水位恢复，断连和完成时释放预算。不能仅限制每轮解析 128 条。

### F4 · P1：未知命令或错误 SET/DEL 参数会卡住连接

位置：`src/server/main.cpp:453` 和 `ExecuteNextCommand`（326 行起）。

例如 `BOGUS`、缺少 value 的 SET 不满足 WRITE 条件，也不满足现有错误参数筛选条件，因而作为 READ 入队。执行器把 executing 置为 true 后，只处理 PING/SELECT/GET/INFO，没有最终兜底分支。这条命令不返回错误、不调用完成函数，后续请求也无法继续执行。

建议：集中进行完整命令/参数校验，将校验结果也作为有序队列项；执行器应保证每个入口都恰好完成一次。

### F5 · P2：参数错误响应可以越过前面的写响应

位置：`src/server/main.cpp:457`。

同一 TCP 数据包含合法 SET 和错误参数 GET 时，DrainClient 对 GET 直接 SendReply；此时前面的 SET 可能尚未提交，甚至尚未开始执行。客户端按顺序匹配 RESP 响应时会把后一个命令的错误误认为前一个请求的回复。

建议：完整但不合法的命令也进入同一响应序列；协议帧错误应采用明确的终止连接策略，不能任意插入回复。

### F6 · P1：测试通过不能覆盖上述生产路径

位置：`tests/readindex_tests.cpp:24`、`tests/CMakeLists.txt:101`、`tests/connection_order_tests.cpp`、`.github/workflows/portable.yml:42`。

- ReadIndex 单测自行实现 ReadIndexManager，没有链接真实 RaftNode；模拟类检查 round_id，生产实现却未执行同样的关联检查。
- connection_order_tests 主要打印说明，Check 没有被调用，实际完成 0 次检查，但仍被 CTest 统计为通过。
- 多个新增复制测试同样测试自行编写的 tracker/manager，应明确它们是模型示例，不能替代生产实现回归。
- CI 只运行可移植服务关闭构建和 `*_tests.py`；`test_linearizable_read.py` 等集成脚本不匹配该模式，也没有单独的 Linux 服务验证 job。

建议：新增边界场景直接驱动真实 RaftNode 和生产连接调度器；将说明程序从通过率统计中移出；真实 Muduo/RocksDB 三节点测试建立独立 CI 层级。

### F7 · P2：新增“可移植”测试在 Windows 无法编译

位置：`tests/replication_edge_cases_unit.cpp:96`。

`std::max(1L, next_index_[peer_id] - 1)` 在本机 MinGW/GCC 13.2 上失败，两个参数分别为 long 和 int64_t（long long）。这不是测试运行失败，而是该测试目标根本未完成构建。

建议：显式使用 int64_t 类型，例如 `std::max<int64_t>(1, ...)`，并在需要承诺 Windows 可移植性时增加对应构建检查。

## 本轮验证

环境：Windows，GCC 13.2，CMake/Ninja，Python 3.12；新建独立且被 Git 忽略的 `build-architecture-review`，没有复用旧二进制。

| 检查 | 结果与边界 |
| --- | --- |
| Git 同步 | main 与本次获取的 origin/main 均为 831a91c；未删除旧本地分支 |
| Python `*_tests.py` | 42/42 通过；不包含真实集群集成脚本 |
| 全部可移植 C++ 目标构建 | 失败：F7 的类型推导错误 |
| 排除不可编译目标，单独构建其余目标 | 11 项构建并运行通过；包含上述低证明力测试，不能视为全套架构通过 |
| ReadIndex 生产实现最小复现 | F1 已复现；真实共识源码，替身网络/数据库，不是 Linux TCP 故障实测 |
| Linux 服务、真实 RocksDB、三进程故障 | 本轮未执行 |

复现源码位于 `build-architecture-review/readindex_review_repro.cpp`，复用 tests/core_tests.cpp 的集群驱动并链接此次新编译的生产共识/存储对象文件。它是诊断程序，发现缺陷时输出 YES (BUG)，不是已经修复后的回归测试。

## 建议的架构整理顺序

1. **先恢复语义边界。** 修复读屏障关联、Follower 拒绝、连接队列预算、非法命令完成和错误回复顺序；加入针对生产路径的回归后再更新 README 的保证范围。
2. **拆出连接调度。** 从 main.cpp 提取 ClientSession/CommandDispatcher，将命令、错误、超时和断连归入一个有界、有序的完成模型；main 保留配置与生命周期装配。
3. **规范回调契约。** RaftNode 头文件要求回调不得同步重入，但服务器完成回调会立即 ExecuteNextCommand，下一条 GET 又可能调用 RequestReadIndex。应先从容器移出完成批次，再向 EventLoop 投递应用回调，避免迭代读队列期间修改同一队列。
4. **强化共识依赖边界。** 通过窄传输接口注入 PeerManager，按实际职责建立 consensus/storage/server 库目标，避免生产实现和测试模拟逐渐分叉；统一时钟注入以验证超时、重试和读轮次。
5. **补齐运行治理。** 明确可信内网部署前提、认证需求、停机清理、读请求取消、队列指标和故障告警；静态 peer ID 校验不应被表述为网络认证。
6. **最后扩展能力与优化。** 在正确性回归和真实集群验证稳定后，再做快照/压缩、客户端请求去重、PreVote/CheckQuorum；异步日志或复制流水线应依据分阶段测量推进。

当前结构无需推倒重写。优先修补新增功能跨越的共识确认、资源计量和回调调度边界，比继续叠加功能更有价值。
