# 更新日志

记录行为改动、验证状态与取舍。每次优化保留原始基线、实现和验收证据；没有实测对比时不填写性能提升比例。

截至 **2026-09-24**，下文按提交与代码核对记录。09-13 之后的条目曾漏记，已补录；不以合并说明或未归档压测数字作为收益证明。标签 `v0.3.0` 的范围写在下一节。它不在 `main`（`531fcf4`），也不在 `v0.2.0`（`8f5142f`）。这个标签之后的条目另写，不改标签本身。

## v0.3.0

相对 `v0.2.0`（Pre-Vote）包含快照与日志回收、请求去重、版本化配置、joint 成员变更、多分片、集群与客户端认证、租约读，以及 `scripts/demo_three_nodes.py`。默认仍是单分片、空令牌、`--lease_reads=false`、`--require_request_id=false`。

这个标签没有干净机器从零安装的证据包，也没有 Docker 构建归档。完整请求超时没有。Follower 时钟快过 10 ms 时，租约读不安全。版本 1 快照元数据仍整份读入内存。

## 2026-09-24 — 租约联系时间改成发送时刻

`--lease_reads` 原先在 AppendEntries 或 InstallSnapshot 的应答到达时记下联系时间。跟随者在收到请求时就开始选举计时，回程再慢一截。漂移上界 10 ms 已经从 150 ms 里扣掉，应答只要比发送晚，租约就可能盖过跟随者最早的竞选。

现在只在 rpc 序号对得上的应答里更新联系时间，记的是这一次 RPC 第一次发出的时刻，只往前移，重试不刷新。对不上的应答不改联系时间。Leader 自己仍记当前时刻。复制延迟统计仍用进程单调钟上的 `first_send`，不改成测试钟。

进程内回归把心跳的去程和回程各拨 30 ms：发送后 60 ms 租约仍服务读；再拨到跟随者最早能竞选的时刻，租约不再本地完成，随后重复投递旧应答也不恢复租约。可移植 CTest 在这一节提交后重跑。Linux 三进程冒烟这次没有重跑。

这一节不在标签 `v0.3.0`。跟随者的时钟如果在一个选举超时里快过 10 ms，租约读仍不安全。这 10 ms 只留给时钟，不再同时支付网络延迟。

## 2026-09-24 — 旧快照打开时改成分片

版本 1 的日志快照把整份镜像放在一个键里。打开这种日志时，先丢掉未完成的暂存分片，再按 1 MiB 把那一份镜像写成现在的分片键，元数据改成版本 2，并删掉原来的整份键。改完后 `RaftLog` 不再留着那份字符串，之后按区间读取。这一次打开仍要读出那个旧键才能拆开，拆开后就释放。已经是版本 2 的快照不走这条路径。

进程内回归用一份比 1 MiB 多 3 字节的版本 1 镜像打开：元数据变成 21 字节且版本字节为 2，整份键消失，分片键是两块，末尾 3 字节仍是 `old`，后面追加的日志在再次打开后还在。可移植 CTest 16/16 通过。Linux 三进程冒烟这次没有重跑。

这一节不在标签 `v0.3.0`。Follower 时钟快过 10 ms 时，租约读仍不安全。

## 2026-09-24 — 请求超时

`--request_timeout_ms` 默认 0，范围 0–60000。0 时写入仍等到应用或这台节点卸任。大于 0 时，已经提案的命令和成员转发到达这个时间就回复一次 `-ERR request timeout; outcome unknown`，回调从待回复表里拿掉。日志条目留下，后来的提交仍会应用，但不会再回第二封。还在写队列里、尚未提案的命令到期后丢掉，回复 `-ERR request timeout`。单节点提案在同一次调用里提交，这条超时用不上。超时不表示回滚。

进程内回归把 Leader 隔开，只拨这台节点的钟：49 ms 不回复，50 ms 回复一次且仍是 Leader，提交索引还没越过这条日志。分区恢复后跟随者读到该键，回调仍是一次。`request_timeout` 计数为 1。可移植 CTest 16/16 通过，其中 `raft_node_coverage_tests` 19 个场景。Linux 三进程冒烟这次没有重跑。

这一节不在标签 `v0.3.0`。Follower 时钟快过 10 ms 时，租约读仍不安全。版本 1 快照元数据仍整份读入内存。

## 2026-09-24 — 冒烟失败日志

Linux 集群构建失败时，把该步骤日志的末尾打到标准错误。`cluster_smoke.py` 在节点启动后立刻退出、连接出错，或日志里出现 `Address already in use` 时，换一套端口再跑一次。读到的值和预期不同时不重试。

## 2026-09-24 — 三节点演示入口

`python3 scripts/demo_three_nodes.py --binary <raft_kv_server>` 启动三个进程，选出 Leader 后写入 `demo:user` 并 `CFGSET rollout canary`，用 SIGKILL 停掉 Leader，再从新 Leader 读回这两个值。脚本复用 `tests/cluster_smoke.py` 的进程和 RESP 处理，不代替冒烟套件。干净系统从零安装的证据包仍然没有。

## 2026-09-24 — 文档目录

主目录只留 README 和本更新日志。仍在用的说明留在 `docs/` 顶层，从 [docs/README.md](docs/README.md) 进入。2026-09-21 的完成度报告、ReadIndex 设计稿、架构检查和整理过程笔记移到 `docs/archive/`。那些文件的正文没有按后来的代码改写。

## 2026-09-24 — 成员变更范围、写入序号和快照内存

可移植 CTest 16/16 通过。Linux 构建的 CTest 17/17 通过，三进程冒烟 10/10 通过。`cluster_linearizable.py` 这次没有重跑。

- `MEMBER JOIN id host port` 记住一个原来不在静态 peer 列表里的主机，再把它加为投票者。只写 `MEMBER JOIN id` 时，该 id 必须已经在本进程的 peer 表里。`host` 不能含 NUL，`port` 为 1–65535。端点写在成员记录的版本 2 里；没有新端点时成员记录仍是版本 1。
- Leader 可以 `MEMBER LEAVE` 自己。应用内部的 `MEMBER COMMIT` 之后，如果自己不再是投票者，就卸任。仍然不能把投票者减空，也不能在上一次变更未完成时再改。
- `--shards` 大于 1 时，本节点是该分片 Leader 就地提案。否则把 `MEMBER` 转给那个分片当前的 Leader（Raft 帧类型 6 和 7）。还不知道 Leader 时返回 `MOVED -1`。
- `--require_request_id` 默认 false。打开后，`SET` / `DEL` / `CFGSET` / `CFGROLLBACK` 缺少 `client_id` 和 `request_id` 时返回 `-ERR request id required`。`MEMBER` 不要求序号。默认关闭时，不带序号的写入重试仍会再执行。
- 新的日志快照按 1 MiB 分片键写入，压缩、发送、接收和安装每次处理一块。发送尚未结束时不开始下一次压缩。接收方先在暂存键里收齐并检查镜像，确认合法后才截断日志。打开旧的版本 1 快照元数据时，仍会把那一份镜像读进内存。

## 2026-09-24 — 版本化配置、成员变更、多分片、认证、租约读

默认都保持原来的行为：`--shards=1`，两个令牌为空，`--lease_reads=false`。可移植 CTest 16/16 通过。Linux 上 `raft_kv_server` 已重新链接；`linux-cluster.yml` 里的三进程冒烟和 `cluster_linearizable.py` 这次没有重跑。

- `CFGSET name value` 经 Raft 发布配置，成功回复新版本号。`CFGROLLBACK name version` 把那个版本的值复制成一个新版本；没有该版本时返回 `-ERR no such config version`，不消耗 `request_id`。`CFGGET` 读当前版本。`CFGCACHE name max_age_ms` 只读本机缓存，年龄超过上限返回 `-ERR config not fresh`，不把过期值当成功。`CFGSET` / `CFGROLLBACK` 可以带与 `SET` 相同的 `client_id` 和 `request_id`，重试不升版本。以 NUL 开头的键在分类时拒绝；若已提交，状态机返回确定的 `-ERR reserved key`。没有配置记录时快照仍是 9 字节、版本字节为 2；有记录时快照版本为 3。版本 1 和版本 2 的安装会清掉配置。
- `MEMBER JOIN` / `MEMBER LEAVE` 一次一个。追加时进入 joint 配置，提交要旧投票者和新投票者都过半数。该条目应用后，Leader 再追加内部的 `MEMBER COMMIT`；应用 COMMIT 才切到新集合并持久化。不能把集合减空，也不能在上一次变更未完成时再改。非投票者不竞选，也不给票。投票者写入 Raft 日志的成员键，InstallSnapshot 的 `voters` 字段在安装完成时采纳；该字段为空则保持当前投票者，手写测试帧不用填。加入列表外主机、Leader 自移除，以及按分片转发，见上面一节。
- `--shards` 为 1–64。大于 1 时同一组 peer 上有多份日志和 KV，键按 FNV-1a 选择分片，帧内的 protobuf 前多 4 字节分片号。各分片各自选主。
- `--cluster_token` 非空时，Raft 帧外是 `MAC1`、内层长度和 32 字节 HMAC-SHA256。校验失败关闭连接。空令牌不改帧字节。`--client_token` 非空时，未 `AUTH` 的连接除 `AUTH` 外返回 `-ERR NOAUTH Authentication required`；密码不对返回 `-ERR invalid password`。`AUTH` 不进 Raft。令牌为空时，`AUTH` 在执行期回复未知命令。
- `--lease_reads` 与 `--linearizable_reads` 都会打开强一致读。投票者 AppendEntries 联系时间里，第 quorum 新的那一次加上（150 ms − 10 ms）之前，Leader 把读排到当前提交位置并增加 `lease_reads`，仍等 `lastApplied` 之后才读。恰好等于该窗口，或窗口之外，退回 ReadIndex，不增加 `lease_reads`。过载检查在这条快路径之前。Follower 时钟快过 10 ms 时，可能在 Leader 仍认为租约有效时开始竞选。

## 2026-09-23 — 客户端请求去重

- `SET key value client_id request_id` 和 `DEL key client_id request_id` 把结果记在状态机里。同一个客户端的同一个序号再提交一次时，返回上一次的回复，用户键不变。每个客户端只保留最新序号。序号必须从 1 开始、每次加 1；跳号或旧序号返回 `-ERR stale request id`，不改键。不带这两个参数的 `SET`/`DEL` 仍会在重试时再执行。
- 会话记录和用户键在同一个写入批次里落盘。快照版本 2 带上这张表，安装后换 Leader 重试也不会再执行。版本 1 只有用户键，安装时清掉会话。`client_id` 为 1–128 字节且不能含 NUL，`request_id` 是不补零的十进制整数。
- 进程内回归覆盖：重复 `DEL` 仍返回第一次的 `1`、同一批次里的重复序号、缺口序号、重启后重试、快照安装后的重试，以及新 Leader 上的重试不改写已经生效的值。

## 2026-09-23 — 快照与日志压缩

- 已应用索引比上次快照多出 `kSnapshotDistance`（默认 1024）条之后，导出 KV 用户键并让 `RaftLog::SaveSnapshot` 丢掉日志前缀。`<= 0` 关闭压缩。镜像再大也压缩，不再因为超过 8 MiB 就保留日志；镜像在压缩和安装时整份留在内存里。Leader 和 Follower 都压缩自己的日志。快照在压缩当时冻结，之后的应用不会进入已经发出的镜像。
- 新 RPC：`InstallSnapshot` / `InstallSnapshotResponse`（帧类型 4 和 5）。镜像按 1 MiB 分片，`offset` 是本片在镜像中的位置，`done` 标记最后一片。接收方收齐后才写入日志并安装 KV。缺片或乱序会丢掉已收的半成品，下一轮从 offset 0 再传。`nextIndex` 落到快照里，或者前一条日志的任期已经读不到时，Leader 发快照而不是逐条回退。某一片失败不在同一轮里紧循环重发，留给下一次心跳。快照不是 ReadIndex 探针。
- 崩溃顺序是先把快照元数据和镜像写入日志并删除前缀，再安装 KV。重启时如果日志快照索引大于 KV 的 `lastApplied`，用日志里的镜像把 KV 补上。快照不能倒退。同一索引上任期冲突则拒绝；已经应用的日志如果和快照任期对不上，节点失败停止，不把状态机倒回去。任期相同的前缀只截断日志，不重放 KV。
- 进程内回归覆盖：前缀删除后后缀和硬状态仍能重开、冲突任期丢掉后缀、KV 镜像往返且删除旧键、落后节点安装快照后再复制后缀、Leader 的 KV 被清空后从日志快照恢复、坏镜像不抬任期、已应用日志与快照任期冲突时失败停止。冲突用例发给选举后的另一台副本，避免节点把自己发出的快照直接丢掉。

## 2026-09-23 — Pre-Vote：没有多数派同意就不要抬任期

- 选举超时进入 pre-candidate。`RequestVote.prevote` 为真，`term` 是当前任期加一。接收方不改自己的任期，也不把这一票写入 `votedFor`。Leader，以及选举截止时间还没到、并且认识 Leader 的 Follower，拒绝预投票。
- 预投票达到多数派之后，才走原来的 `BecomeCandidate()`：任期加一，发出真正的 `RequestVote`。预投票没凑齐，或者 Candidate 再次超时，回到 pre-candidate，任期不变。单节点集群仍然在同一次超时里成为 Leader。
- 应答带上同样的 `prevote`，避免过期的正式投票被当成预投票。同意预投票时，应答里的任期是接收方自己的当前任期；比自己更高的任期仍然会让预候选卸任。当前 Leader 的心跳也会把 pre-candidate 拉回 Follower。落后节点会从拒绝应答里学到更高任期，分区恢复后仍能完成选举。
- 进程内回归覆盖：隔离节点多次超时不抬任期、恢复后原 Leader 任期不变且写入仍能在三副本提交、错过一轮任期的节点在 Leader 消失后能和存活节点选出行的 Leader。

## 2026-09-23 — 选举时钟与 ReadIndex 租约对齐，并补上检查日志

- 选举超时改成 `steady_clock` 上的绝对截止时间。`Tick` 只观察截止时间，不再每次扣 10 ms。定时器提前触发时，Follower 不能提前开始竞选。ReadIndex 在探针年龄 **大于等于** 最短选举超时（150 ms）时拒绝 ACK，并在应答到达时失败该轮。两条路径用同一把钟，原先大约一个 Tick 的窗口里，延迟 ACK 不能再确认读。
- 补上 CheckQuorum。Leader 在最短选举超时之后，如果这段时间里没有多数派的 AppendEntries 应答，就在当前任期卸任，并失败未完成的读。单节点集群只计自己。没有 PreVote。
- 单节点 ReadIndex 确认后如果回调里又排了下一条，用循环开下一轮，避免按队列深度递归。
- 角色变化、拒票、读屏障超时、过期 ACK、CheckQuorum 卸任、队列打满和存储失败写到 stderr，前缀是 `INFO` / `WARNING` / `ERROR`。非法 RESP 帧和线性一致 GET 的失败或重定向也记一条。成功的读不逐条打日志。

## 2026-09-23 — 收紧读路径检查：坏帧、记账、过期探针与隔离 GET

- 非法 RESP 帧在已有执行中、等待 Raft 或队列非空时只关连接，不再插入一条错误回复，避免客户端把它当成前面 SET/GET 的应答。空闲连接仍回复该帧错误。断连或 `closing` 时丢掉在途回复并清掉 `executing`，不再把会话留在执行中。
- 线性一致 GET 在多数派确认之后、读状态机之前再看一次 `IsLeader()`。已经卸任则 `MOVED`，不再返回本地值。仍是 Leader 时的 `read index timeout` / 队列满保持本地错误。判定在 `DecideLinearizableGet`，由 `connection_order_tests` 覆盖。
- 每连接队列按线帧与参数中较大者记一次字节。此前线帧和参数相加，value 被算进 4 MiB 两次。`DrainCommands` 用真实 RESP 打到上限。
- 过期探针 ACK 到达时立刻失败该轮，不再等到下一次 `Tick`。
- Linux 隔离旧 Leader 的 GET 必须在 1 秒内收到 `read index timeout` / `MOVED` / 卸任错误。套接字超时不再算通过。

## 2026-09-23 — ReadIndex：刚听过心跳的 Follower 不得给其他候选投票

- Follower 在选举时钟尚未到期、且已知当前 Leader 时，对其他节点的 `RequestVote` **既不抬任期也不给票**（Raft thesis §4.2.3）。否则同一节点可以先给探针 ACK，再在更高任期投票；延迟 ACK 仍落在 150 ms 窗口内时，旧 Leader 会确认读，新 Leader 已经提交新值。
- `readindex_tests` 覆盖：探针之后的 disruptive vote 被拒绝、延迟 ACK 仍由原 Leader 合法确认。隔离旧 Leader 的多数派选举改为先让 Follower 超时再选（`ElectAmong`）。
- 这不是完整 CheckQuorum：隔离旧 Leader 仍保持 `IsLeader()`，线性一致 GET 靠探针超时失败，不会因失去多数派而卸任。没有 PreVote。

## 2026-09-23 — ReadIndex 剩余缺口：过期探针 ACK 与 GET 重定向

- 探针轮次只在最短选举超时（150 ms）内接受 ACK。超过该窗口的同任期 ACK 不能确认读，避免多数派已经另选 Leader 并提交新值之后，旧 Leader 仍凭延迟回复返回过期 GET。应用落后与未发送队列仍用 1000 ms。
- 请求之前已在途的 AppendEntries 在超时后重试同一 `rpc_id` 时仍然不能绑定为探针；`readindex_tests` 覆盖这条路径。
- `--linearizable_reads=true` 时，`leadership lost` / `server stopped` 与 `not leader` 一样映射为 `MOVED`。`IsReadIndexRedirectError` 与 `ClassifyCommand` 一样由 CTest 驱动。
- 应用追上后若回调里又排队了下一条 ReadIndex，立即开启下一轮，不再等到下一次 Tick。
- Linux `cluster_linearizable.py` 的读断言在上一轮 CI 已通过；job 失败是 `RaftProxyMesh.close()` 在 Raft 仍连着时 `wait_closed` 超时，随后关掉 event loop，残留 `_relay` 再 `Task.cancel()` 报 `Event loop is closed`。关闭改为 `abort` 传输、取消任务，loop 已关闭时不再 cancel；脚本在拆代理前先停服务进程。

## 2026-09-23 — F6 剩余缺口：生产连接调度器与 Linux 三节点 CI

- 每连接 DrainClient 解析/准入/记账抽到 `src/common/session_queue.h` 的 `SessionCommandQueue` / `DrainCommands`，`main.cpp` 共用。弹出时减去入队时的完整记账字节，不再只减参数长度（此前已完成命令的 RESP 帧会残留在 `queued_bytes`）。
- `connection_order_tests` 直接驱动该生产调度器：FIFO ERROR、小写动词、128 条/轮、1000 条与 4 MiB 停读、非法 RESP 不入队。
- 新增独立 CI 工作流 `.github/workflows/linux-cluster.yml`：安装系统依赖，`scripts/build_linux.py --smoke` 构建真实 Muduo/RocksDB 服务并跑三节点冒烟，再跑 `tests/cluster_linearizable.py`（Follower `MOVED`、Leader 写后读、隔离旧 Leader 不得成功返回过期 GET）。
- `cluster_linearizable_tests.py` 只检查判定器，不启动服务；`test_linearizable_read.py` 仍是手工脚本，不进入 `*_tests.py`。

## 2026-09-23 — 可移植全面回归（生产路径，非自制模型）

- 新增 CTest 目标：`kv_state_machine_tests`（SET/DEL/GET、空命令 no-op、大小写、非法已提交命令、旧库无 lastApplied 拒绝）、`raft_log_tests`（追加/截断/硬状态往返、非连续日志拒绝）、`raft_node_coverage_tests`（Follower/停机/不健康/超限提案、选举日志新旧、非法 RequestVote、Candidate 见更高任期心跳）。
- `readindex_tests` 增加 10000 条排队过载与隔离旧 Leader：少数派自确认不能完成 ReadIndex，多数派可继续写入。
- `connection_order_tests` 覆盖全部命令 arity、大小写分类，以及 F3 的 1000 条 / 4 MiB 队列上限。每连接限额抽到 `src/common/session_queue.h`，与 `main.cpp` 共用。
- `protocol_tests` 补命名空间长度、连接隔离。`cluster_smoke_tests.py` 把 RESP 自检纳入 `*_tests.py`，可移植 CI 会跑。
- 仍不把 `replication_ack_tests.cpp`（FakeRaftNode）加入 CTest；未新增 Linux 三进程 CI。

## 2026-09-22 — ReadIndex 探针关联与生产路径回归

- **F1**：读屏障不再把 `round_id` 和每个 peer 各自递增的 `rpc_id` 直接比较。读请求先进入未发送队列；一轮冻结后再发送的新 AppendEntries 才记入 `probe_rpc_ids`。只有这些 RPC 的 ACK 计入多数派。请求之前已在途的复制/心跳及其重试不能确认该读。后来的读进入下一轮。
- `Stop()` 会拒绝未完成的 ReadIndex，与卸任路径一致。
- **F6**：CTest 里原先用自制 tracker / ReadIndexManager / 空断言的目标改为链接生产 `RaftNode` 或生产 `ClassifyCommand`。`connection_order_tests` 断言真实分类规则；`readindex_tests` / `replication_*` 用进程内集群复现旧 ACK、分区、五节点多数派等路径。`replication_ack_tests.cpp` 仍是 FakeRaftNode，不加入 CTest。未新增 Linux 三进程 CI。

## 2026-09-22 — 架构检查与 P1/P2 修补（生产路径仍有残留）

- 对提交 `831a91c` 做了架构检查，结论写入 [architecture-review-2026-09-22.md](docs/archive/architecture-review-2026-09-22.md)。检查指出 ReadIndex 与连接队列存在正确性和资源边界缺陷，当时不能按 README 认定线性一致读已可靠完成。
- PR #9（`60ddf84`）修补审查中的 P1：
  - **F1**：AppendEntries 响应用 `rpc_id` 关联读轮次，不再把任意当前任期回复计入所有未确认 round。
  - **F2**：`--linearizable_reads=true` 时 Follower 的 GET 返回 `MOVED`，不再静默走本地读。
  - **F3**：每连接队列上限 1000 条 / 4 MiB，满队列停止读取。
  - **F4**：未知命令与参数错误走统一完成路径，避免连接卡住。
- PR #10（`673fbb6`）修补 P2：
  - **F5**：非法命令以 `ERROR` 类型入队，按 RESP 顺序回复，避免错误响应越过尚未完成的写。
  - **F7**：`replication_edge_cases_unit.cpp` 使用 `std::max<int64_t>`，消除 MinGW 上 `long` / `int64_t` 推导失败。
- 随后核对生产代码时 **F1 仍未真正关闭**（见本条当时记录）：`round_id` 来自 `_next_round_id`，匹配条件却是 `round.round_id == flight.id`，而 `flight.id` 是每个 peer 的 `_rpc_sequence`。该缺口由上一节关闭。
- 当时 **F6 仍未关闭**：`readindex_tests.cpp` 不链接生产 `RaftNode`；`connection_order_tests` 的 `Check` 未被调用。上一节补了生产 `RaftNode` 回归和真实断言。2026-09-23 又把 DrainClient 调度器抽到 `session_queue.h` 并由 CTest 驱动，且增加独立 Linux 三节点 CI（冒烟 + 隔离旧 Leader 线性一致读）。
- 提交说明称单测与冒烟通过。本轮未重新执行历史 Linux 分区 / 崩溃重启 / 过载归档，也没有针对修补后的 ReadIndex 做隔离旧 Leader 的真实集群核验。
- 取舍：默认 `--linearizable_reads=false`，未开开关时 GET 仍是本地读。

## 2026-09-21 / 09-22 — ReadIndex 线性一致读（默认关闭；未做 Linux 证据归档）

- 新增 `--linearizable_reads`（默认 false）。Leader 在本任期 no-op 提交后，记录当时 `commit_index` 作为读屏障，向 peer 发空 AppendEntries，多数派确认且 `lastApplied >= read_index` 后再读本地 KV。超时 1000 ms，队列深度上限 10000。卸任时拒绝未完成读。INFO 增加 `read_index_*` 计数。
- Server 层 GET 走 `RequestReadIndex` 回调，不再用 1 ms 轮询；连接用 `weak_ptr`，断连后不再回复。与每连接命令队列串行衔接：同连接写完成前不会开始这条 GET。
- 实现过程中修过一轮心跳 ack 只计入最后一个 round 的错误（`1c71092`），后被 09-22 架构检查再次指出关联标识不足，见上条。
- 验证边界：可移植单测与若干 Python 辅助脚本被报告通过；`readindex_tests` 不覆盖生产 `RaftNode`。仓库内 `benchmark-results/benchmark-summary.json` 给出混合 6261、读多 10187、纯读 11547 次/秒，这是不同读写比例下的吞吐，**不是** ReadIndex 相对本地读的对照，也没有 09-13 那种源码/二进制指纹与核验脚本。不填写性能提升比例。
- 未改写路径的同步落盘、多数派提交或去重语义。没有快照、Pre-Vote/CheckQuorum。隔离旧 Leader 不得返回过期强一致读，这一验收场景没有对应的已核验 Linux 证据包。

## 2026-09-21 — 每连接命令队列

- 问题：同一 TCP 连接上 GET/PING 原先立即执行，SET/DEL 经 Raft 异步完成，pipeline 下后发的 GET 可能先于前面的 SET 提交而读到旧值，回复顺序也可能与请求顺序不一致。
- 改动：每个 `ClientSession` 增加 FIFO `command_queue` 与 `executing` 标志。读立即执行，写提交后由完成回调驱动下一条。09-22 起非法命令也入同一队列。
- 多连接之间仍并发；单连接变为串行。未单独测量由此带来的延迟变化，不宣称吞吐收益。
- 验证：新增 `tests/test_connection_queue.py` 等辅助用例。`connection_order_tests.cpp` 不能当作生产调度器回归。真实 Linux 三节点未为该改动单独归档。

## 2026-09-21 — 存储健康追踪、协议注释与复制边界测试

- RaftNode 增加 `_storage_healthy`。Leader/Follower 日志追加与状态机应用捕获存储异常后置为不健康并重新抛出（fail-stop）；不健康时 `Propose` 返回 `-3`，拒绝新提案。不把存储失败伪装成成功或不存在。
- 新增 `tests/storage_failure_tests.cpp`：用 RocksDB 测试替身注入写失败，覆盖 Leader/Follower 追加与单节点应用失败。这证明调用顺序与健康标志，**不证明**真实磁盘损坏或掉电。
- 关键状态转换补了 Raft §5.1–§5.4 注释。新增 `replication_partition_tests.cpp`、`replication_edge_cases_unit.cpp` 等独立逻辑测试；它们是模型/追踪器示例或分区编排的可测部分，不能替代生产 `RaftNode` 在真实 TCP 上的行为。
- 同步整理了代码审阅文档、构建目录约定和测试说明。无行为对比实验，不填写性能数字。

## 2026-09-16 — 许可证、可移植 CI 与日志尾缓存

- 根目录增加 MIT `LICENSE` 与 `NOTICE`，README 补充来源与改造范围说明。
- 新增 GitHub Actions `portable.yml`：`RAFTKV_BUILD_SERVER=OFF` 构建可移植 C++ 回归，并运行 `tests/*_tests.py`。CI 仍不构建真实 Muduo/RocksDB 服务，也不跑三节点故障脚本。
- Raft 日志在扫描和追加后缓存尾部 term，避免对最后一条索引反复读盘。`storage_batch_tests` 增加对应检查。没有独立吞吐对照。
- 文档改为以可复现构建、故障证据和项目实践长文为主；去掉未跟踪的捆绑依赖。

## 2026-09-13 — 客户端回复头部合并读取（已实测，未观察到收益）

- 新增可选 `combined-header` 模式，将 RESP 类型字节与头部行合并为一次 StreamReader 读取；默认 `classic` 保留。长度限制、回复校验、部分超时计数和连接清理不变。
- 新增一键 classic / combined-header / combined-header / classic 四轮对照；复用同一服务端二进制和诊断窗口，校验跨轮负载及指纹，按成功数与总耗时汇总，保留各轮 P99。
- 客户端、协议边界、诊断及汇总辅助测试 **16/16 PASS**，包括四轮执行及失败停止检查；真实 Linux 四轮对照 **PASS**，共 400,000 次正式请求零错误，完整报告、日志与 34 个源码指纹已[核验归档](docs/benchmarks/client-header-validation.md)。
- 本轮合并读取相对 classic：合并吞吐 **6705.69 → 6602.85 次/秒（−1.53%）**，客户端 CPU 成本 **113.7 → 121.8 微秒/次（+7.12%）**。同模式轮间波动明显，未观察到收益，不宣称普遍性能退化。
- 决定：保留 `classic` 默认，合并读取仅保留为实验选项；服务端不变，不继续重跑挑选结果。实现说明与历史复现方法见[实验记录](docs/optimizations/client-overhead.md)。

## 2026-09-13 — 正常读写诊断证据归档

- 核验提交 `74880f3` 的完整原始证据，33 个编译/测试输入匹配，引用构建与既有已验收二进制一致。
- 双核 VM 同机三节点与客户端，正式 100,000 次请求零错误；吞吐约 5,146 次/秒，P99 21.663 毫秒。客户端约占单核 62.88%，整机空闲约 4.97%。
- 38 条采样位于正式窗口内；写计数差分 50,000，三副本验证 32 个键并收敛。单轮诊断不表述为重连优化 QPS 收益；[原始证据和核验结果](docs/benchmarks/profile-validation.md)已归档。

## 2026-09-09 — 正常读写性能诊断（工具发布；09-13 已验收）

- 新增一键诊断：在独立负载进程中预热后运行 10 万次请求，记录客户端、三个节点和整机 CPU，吞吐/P99 与 INFO 阶段差分。
- 给既有压测函数增加可选测量边界回调，避免将预填充、预热和关闭连接纳入正式统计；普通压测 CLI 的默认行为不变。
- 服务端 C++、数据库参数和持久化语义未修改；不将压测工具改进表述为服务性能提升。
- 本地 CPU/阶段差分与观察回调测试、原压测客户端自测通过；真实 Linux 诊断已于 09-13 [核验归档](docs/benchmarks/profile-validation.md)。执行与统计边界见[诊断说明](docs/optimizations/performance-diagnostic.md)。

## 2026-09-09 — 断线重连退避（Linux 实测已核验）

- 问题：两轮不同故障场景分别记录 24,731 和 7,897 次转发器拒绝重连。它们不是相同测试的重复样本，不互算提升比例。
- 改动：PeerManager 不再启用 TcpClient 的断线立即重连；TCP 已建立后断开时，按每个 peer 独立的 500 / 1000 / 2000 毫秒退避创建新客户端。连接连续存在满 10 秒才重置退避。
- 建连失败仍由 Connector 自行退避；不会在同一 Connector 尚连接或正在重试时重复调用 connect()。原始 Muduo 压缩包不变。
- 增加定时任务代际校验、对象生命周期保护、客户端名称代际及 Start 幂等处理。
- 验证：Windows CTest **6/6 PASS**，分区辅助检查 **8/8 PASS**。新策略覆盖反复断线、上限、10 秒重置边界、重复/旧定时任务和 peer 独立状态。
- 新增 Linux 真实 Muduo 生命周期回归，以及分区脚本可选的 `--max-reconnect-refusals` 失败阈值。实现提交 `ef17ef7` 已完成 Linux 构建、CTest **7/7**（含真实重连专项）、三节点冒烟 **10 项**、分区 **5 个阶段**验收，完整证据已核验。
- 取舍：断线后最多等待 2 秒才发起下一次连接；后续若建连失败，还会进入 Connector 原有最长 30 秒退避，不能承诺两秒内恢复。
- 未改同步持久化、多数派确认或 GET 一致性语义；本次同类分区前后对比中，拒绝重连 **24,731 → 11**（约减少 **99.96%**），完整节点日志 **5,498,325 → 146,110 字节**（约减少 **97.34%**）；未测 QPS 或 CPU 收益。

原因、源码依据和执行方法见[重连优化记录](docs/optimizations/peer-reconnect-backoff.md)。[完整实测与原始证据](docs/benchmarks/reconnect-validation.md)已归档。结论仅限本次 VM 同类场景，初始 Leader 和选举时序不同；退避可能延长恢复等待，不表述为所有场景的固定收益。

## 2026-09-09 — 三组收尾测试证据归档

- 提交 `b654d3d` 完成过载组归档；[分区](docs/benchmarks/partition-validation.md)、[写入重启](docs/benchmarks/write-restart-validation.md)、[过载](docs/benchmarks/overload-validation.md)三组原始证据均已核验。
- 重连开销作为已知问题保留；60 秒资源观察不表述为长期无泄漏或生产可用。
