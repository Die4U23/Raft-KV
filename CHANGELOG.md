# 更新日志

记录行为改动、验证状态与取舍。每次优化保留原始基线、实现和验收证据；没有实测对比时不填写性能提升比例。

截至 **2026-09-22**，仓库 HEAD 为 `cb1b3f8`（`main`，已合并 PR #1–#10）。09-13 之后的条目此前未写入本文件；下文按提交与代码核对补录，不以合并说明或未归档压测数字作为收益证明。

## 2026-09-22 — 读屏障关联与连接队列记账

- ReadIndex 不再用独立的 `round_id` 去对某个 peer 的 `rpc_id`。读先进入尚未发出探测的批次；只有该批入队之后新分配的 AppendEntries 响应才算确认。已有在途 RPC 的响应不能给这批读过障。后到的读进入下一批，确认回调在轮次弹出之后才执行。
- 单节点由 Leader 自己的一票立即确认。五节点需要两名 Follower 对本次探测的响应，一名不够。
- 服务端把 ReadIndex 完成投递回 EventLoop，避免在 `HandleAppendEntriesResponse` 栈上开始下一条命令。
- 每条已解析命令按入队时的 RESP 字节记账，完成或断连时释放同一数值。全局已解析命令上限 16 MiB；达到每连接或全局上限时暂停读取，有空位后恢复。
- `core_tests` 增加四项生产 `RaftNode` 回归：请求前 ACK、回调里的下一次读、五节点多数派、单节点。未重跑 Linux 三进程分区。

## 2026-09-22 — 架构检查与 P1/P2 修补（生产路径仍有残留）

- 对提交 `831a91c` 做了架构检查，结论写入 [architecture-review-2026-09-22.md](docs/architecture-review-2026-09-22.md)。检查指出 ReadIndex 与连接队列存在正确性和资源边界缺陷，当时不能按 README 认定线性一致读已可靠完成。
- PR #9（`60ddf84`）修补审查中的 P1：
  - **F1**：AppendEntries 响应用 `rpc_id` 关联读轮次，不再把任意当前任期回复计入所有未确认 round。
  - **F2**：`--linearizable_reads=true` 时 Follower 的 GET 返回 `MOVED`，不再静默走本地读。
  - **F3**：每连接队列上限 1000 条 / 4 MiB，满队列停止读取。
  - **F4**：未知命令与参数错误走统一完成路径，避免连接卡住。
- PR #10（`673fbb6`）修补 P2：
  - **F5**：非法命令以 `ERROR` 类型入队，按 RESP 顺序回复，避免错误响应越过尚未完成的写。
  - **F7**：`replication_edge_cases_unit.cpp` 使用 `std::max<int64_t>`，消除 MinGW 上 `long` / `int64_t` 推导失败。
- 本轮核对生产代码后，**F1 仍未真正关闭**：读轮次 `round_id` 来自独立计数器 `_next_round_id`，匹配条件是 `round.round_id == flight.id`，而 `flight.id` 是每个 peer 各自递增的 `_rpc_sequence`。三节点一次 `BroadcastAppendEntries` 会给两个 Follower 分配连续 rpc_id，通常只有其中一个可能对上 round。新读请求在已有 in-flight 轮次时仍会挂到已经发出的 round 上。审查里“只用请求之前产生的旧 ACK 就通过读屏障”的场景不能视为已关闭。
- **F6 仍未关闭**：`tests/readindex_tests.cpp` 自实现 `ReadIndexManager`，不链接生产 `RaftNode`；`tests/connection_order_tests.cpp` 主要打印说明，`Check` 未被调用，CTest 仍计为通过。CI 只跑 `RAFTKV_BUILD_SERVER=OFF` 的可移植目标与 `*_tests.py`，不含真实三节点 Linux 服务。
- 提交说明称单测与冒烟通过。本轮未重新执行历史 Linux 分区 / 崩溃重启 / 过载归档，也没有针对修补后的 ReadIndex 做隔离旧 Leader 的真实集群核验。
- 取舍：默认 `--linearizable_reads=false`，未开开关时 GET 仍是本地读。README 已把 ReadIndex 标为完成；[read-consistency.md](docs/read-consistency.md) 与 [review-status.md](docs/review-status.md) 仍写“线性一致读未实现”，文档与代码不一致。

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
