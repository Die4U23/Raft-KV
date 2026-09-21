# Raft-KV 代码优化审阅

审阅分支：raft-cluster-namespace  
固定提交：bc853d5fe8d0ffddbadbe736ccfe5100c7610b8c  
范围：原始源码审阅 + 原始 RESP 解析器、NamespaceManager 的局部编译复现。未构建完整 Linux 服务，未运行三节点集群或压测。本文保留修改前的审阅结论；后续本地修改与验证范围见 [本地优化状态](Raft-KV/LOCAL_REVIEW_STATUS.md)。

## 结论与执行顺序

建议保留“接入层—共识核心—状态机—存储”的已有划分，优先修复协议解析、日志恢复、复制确认与存储错误处理。随后建立可重复的故障测试，再进行批量复制、批量持久化和线程模型优化。

第一批：协议解析和消息校验。  
第二批：日志恢复、持久化顺序、复制确认和角色转换。  
第三批：同连接命令顺序、请求生命周期、读取语义。  
第四批：有上限的批量复制、写入合并、性能测量。

## 1. RESP 半包丢失与非法输入接受【已局部复现】

位置：[服务入口 L127](https://github.com/Die4U23/Raft-KV/blob/bc853d5fe8d0ffddbadbe736ccfe5100c7610b8c/src/server/main.cpp#L127)、[RESP 解析器](https://github.com/Die4U23/Raft-KV/blob/bc853d5fe8d0ffddbadbe736ccfe5100c7610b8c/src/common/resp_parser.h)。

接入层通过 retrieveAllAsString 取走所有字节，Parse 每次重新覆盖静态缓冲区。遇到不完整命令后，残余数据无法和下一次接收拼接。ParseBulkString 还会直接跳过末尾两个字节，没有校验是否为 CRLF；stoi 也接受带垃圾后缀的数字。

对原始解析器的复现结果：

| 输入 | 实际结果 |
|---|---|
| 完整 PING | 解析出 1 条命令 |
| 两条完整 PING 放在一起 | 解析出 2 条命令 |
| 一条 PING 在 13 个内部字节边界分别切成两次输入 | 13 种情况全部丢失命令 |
| Bulk string 末尾使用 xx 代替 CRLF | 被接受 |
| 数组长度使用 1junk | 被接受 |

优化方案：

- 增加“完整 / 还需数据 / 非法”三态返回，同时返回已消费字节数。
- 直接查看每个连接的 Muduo Buffer；仅在完整命令解析成功后消费对应字节，保留尾部半包。
- 去掉解析器共享的 static 可变状态；常见半包不作为异常或错误日志。
- 严格校验数字、负数语义、CRLF、参数数量和长度，并设置单命令与连接缓冲区上限。
- 内部状态机使用结构化 Command，避免重复解析客户端 RESP；协议解析只发生在接入边界。

参考接口：

```cpp
enum class ParseState { Complete, NeedMore, Invalid };

struct ParseResult {
    ParseState state;
    std::size_t consumed;
    std::vector<std::string> args;
};

ParseResult TryParseOne(std::string_view bytes);
```

验收：对合法请求的所有切分位置、多段输入、完整命令加半包、非法长度和 CRLF 分别断言结果。

## 2. 日志末尾与状态机恢复【源码确认；未跑重启实验】

位置：[RaftLog 构造函数 L5](https://github.com/Die4U23/Raft-KV/blob/bc853d5fe8d0ffddbadbe736ccfe5100c7610b8c/src/raftcore/raft_log.cc#L5)。

_last_index 初始化为 0 后，构造函数调用 Get(LastIndex())。这实际读取了用于硬状态的 0 号键，并未定位持久化日志末尾。因此重启后的复制起点不可信，后续写入还可能覆盖旧索引。

优化方案：

- 明确区分日志键和元数据键，例如使用独立前缀或 Column Family。
- 通过日志迭代器定位末尾，或者将 lastIndex 元数据与日志追加、截断放进同一个数据库的 WriteBatch 中更新。
- 保存状态机 lastApplied，并与业务数据在“状态机所在的同一个 RocksDB 实例”中原子更新。
- Raft 日志与状态机目前是两个数据库实例，不能把它们当成一个可跨库原子提交的 WriteBatch；通过持久化顺序和重放协议衔接。
- 恢复时区分已应用数据、已确认提交日志和未提交尾部，禁止把本地所有日志直接当成已提交日志重放。

验收：连续写入后重启单节点；保留未提交尾部后重启；验证新日志索引、旧数据和后续复制。验证过程中不得清空旧数据目录。

## 3. 复制确认位置不等于 Follower 日志长度【源码推导出的安全性风险】

位置：[Follower 响应 L305](https://github.com/Die4U23/Raft-KV/blob/bc853d5fe8d0ffddbadbe736ccfe5100c7610b8c/src/raft/raft_node.cc#L305)、[Leader 处理响应 L338](https://github.com/Die4U23/Raft-KV/blob/bc853d5fe8d0ffddbadbe736ccfe5100c7610b8c/src/raft/raft_node.cc#L338)。

Follower 成功时返回自身 LastIndex，Leader 直接把它赋给 matchIndex。Follower 可能在共同前缀后保留旧 Leader 的未提交尾部；“本地有这么长”不能证明“这些条目与当前 Leader 相同”。较早心跳的成功响应若在新提议之后到达，就可能高估新条目的复制进度。

优化方案：

- 响应明确表示本次请求确认的匹配位置：连续日志情况下，为 prevLogIndex + entries_size。
- Leader 根据对应请求更新进度；同一领导任期内，成功确认不得让 matchIndex 倒退。
- 对延迟、过期的成功或失败响应建立处理规则，可先限制每个 peer 一个在途复制请求以降低复杂度。
- Follower 提交推进不得超过该请求已经确认匹配的范围。
- prevLogIndex / prevLogTerm 不匹配时先拒绝；只在实际收到的新条目与本地条目冲突时截断，且保护已提交前缀。

验收：共同前缀加旧未提交尾部、延迟心跳响应、冲突回退、不同复制批次。断言任何已提交条目在多数节点上具有相同 index、term 和 command。

协议依据：[Raft 论文 Figure 2](https://raft.github.io/raft.pdf)。

## 4. 持久化失败仍沿成功路径推进【源码确认】

位置：[日志追加 L37](https://github.com/Die4U23/Raft-KV/blob/bc853d5fe8d0ffddbadbe736ccfe5100c7610b8c/src/raftcore/raft_log.cc#L37)、[状态机 Apply L48](https://github.com/Die4U23/Raft-KV/blob/bc853d5fe8d0ffddbadbe736ccfe5100c7610b8c/src/raft/kv_state_machine.cc#L48)。

Append 返回 void，错误只记录日志；调用方无法可靠阻止后续复制确认。状态机忽略 Put / Delete 的结果，并返回成功。Get 的 bool 也把“不存在”和“I/O 错误”合并了。

优化方案：

- 存储接口返回明确状态，区分 NotFound、成功和存储故障。
- 日志、任期和投票持久化失败时，禁止给出成功确认或继续使用未经持久化的状态。
- 状态机应用失败时停止推进 lastApplied 和成功回调，并将节点置为故障状态或有序退出；不要跳过失败条目继续执行后续日志。
- 明确容错目标。RocksDB 默认非同步写不等于机器掉电后仍然持久，若承诺该故障模型，Raft 日志和硬状态必须在响应前完成相应同步持久化。
- 先实现可验证的同步基线；后续通过批量同步降低成本，仍须保证持久化完成后才能确认。

验收：注入 Append、SaveHardState、Apply 的写失败，确保无伪成功、无错误索引推进。进程终止测试和机器掉电模型应分开说明。

依据：[RocksDB 官方写入说明](https://github.com/facebook/rocksdb/wiki/Basic-Operations#synchronous-writes)、[Raft 论文 Figure 2](https://raft.github.io/raft.pdf)。

## 5. Raft 帧长度和 peer ID 校验不足【静态审阅】

位置：[帧解码 L67](https://github.com/Die4U23/Raft-KV/blob/bc853d5fe8d0ffddbadbe736ccfe5100c7610b8c/src/raft/raft_codec.h#L67)、[帧分发 L168](https://github.com/Die4U23/Raft-KV/blob/bc853d5fe8d0ffddbadbe736ccfe5100c7610b8c/src/raft/peer_manager.cc#L168)。

当前只检查 payload_len 的最大值，没有检查其至少包含 1 字节类型和 4 字节 sender_id。小于 5 的长度可触发越界读取及无符号减法下溢。另一个问题是未验证的 sender_id 会进入 Raft 处理，而核心使用 peer ID 作为 vector 下标；非连续配置和非法 ID 都需处理。

优化方案：

- 同时校验最小长度、最大长度、完整帧长度、消息类型与字段合法性。
- 在分发前验证 sender 属于配置成员；已建立的连接与 peer 身份绑定。
- 明确要求 ID 连续并在启动时校验，或者用 id → slot 映射访问进度数组。
- 使用统一 Codec 处理长度，避免 PeerManager 和 Codec 中的重复逻辑漂移。

本次没有执行可能触发未定义行为的大分配或越界输入，只做源码判断。

## 6. 角色转换、同连接顺序和请求生命周期【静态审阅】

[RaftNode](https://github.com/Die4U23/Raft-KV/blob/bc853d5fe8d0ffddbadbe736ccfe5100c7610b8c/src/raft/raft_node.cc) 中，Candidate 收到同任期的 Leader AppendEntries 时不会转换成 Follower，应补充此状态转换。投票计数建议按 peer 去重，不能简单累计响应次数；这是对重复消息的防护，不能将 TCP 重传误解成必然出现应用层重复消息。

[服务入口](https://github.com/Die4U23/Raft-KV/blob/bc853d5fe8d0ffddbadbe736ccfe5100c7610b8c/src/server/main.cpp) 中，SET 异步等待提交，而紧随其后的 GET / PING 可立即发送回复。同连接管线请求因此可能出现执行或响应顺序问题。

优化方案：

- 第一版用每连接命令队列顺序执行；写入等待提交和应用后再执行该连接下一条命令。其他连接仍可并行推进。
- 仅给回复排序不够，还必须约束“写后读”的执行顺序。
- 为未完成提议和发送缓冲区设置数量、字节上限，避免失去多数派后持续积压。
- 使用弱连接引用或可取消的响应上下文，防止等待中的提议长期持有已断开的连接。
- 超时只表示调用方不知道最终结果，不能把可能提交的日志直接删除。若提供重试去重，需引入请求 ID 和可恢复的去重状态。

验收：同连接 SET→GET、SET→PING，断开客户端，失去多数派后持续请求，恢复多数派后检查结果和资源占用。

## 7. GET 的一致性应明确设计【能力边界】

[当前 GET 路径](https://github.com/Die4U23/Raft-KV/blob/bc853d5fe8d0ffddbadbe736ccfe5100c7610b8c/src/server/main.cpp#L155) 直接读本地状态机。允许 Follower 本地读可以作为明确标注的弱一致接口；仅检查 IsLeader 不足以实现线性一致读。

建议分阶段：

1. 文档先清楚标注本地读语义。
2. 如需强读，先实现经日志提交并按序执行的读屏障或读取命令，建立简单正确的基线。
3. 再优化为 ReadIndex：具备当前任期的提交依据，向多数派确认领导权，记录读取屏障，并等待 lastApplied 追上后才读取。
4. 使用网络分区场景验证被隔离的旧 Leader 无法继续成功响应强读。

协议依据：[Raft 论文第 8 节](https://raft.github.io/raft.pdf)。

## 8. 性能优化应针对现有路径

| 优化点 | 当前证据 | 建议与验收 |
|---|---|---|
| 有上限的批量复制 | SendAppendEntries 将落后节点缺少的全部日志塞进一个 RPC，接收端却设置 10 MiB 上限 | 同时设置条数与序列化字节上限，配置单条命令上限；验证积压超过 10 MiB 时仍能追赶 |
| 合并日志写入与同步 | 每条日志单独 Put，每次 Propose 都立即广播 | 按字节数、条数和最大等待时间成批写入及复制；同步完成后才确认，比较延迟与吞吐 |
| 降低重复复制 | 新提议到达会再次发送尚未确认的日志 | 维护每个 peer 的复制进度和在途请求，先实现单在途，再按测量结果扩大窗口 |
| 避免存储拖慢心跳 | RocksDB 操作和 Raft Tick 共享 EventLoop 线程 | 先测写入延迟与选举抖动；需要时使用有界、按序的存储执行队列，完成事件回到 Raft 所属线程 |
| 减少重复解析和拷贝 | RESP 在接入层和状态机分别解析，广播逐次序列化 | 统一内部 Command，按批次复用序列化结果；在正确性完成后测量收益 |

不建议直接将 Raft 改成多个线程并发访问。保持核心状态由一个执行线程拥有，磁盘和其他任务通过有序消息协作，更容易验证顺序。

## 9. 测试与工程整理

- tests 目录目前只有占位文件，文档中的预期输出不能当作测试通过证据。
- NamespaceManager 的局部复现确认：同连接保留 tenant_a，新连接回到 default。因此 TESTING.md 中多次独立 redis-cli 调用不能验证连接级 SELECT。
- 将测试拆为解析器/编码器单元测试、可注入时钟和消息的 Raft 核心测试、真实三进程集成测试。
- 使用固定随机种子与可控消息调度测试丢消息、延迟和分区；额外验证重启和持久化故障。
- 拆分 main.cpp 的组装、命令分发、回复编码与连接状态；用 RAII 管理 RocksDB 所有权并禁止意外复制。
- 删除状态机对 RESP 的重复实现；将必要依赖与可选依赖分开，不把依赖压缩包的存在算作已经集成该库。
- 基准报告固定硬件、进程部署方式、持久化模式、value 大小、读写比例和并发数，同时记录错误率与 P95/P99。

## 本次验证记录

局部复现通过 C++17 编译，原始解析器与 NamespaceManager 的业务逻辑未修改；仅以简单输出替代 glog 日志依赖。结果见 [protocol_probe.results.txt](review/protocol_probe.results.txt)。

这不是完整服务测试，不证明 Raft 集群、RocksDB 恢复或性能已经通过。上述 Raft 和存储结论均来自固定提交的源码检查及协议规则核对。
