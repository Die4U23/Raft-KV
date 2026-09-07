# Ubuntu 双核虚拟机：同步与异步应用对照

整理及原始证据核验日期：2026-09-07。本文归档用户在 Ubuntu 虚拟机执行测试后提供的原始报告，并已逐项核对终端摘要。四轮正式测试共完成 400,000 次请求，错误为 0；同步合并吞吐为 3656.53 次/秒，异步为 3824.56 次/秒。

这是一组受同机 CPU 竞争影响的小工作集基线。每种模式只有两轮，异步合并吞吐高约 4.60% 是本组观测差异，不是已证明的稳定优化收益或生产容量上限。

## 证据来源与缺口

- [结构化转录摘要](ubuntu-2cpu-abba-summary.json)保留四轮用户回传的数值、顺序与原始目录索引；已与原始文件核对一致。它不是原始 result.json，也不包含逐请求延迟样本。
- [原始证据包](evidence/abba-evidence-1szk2s.tar.gz)已复制到仓库工作区，保持用户提供的压缩包字节不变：53,940 字节，含四个目录、44 个普通文件；没有数据库目录。SHA256 为 `ffd6ee7ea83db1e4b8a771be5c399e1e9be782ae9eccc4d2b9abd6c9a2e04088`。此哈希用于标识收到的文件，不是独立的来源认证。
- [核验结果与逐文件哈希](evidence/verification.json)由[只读核验脚本](verify_ubuntu_abba.py)生成，覆盖配置、预热/正式请求计数、吞吐计算、逐轮分位数、INFO 差分和 CPU 汇总。核验不解压或执行压缩包内容，也不表示在 Windows 上重新运行了真实 Linux 服务。Git 提交与发布状态以仓库历史为准。
- [冒烟原始报告与事后构建快照](ubuntu-build-and-smoke.md)已另行归档并核验，包含采集时 Git HEAD、修改清单、二进制哈希记录、CMakeCache 和相关依赖版本。测试当时没有记录二进制指纹，当前文件时间与沿用同一构建的过程相符，但不能独立证明四轮使用的精确二进制身份。
- Ubuntu 上的 Boost.System 依赖调整与 Muduo HttpResponse 缓冲区/格式修补已核对，详情见构建快照；后续已纳入 [自动构建流程](../linux-build.md)，该新流程的完整 Linux 验收仍待执行。
- 节点日志中的日期为 2026-09-07，符合本次整理日期；没有独立校验虚拟机时钟。

## 测试条件

| 项目 | 条件 |
| --- | --- |
| 系统 | 用户回传 Ubuntu 26.04 LTS；四轮原始 result.json 均记录 Linux 7.0.0-28、glibc 2.43、Python 3.14.4 |
| CPU / 内存 | nproc=2，约 7.2 GiB RAM；事后 lscpu 呈现 Intel Core i5-12450H，宿主机负载未归档 |
| 磁盘 | 虚拟 80 GiB 磁盘，ext4；虚拟设备 ROTA=1 不能确定宿主机物理磁盘类型 |
| 部署 | 三个服务进程和 Python asyncio 客户端同一 VMware VM，loopback 通信 |
| 构建 | 使用 build-ubuntu-server/raft_kv_server；事后缓存确认 RelWithDebInfo，生成参数为 -O2 -g -DNDEBUG -std=gnu++17；测试时二进制身份未独立证明 |
| 请求 | 32 个连接，pipeline=1，value=128 字节，读写各 50%，namespace=bench，超时 5 秒 |
| 每轮 | 新建配套 KV/Raft 数据目录，预热 10,000 次，再正式测量 100,000 次 |
| 模式 | 顺序为异步→同步→同步→异步，全部三个节点切换同一模式 |
| 存储与读取 | 保持源码的同步持久化路径；GET 是本地读，发往 Leader 也不等于线性一致读 |
| 采样 | vmstat 每秒一次；top 每秒记录三个服务和压测客户端的线程；正式客户端退出后自动停止 |
| INFO | 预热后、正式客户端退出后各采集一次；不是持续采样，不能证明运行中 apply_lag 始终为 0 |

客户端为有界闭环负载，每个连接反复访问自己的少量 key。该结果不代表大数据集随机访问、纯写吞吐、恒定到达率过载或分机器部署的表现。监控也在同一虚拟机运行，其开销未独立测量。

## 四轮结果

| 顺序 | 模式 | Leader ID | 耗时（秒） | 成功 / 错误 | 吞吐（次/秒） | P50（ms） | P95（ms） | P99（ms） |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 异步 | 1 | 26.194463 | 100000 / 0 | 3817.60 | 8.35 | 15.02 | 18.39 |
| 2 | 同步 | 2 | 25.422402 | 100000 / 0 | 3933.54 | 7.88 | 14.69 | 20.61 |
| 3 | 同步 | 1 | 29.274282 | 100000 / 0 | 3415.97 | 8.85 | 18.27 | 25.67 |
| 4 | 异步 | 1 | 26.099124 | 100000 / 0 | 3831.55 | 8.05 | 17.19 | 26.89 |

合并吞吐 = 同模式成功请求总数 / 同模式测量耗时总和：

- 同步：200000 / (25.422401980002178 + 29.27428207801131) = **3656.53 次/秒**。
- 异步：200000 / (26.19446299399715 + 26.099124268992455) = **3824.56 次/秒**。
- 本组差异：(3824.5607247056946 / 3656.5287904449933 - 1) × 100% = **4.60%**。

P99 保留各轮数值，没有用均值、最大值或中位数伪造整体 P99。pipeline=1 时一批只有一个操作；更高 pipeline 下该字段是整批延迟。

## 整轮 CPU 采样

| 顺序 | 样本数 | 用户态均值 % | 内核态均值 % | 空闲均值 % | I/O 等待均值 % | 可运行任务均值 | 空闲低于 10% 的样本 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 异步 | 26 | 45.08 | 49.00 | 4.73 | 1.46 | 3.69 | 26/26 |
| 2 同步 | 25 | 45.72 | 46.68 | 4.88 | 3.08 | 3.08 | 22/25 |
| 3 同步 | 29 | 45.66 | 46.28 | 5.34 | 3.17 | 3.41 | 28/29 |
| 4 异步 | 26 | 44.81 | 48.27 | 5.65 | 1.46 | 4.19 | 26/26 |

四轮均没有观察到换入/换出。可运行任务包含正在运行和等待 CPU 的任务。vmstat 的内核态时间包含中断处理；不要再把 top 中的软件中断比例加上去。top 的 si 是软件中断 CPU 时间，vmstat 的 si 是换入速率，二者不同。

中段线程快照中，Python 客户端占约 55%～78% 的一个逻辑 CPU（按当时 top 的显示口径）；这些是单个采样间隔，不能当作全程平均。全程采样支持 CPU 余量持续很小，但还不能确定客户端、事件循环、网络内核路径或存储调用各自限制吞吐的程度。低 I/O 等待也不排除短暂的同步写延迟。

## Leader 阶段耗时

以下为预热之后、正式客户端运行前后累计计数器的差分均值，单位为微秒；原始样本数保留在结构化摘要。

| 阶段 | 1 异步 | 2 同步 | 3 同步 | 4 异步 |
| --- | --- | --- | --- | --- |
| leader_log_write | 897.3 | 921.3 | 1117.4 | 1007.4 |
| replication_data_ack | 2371.4 | 3210.4 | 3848.4 | 2716.3 |
| kv_apply | 1048.0 | 820.3 | 1001.3 | 1127.4 |
| apply_dispatch | 781.3 | 0.0 | 0.0 | 925.7 |
| write_completed | 7094.9 | 6925.2 | 8270.7 | 7951.0 |

- 区间均值计算为 Δtotal_us / Δcount；没有直接相减累计均值或累计最大值。已回传的阶段均值经过小数位舍入，无法还原精确微秒总计。
- INFO 窗口包含正式客户端准备及每连接一次预填充，因此四轮 write_completed 样本均为 50,032，而正式测量各有 50,000 次写请求。
- 日志写入、KV 应用以批为样本；复制确认以成功数据 RPC 为样本，包含缓冲和重试，不能视为纯 RTT；write_completed 是服务端成功写完成耗时。
- apply_dispatch 是 worker 完成到 owner 收到通知的等待。同步模式仍计样本，每个样本耗时为 0；本组同步样本数分别为 5761 和 5714。
- 各阶段样本、边界不同，可能交叠，均值不能直接相加，也不能直接从客户端 P99 中扣除。
- 已从原始快照核对每轮三个节点在前后时点都认可同一 Leader，任期均为 2；未归档运行中连续 INFO/心跳时序。

## 原始 INFO 核验新增证据

四轮的全部三个节点在前后快照中均满足 commit_index=last_applied，apply_lag、apply_inflight、pending_proposals、queued_writes 和 overload_rejections 为 0。第一轮结束时三个节点索引均为 55066，其余三轮均为 55065；每轮每节点提交索引增量均为 50032，与正式写入加预填充数量一致。这些是端点状态，不能据此声称运行中从未积压。

四轮每节点的 replication_retry_attempts 前后差值均为 0。第一轮节点 0 的累计值在采样前后都是 6，第四轮节点 1 都是 3；它们在 INFO 测量窗口之前已经发生，不能称为“全程零重试”，也不能算作测量期间新增重试。

节点日志混合 Muduo 和 glog 输出，部分行交织、尾行不完整；保留原始字节，不以日志关键词未命中作为“没有异常”的证明。冒烟报告、事后源码版本及构建缓存见独立构建证据包；两个包均不包含二进制本体。

## 结论与适用范围

1. 四轮请求校验均通过，支持该短时、小工作集负载下的功能表现；不是长时间稳定性或故障安全证明。
2. 同步的两轮吞吐相差约 13%，超过第一对模式比较约 3% 的差距；异步两轮吞吐接近，但 P99 从 18.39 ms 变到 26.89 ms。不能根据单轮或两轮数据宣布稳定收益。
3. 两种模式都受到同机 CPU 资源竞争影响。更换 Leader、宿主机调度、缓存和后台负载均未完全控制；一组 ABBA 顺序不能消除这些影响，也不构成统计显著性检验。
4. 早先“先三轮同步、再三轮异步”的结果属于另一批实验，保留为探索性记录，不混入本组；早先监控约 305 秒但压测约 40 秒的尾部数据也不混入本组。
5. 本组到此结束。构建兼容修补和新测试身份记录已纳入自动流程，下一步在 Linux 验证该流程；后续性能实验优先隔离客户端与服务端 CPU 争用，保持负载和持久化语义一致，再判断是否需要优化线程派发、网络路径或存储批次。

## 三节点冒烟测试的独立证据

用户此前回传真实 Linux 测试 PASS，原始报告位于 /home/a/projects/raft-kv/build-ubuntu-server/cluster-reports/run-16oxukmh/report.json，现已随[构建与冒烟证据包](evidence/build-evidence-jQOIcr.tar.gz)归档并核验。重复粘贴相同路径的 PASS 只算一次运行。

原始报告确认 10 项步骤通过：Leader 一致、分片 PING 与二进制 SET/GET、跨调度轮流水线、同连接命名空间切换、DEL、Follower 拒绝写入、32 并发连接、强制终止 Leader、幸存节点选主后写入，以及旧节点使用原配套目录重启和收敛。最终三节点任期为 3，提交/应用索引均为 42；整个测试耗时 1.589 秒，不能当作故障恢复时延。详见[冒烟核验](ubuntu-build-and-smoke.md)。此项是三进程同机测试，不包含整机掉电、磁盘故障、网络分区或线性一致性证明。

## 复现流程与原始材料归档

负载入口为 [tests/load_benchmark.py](../../tests/load_benchmark.py)，服务启动和 Leader 发现可参考 [tests/cluster_smoke.py](../../tests/cluster_smoke.py)。本组通过会话中的临时 Python 编排脚本执行；本文记录方法，不宣称仓库已有一键重现全部采样的命令。

1. 记录源码版本、工作区修改、服务二进制哈希、实际编译配置、依赖版本及机器信息。
2. 每轮新建目录，启动三个节点，全部使用同一 async_apply 值；等待三个节点认可同一 Leader，并检查 INFO 模式字段。
3. 对实际 Leader 执行下方同一负载两次：先 requests=10000 预热，再 requests=100000 正式测量。
4. 预热后保存三个节点的 INFO；启动 vmstat -w -n -y -t 1、正式客户端和 top -b -H -p <三个节点及客户端PID> -d 1 -w 160。客户端退出后立即停止采样，保存第二次 INFO；异常时也清理本次子进程。服务在采样和 INFO 结束后停止。
5. 按异步→同步→同步→异步执行，保留所有轮次。CPU 监控窗口略宽于客户端内部计时窗口，不能包含数分钟的手工等待。

单轮正式负载命令（leader_port 和 report_dir 应由当轮编排设置）：

```bash
python3 tests/load_benchmark.py \
  --host 127.0.0.1 --port "$leader_port" \
  --connections 32 --requests 100000 --pipeline 1 \
  --value-size 128 --write-ratio 0.5 --timeout 5 \
  --namespace bench --output "$report_dir/result.json"
```

四个原始目录均位于 /home/a/projects/raft-kv/build-ubuntu-server/：

| 顺序 | 目录 |
| --- | --- |
| 1 | bench-aligned-u8ubq5kq |
| 2 | bench-sync-aligned-799e4974 |
| 3 | bench-sync-aligned-297fwzvz |
| 4 | bench-async-aligned-ew_qbx_z |

本次已归档每轮的 result.json、warmup.json、info-before.json、info-after.json、vmstat.log、threads.log、benchmark.log、warmup.log、node-*.log，共 44 个文件。四轮 benchmark.log 与对应 result.json 字节一致，warmup.log 与 warmup.json 字节一致。数据目录没有归档。

在仓库根目录重新核验这批文件（Python 标准库即可，不需要 Linux 服务端）：

```sh
python docs/benchmarks/verify_ubuntu_abba.py
```

预期退出码为 0，输出 JSON 的 status 为 PASS，file_count 为 44。脚本核对原始压缩包固定哈希和摘要；它不自动修正不一致数据，发现差异会报错退出。所有 40 万次正式请求和 4 万次预热请求的原始计数都通过核验，性能表仅统计正式请求。

以下为本次已使用的 Ubuntu 打包方法，保留供后续参考，无需重新打包或重跑测试：

```bash
(
set -e
cd /home/a/projects/raft-kv
base="$PWD/build-ubuntu-server"
runs=(
  bench-aligned-u8ubq5kq
  bench-sync-aligned-799e4974
  bench-sync-aligned-297fwzvz
  bench-async-aligned-ew_qbx_z
)
for run in "${runs[@]}"; do
  for file in result.json warmup.json info-before.json info-after.json \
              vmstat.log threads.log benchmark.log warmup.log \
              node-0.log node-1.log node-2.log; do
    test -f "$base/$run/$file" || {
      echo "缺少：$base/$run/$file"
      exit 1
    }
  done
done
archive=$(mktemp "$base/abba-evidence-XXXXXX.tar.gz")
tar --exclude='*/data' -czf "$archive" -C "$base" "${runs[@]}"
sha256sum "$archive"
printf '归档文件：%s\n' "$archive"
)
```

四轮性能材料、冒烟原始报告与事后构建快照均已归档核验，本轮收集结束。事后采集信息不追溯冒充测试时记录；剩余身份限制应在下一次独立构建和测试时通过即时记录解决。
