# 优化记录 001：已建立的 TCP 连接反复断开

日期：2026-09-09。实现完成、本地检查通过，**新版 Linux 验收 UNRUN**。此前三组基线测试已经完成，本次针对已测出的重连问题优化；历史证据保留，不将旧结果记为新版本通过。

## 基线和原因

| 场景 | 基线提交 | 转发器拒绝重连 | 完整节点日志 |
| --- | --- | ---: | ---: |
| 两次分区及恢复 | `9cfcfd6` | 24,731 | 5,498,325 字节 |
| 写入积压过载中的分区 | `b00e221` | 7,897 | 1,740,981 字节 |

来源：[分区报告](../benchmarks/partition-validation.md)、[过载报告](../benchmarks/overload-validation.md)。两个场景时长和负载不同，不能相互计算改善比例。

项目自带 `third_party/muduo.zip` 中的 `muduo/net/TcpClient.cc` 与 `Connector.cc` 显示：建连失败由 `Connector::retry` 以 500 毫秒为起点退避，最长 30 秒；但启用 `TcpClient::enableRetry()` 后，已建立连接的 `removeConnection` 调用 `Connector::restart`，后者重置间隔并直接 `startInLoop()`。转发器接受 TCP 后立即关闭，就会反复经过这条立即重连路径。

因此旧代码“enableRetry 已处理退避”的说明不适用于成功建连后立即断线。本次只改项目的 PeerManager，不改依赖压缩包或 Connector 内部状态。

## 实现及取舍

1. 每个主动连接的 peer 维护独立策略；首次连接立即启动，建连失败由 Connector 自己重试。
2. 已建立连接断开后，等待 500、1000、2000 毫秒，上限保持 2000 毫秒。只有连接连续存在满 10 秒才重置，短暂成功不会重置。
3. 延迟到旧客户端断线处理返回后创建新 TcpClient。不能对旧的 kConnected Connector 直接重复 connect()。客户端名称带代际，避免连接标识复用。
4. 策略 token 拦截重复与旧定时任务；弱生命周期标记使 PeerManager 销毁后的回调不再访问对象。所有状态都在 EventLoop 线程中操作。

“稳定”仅指 TCP 存续时长，不是 Raft 多数派确认或应用健康检查。两秒是已建连后断线的调度上限，后续建连失败仍有 Connector 最长 30 秒退避及网络、调度延迟。不增加业务写入重试，不修改持久化和读取语义。

本实现未增加随机抖动，适用当前固定小集群，大规模同步重连不在本次验收范围。保留连接事件日志，先减少事件数量，再测日志收益。

## 验证状态

| 检查 | 当前状态 |
| --- | --- |
| 可移植 CTest | 6/6 PASS；新策略检查 100 次反复断线、上限、稳定边界、旧 token、重复消费和 peer 隔离 |
| Python 分区辅助检查 | 8/8 PASS；拒绝数等于阈值可通过，超过阈值或负计数失败 |
| 新版 Linux 构建 | UNRUN |
| Linux `peer_manager_transport_tests` | UNRUN；初始拒绝、反复短连接、恢复后收到 Raft 帧、待重连时销毁对象 |
| 新版三节点冒烟和分区 | UNRUN；分区保留原来 5 个安全性/恢复阶段，并要求累计拒绝数不超过 100 |

Linux 构建包含 6 个可移植目标及 1 个真实 Muduo 目标，共 7 个 CTest。真实传输测试使用本机 TCP 和 EventLoop，约 9 秒、超时 20 秒；不包含 RocksDB 或完整 Raft 共识，三节点测试另外覆盖这些部分。

## Ubuntu 分步验收

第一步生成新的二进制及构建身份，保留旧目录作基线。在项目根目录执行：

```bash
git pull --ff-only origin main &&
python3 scripts/build_linux.py --build-dir build-linux-reconnect --jobs 2 --smoke
```

预期 `Status: PASS`，同时输出新的 `build-report.json` 路径。失败时先回传失败步骤，不复用旧二进制判断优化效果。第一步结果确认后，再做第二步。

第二步使用第一步实际输出的报告路径，以下占位符必须替换：

```bash
python3 tests/cluster_partition.py \
  --build-report build-linux-reconnect/reports/run-实际目录/build-report.json \
  --artifacts build-linux-reconnect/reconnect-reports \
  --max-reconnect-refusals 100
```

保留默认的 4 秒分区观察窗口。100 是本场景的验收保护线，不是精确理论次数或容量目标；计数下降也必须同时通过原有的安全性、恢复和收敛检查。报告的 `reconnect_check` 保存阈值、实际次数及结束前节点日志字节，完整日志继续保留供归档。

## 待回填实测

新版提交、二进制指纹、故障观察时长、重连次数、完整日志大小和恢复结果均待报告。仅在可比场景中计算下降比例；日志统计注明是否包含最后清理阶段。本轮不据此宣称正常业务 QPS 提升。
