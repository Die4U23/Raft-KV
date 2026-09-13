# 优化实验 003：降低压测客户端回复读取开销

状态：**Linux 四轮对照 PASS，完整证据已核验归档**。本轮合并读取吞吐变化 −1.53%，客户端 CPU 成本变化 +7.12%，未观察到收益。继续保留 `classic` 默认，合并读取仅作为实验选项；[各轮数据与判断边界](../benchmarks/client-header-validation.md)。

## 改动与目的

[已归档诊断](../benchmarks/profile-validation.md)中，客户端约占单核 62.88%，整机平均空闲约 4.97%。在继续优化服务端之前，先验证减少客户端工作是否有帮助。

`load_benchmark.py` 的 `--client-mode` 提供两种回复头部读取方式：

- `classic`：先读取一个类型字节，再读取以 CRLF 结尾的头部行；默认行为。
- `combined-header`：一次读取包含类型字节的头部行，再分离类型与内容，减少一次 StreamReader 方法调用。Bulk 内容仍按声明长度精确读取。

这是 Python 客户端的改动；StreamReader 调用次数不等于系统调用次数，也不代表每次调用都会切换任务。增加切换参数并不保证合并模式更快。两种模式共用长度、嵌套限制和回复内容校验，超时仍保留已收到的回复，并关闭存在未匹配回复的连接。连接建立、SELECT 和预填充沿用原路径，正式负载与预热按所选模式解析回复。

服务端二进制、Raft、多数派确认、同步持久化和 GET 语义不变。没有关闭错误校验、增加 pipeline 或删减延迟样本来提高数字。

## 四轮对照

`tests/compare_clients.py` 依次执行 **classic → combined-header → combined-header → classic**，每轮启动全新三节点集群及数据目录，预热 10,000 次、正式执行 100,000 次。每轮复用完整诊断程序，保留 CPU、阶段差分、三副本键校验、日志及构建指纹。

固定负载为 32 连接、pipeline=1、128 字节、读写各半，服务为 async_apply=true、group_commit_ms=1。四轮须使用相同二进制、源码指纹、负载参数和记录的系统环境；随机分配的端口、Leader 与选举时间可以不同，报告保留差异。

输出指标：

- 每轮吞吐、P50/P95/P99、客户端 CPU 微秒/成功操作、整机 CPU 和 Leader。
- 按模式合并的吞吐 = 总成功数 / 总正式耗时；CPU 成本 = 总客户端 CPU 秒 / 总成功数。
- 两种模式的合并吞吐与 CPU 成本变化；保留每轮分位数，不平均成“合并 P99”。

四轮都通过正确性及身份检查才标记 PASS；PASS 不等于性能提升。单次 ABBA 只能观察本机差异，不能排除调度、初始 Leader、系统缓存等波动。无稳定收益时保留 classic 默认，不筛掉较慢轮次。

## 历史复现方法

本轮已经完成，无需再次执行。以下保留复现入口，无需重新编译服务端：

```bash
cd ~/projects/raft-kv &&
git pull --ff-only origin main &&
python3 tests/compare_clients.py \
  --build-report build-linux-reconnect/reports/run-c4arz1uk/build-report.json
```

通常数分钟，具体由机器决定；每轮原有 240 秒期限和独立清理保持不变。任意一轮失败即停止，查看该轮报告和日志，不继续挑选结果。

复现时预期 `PASS: client header ABBA; report: .../comparison.json`。终端开头 `Comparison artifacts:` 对应的整个目录包含四轮报告及日志；本轮这些材料已归档，不需要补交。

本地验证：`python tests/load_benchmark.py --self-test`、`python tests/client_mode_tests.py`、`python tests/profile_benchmark_tests.py`、`python tests/compare_clients_tests.py`。这些检查覆盖两模式完整请求计数、分片/嵌套/二进制响应、畸形与截断输入、部分回复后超时、外部取消、CPU/阶段统计、测量边界以及对照汇总拒绝条件，不替代 Linux 实测。
