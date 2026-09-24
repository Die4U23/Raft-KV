# 文档

当前行为以仓库里的代码、[README](../README.md) 和 [更新日志](../CHANGELOG.md) 为准。下面只保留还在用的说明；过程笔记和过时的完成度报告在 [archive](archive/README.md)。

| 想看什么 | 文件 |
| --- | --- |
| 现在做到哪、还有哪些限制 | [review-status.md](review-status.md) |
| 线程、批量、回调在哪条线程上 | [concurrency.md](concurrency.md) |
| 本地读、ReadIndex、租约读 | [read-consistency.md](read-consistency.md) |
| Ubuntu 上怎么构建 | [linux-build.md](linux-build.md) |
| 压测客户端怎么读数 | [benchmark.md](benchmark.md) |
| 测试能证明什么、不能证明什么 | [tests/README.md](../tests/README.md) |
| 分区、崩溃重启、过载脚本怎么跑 | [partition-test.md](partition-test.md)、[write-restart-test.md](write-restart-test.md)、[overload-soak-test.md](overload-soak-test.md) |
| 三节点演示：写入、发布配置、杀掉 Leader 再读 | [scripts/demo_three_nodes.py](../scripts/demo_three_nodes.py) |
| 已经核验过的原始证据 | [benchmarks/](benchmarks/) |
| 重连、客户端读取、性能诊断的实现说明 | [optimizations/](optimizations/) |
| 实习长文。第六节以后的日期记录是后来补的；更早的叙述按当时归档保留 | [raft-kv-project-practice.md](raft-kv-project-practice.md) |
