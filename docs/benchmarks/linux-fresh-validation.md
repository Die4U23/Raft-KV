# Linux 空目录全量构建验证

核验日期：2026-09-08。用户在已有依赖的 Ubuntu 26.04 VM 中创建新的空目录，执行提交 `35a348d` 的自动构建与测试脚本。原始记录确认 Muduo、服务和测试程序重新编译、链接，CTest 5/5、真实三节点冒烟 10 项通过。

本次补齐[前一轮增量验收](linux-workflow-validation.md)缺少的全量编译证据。系统依赖来自现有 VM，因此仍不等同于干净操作系统复现、Docker 构建或逐字节可重现构建。

## 原始证据

- [原始压缩包](evidence/linux-fresh-validation-wjgjNs.tar.gz)：39,873 字节，25 个普通文件、3 个目录，原始字节保留。
- SHA256：`60f0772a8a149e71ffb99d18e63046b3141bea80678c95580e9e33faaf6951ff`。
- [逐文件指纹与核验结果](evidence/linux-fresh-validation-verification.json)，由[只读核验脚本](verify_linux_fresh_validation.py)生成。
- VM 报告目录：`/home/a/projects/raft-kv/build-linux-fresh-1vukPy/reports/run-a3q01rgw`。

## 全量编译和测试

| 项目 | 本轮记录 |
| --- | --- |
| 执行时间 | 2026-09-08 15:26:40～15:27:39 UTC（北京时间 23:26～23:27），约 59.01 秒 |
| 源码提交 | `35a348d951b43470d1dd6491a7d6522a5a83af96` |
| 输入身份 | 48 个已跟踪文件匹配该提交；418 个准备后的 Muduo 文件匹配固定 zip 加补丁 |
| Muduo 构建 | 41 条 C++ 编译记录、4 个静态库链接，构建步骤约 19.81 秒 |
| 服务与测试构建 | Protobuf 代码生成、23 条 C++ 编译记录、6 个可执行文件链接，构建步骤约 33.74 秒 |
| 链接来源 | 本次新目录 `deps/install` 内的 Muduo 静态库 |
| CTest | 5/5，通过日志总耗时 0.22 秒 |
| 三节点冒烟 | 10 项通过，整个冒烟 1.708 秒；不是故障恢复时延 |
| 流程 | 15 个命令步骤依次退出 0，最终状态 PASS |

空目录创建来自用户执行的 `mktemp -d` 命令及回传路径，归档日志另提供实际重新编译和链接记录。此证据范围是该 VM 的本轮运行，未在 Windows 重新执行 Linux 服务。

构建后、冒烟前记录的服务二进制 SHA256 与冒烟所用记录一致：

```text
bf3c71a718ca7d19c700bb4063bd19e38e895aafa182e6210355ef05c5a51be6
```

脚本还在测试后检查二进制与源码清单未变。原始包没有二进制和静态库本体，本次核验的是运行记录和源码指纹，不是重新计算缺失二进制的哈希。源码清单额外含 `src/server/main.cpp.bak` 的哈希；该备份不参与编译，内容未提供，单独标记为未核验，不计入 48 个匹配的 Git 输入。

## 三节点结果

初始 Leader 为节点 2；测试强制退出后，节点 0 成为 Leader，剩余节点确认新写入。节点 2 用原参数和配套数据目录重启，最终三个节点任期均为 3，commit_index=last_applied=42，apply_lag=apply_inflight=0。

节点 2 的退出记录为 -9、-15，其他节点为 -15，对应测试中的强制退出和结束清理。JSON 与 smoke.log 的 10 项检查一致，覆盖协议、流水线、命名空间、并发请求、Follower 写拒绝及故障恢复。最终节点 0 的 replication_retry_attempts 为 6；通过不代表全过程无复制重试。

## 范围与后续

- 已完成：现有 Ubuntu VM 的空目录全量构建、CTest、真实三节点冒烟与测试时身份记录。
- 仍待完成：干净系统依赖安装复现、Docker、网络分区与失去多数派专项测试、真实存储故障和长时间负载。
- 配置日志仍有上游 CMake 弃用提示、CMP0167 警告及可选 CURL 未找到；没有阻止本轮配置构建。通过不代表无警告。
- 本轮无性能压测，不修改旧 ABBA 吞吐统计，也不证明线性一致性、掉电安全或生产可用。

在包含提交 `35a348d` 历史对象的仓库中重新核验：

```sh
python docs/benchmarks/verify_linux_fresh_validation.py
```

脚本仅使用 Python 标准库和 Git，复用增量验收的身份、时序、CTest 和冒烟检查，并额外检查全量编译、代码生成和链接记录。输出 status 应为 PASS。
