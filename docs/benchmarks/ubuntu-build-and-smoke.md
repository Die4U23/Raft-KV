# Ubuntu 冒烟测试与事后构建信息核验

核验日期：2026-09-07。用户提供的三节点冒烟原始报告确认 10 项检查全部通过；当前构建配置和兼容修改也已归档。构建信息采集于测试之后，不能补造测试当时没有记录的二进制指纹。

## 原始材料

- [用户提供的原始压缩包](evidence/build-evidence-jQOIcr.tar.gz)：13,465 字节，25 个普通文件（21 个构建记录、1 个冒烟报告、3 个节点日志），保持原始字节。
- 压缩包 SHA256：`c0e5217b5e0e2bc6aede875ff2c4759b1b023b65a6f1a7a11b7c361570601f3b`。哈希标识收到的文件，不构成独立的来源认证。
- [核验结果与逐文件哈希](evidence/build-verification.json)，由[只读核验脚本](verify_ubuntu_build.py)生成。脚本不解压或执行包中的命令、补丁和程序。
- 独立的四轮性能测试及其原始材料见[双核 VM 性能基线](ubuntu-2cpu-abba.md)，不与冒烟测试的计时混用。

## 冒烟测试结果

原始报告对应 `/home/a/projects/raft-kv/build-ubuntu-server/cluster-reports/run-16oxukmh/report.json`。节点日志时间为 2026-09-05 22:01 左右（虚拟机时钟）；此前重复粘贴相同 PASS 路径只计一次运行。

| 核验项 | 原始报告结果 |
| --- | --- |
| 总状态 | PASS，10 个步骤 |
| 初始 Leader | 节点 0 |
| 故障后及最终 Leader | 节点 2 |
| 最终任期 | 三节点均为 3 |
| 提交 / 已应用索引 | 三节点均为 42 / 42 |
| 最终 apply_lag / apply_inflight | 三节点均为 0 / 0 |
| 模式 | 三节点 async_apply=1 |
| 整个测试耗时 | 1.589 秒；不是 Leader 故障恢复时间 |

10 项检查包括选主一致、分片 PING 与二进制 SET/GET、跨 128 命令调度轮的流水线、同连接命名空间切换、DEL 语义、Follower 拒绝写入、32 并发连接、强制终止 Leader、剩余节点选主后成功写入，以及旧节点使用原目录重启并收敛。

节点 0 的两次启动参数完全相同，配套 KV 与 Raft 日志路径保持一致；退出码依次为 -9、-15，另外两个节点为 -15。-9 对应测试主动发送 SIGKILL，-15 对应结束清理，不据此认定非预期崩溃。报告中的步骤是测试脚本记录的断言结果，核验并未重新执行这些网络和存储操作。

这是三进程同机的有限场景验收，不是整机掉电、磁盘故障、网络分区、三机容灾或线性一致性的证明。最终应用追平也不表示全过程没有短暂积压。

## 当前构建快照

采集时间：`2026-09-07T16:50:52+08:00`。以下内容属于事后采集的虚拟机工作区和现有构建记录。

| 项目 | 已核对记录 |
| --- | --- |
| 当前 Git HEAD | `64f60a783bb9de0bfd3bf5b5068bbb7d79fe3ee0` |
| 当前工作区 | 已跟踪修改只有 CMakeLists.txt；另有其未跟踪备份文件 |
| 编译配置 | CMakeCache 为 RelWithDebInfo；flags.make 为 `-O2 -g -DNDEBUG -std=gnu++17` |
| 编译器 | `/usr/bin/c++`；采集时版本 GCC 15.2.0（Ubuntu 15.2.0-16ubuntu1） |
| CMake / protoc / Python | 4.2.3 / 3.21.12 / 3.14.4 |
| 已安装 Boost | 1.90.0-6ubuntu1；缓存与链接参数指向 Boost 1.90 |
| 已安装 RocksDB / Protobuf | 9.11.2-1 / 3.21.12-15ubuntu1 |
| 已安装 gflags / glog | 2.2.2-3 / 0.6.0-3 |
| Muduo | 链接 `/usr/local/lib/libmuduo_net.a`、`libmuduo_base.a`；静态库本体与哈希未提供 |
| 虚拟 CPU | lscpu 报告 2 个逻辑 CPU，VMware，呈现 Intel Core i5-12450H 型号；不是独立宿主机硬件核验 |

包清单只把状态为 `ii` 的条目计为已安装；`un` 或版本为空的历史条目不算已安装依赖。这是一份相关依赖清单，不覆盖所有传递依赖和实际运行时加载版本。

记录中的当前服务二进制 SHA256：

```text
46fe526f13ba6c931e16338bd853c6fc4af4237fa5c28e3325360d0f7e3aa607
```

记录大小为 10,902,096 字节，修改时间为 2026-09-05 22:00:58 +0800，早于冒烟及四轮性能测试。这与“沿用同一构建”的操作过程相符，但文件时间不是不可篡改的历史证明。包中只有哈希文本、stat、构建缓存与参数，没有二进制本体，也没有测试启动时采集的哈希；因此该 SHA256 是用户事后采集记录，不能声称本次已对二进制本体重新计算或证明其就是每轮测试使用的版本。

## 两处兼容调整

本次在 Windows 工作区做了额外的只读源码对照：

1. 归档的项目 CMakeLists.txt 与 Git 提交 `64f60a7` 中该文件相比，仅将 `find_package(Boost COMPONENTS system thread REQUIRED)` 改为 `find_package(Boost COMPONENTS thread REQUIRED)`，与归档补丁一致。源码目录未出现在采集时的 Git 修改清单中。
2. 归档的 Muduo HttpResponse.cc 与仓库 `third_party/muduo.zip` 中对应文件相比，仅将 `char buf[32]` 改为 `char buf[64]`，将 Content-Length 的 `%zd` 改为 `%zu`。

以上描述的是归档快照中的修补。后续工程改动已将其纳入 [Ubuntu 自动构建流程](../linux-build.md)：项目 CMake 要求 Boost >= 1.69 并仅查找 thread，准备脚本对固定的原始 Muduo zip 自动应用两处修补，原始 zip 不变；新构建/测试即时记录身份。准备逻辑与可移植测试已在本地检查，自动流程的完整 Linux 验收仍待执行，不追溯改变本报告中的旧测试身份限制。

## 重新核验归档数据

在仓库根目录执行（Python 标准库即可）：

```sh
python docs/benchmarks/verify_ubuntu_build.py
python docs/benchmarks/verify_ubuntu_abba.py
```

预期两条命令退出码均为 0，输出 JSON 的 status 均为 PASS；文件数分别为 25 和 44。脚本读取固定哈希的证据包并核对内容，不重新编译或压测。源码与历史 Git/第三方归档的精确对照是本次另行完成的检查，构建核验脚本只检查归档配置及修补特征，不代替该源码对照。

本轮证据收集到此完成，无需为这组旧测试继续补跑。尚不能追溯证明的测试时二进制身份保留为限制；干净环境构建复现、运行中持续指标和更广故障覆盖属于后续独立验收。
