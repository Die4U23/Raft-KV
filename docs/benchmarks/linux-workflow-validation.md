# 自动 Linux 流程：原始证据核验

核验日期：2026-09-08。用户在 Ubuntu VM 执行提交 `35a348d` 的 `python3 scripts/build_linux.py --jobs 2 --smoke`，自动流程、CTest 5/5 和真实三节点冒烟 10 项检查均通过。原始日志与运行时指纹已归档核对。

后续更新：独立的[空目录全量构建报告](linux-fresh-validation.md)已核验通过。以下保留本轮增量运行的证据范围，不追溯扩大它的结论。

本次属于已有构建目录上的增量验证：Muduo 与服务的构建日志均只有 Built target，没有重新编译或链接的记录。它证明该目录上的自动配置、构建检查、私有安装和测试流程成功，不是空目录全量编译或干净操作系统复现的证据。

## 证据索引

- [原始压缩包](evidence/linux-validation-1WQbP1.tar.gz)：38,922 字节，25 个普通文件、3 个目录；保持原始字节。
- 压缩包 SHA256：`20841bcfbf9bb69d471728c7e3964bb9122cd84f6d2a365e13388770bc00627a`，用于标识收到的文件，不是独立的来源认证。
- [核验结果与逐文件哈希](evidence/linux-validation-verification.json)，由[只读核验脚本](verify_linux_validation.py)生成。
- 虚拟机原始报告目录：`/home/a/projects/raft-kv/build-linux-repro/reports/run-gnt4rq_1`。
- 本次没有在 Windows 重新运行 Linux 服务，也没有执行压缩包中的命令或补丁。

## 结果与构建身份

| 核验项 | 原始证据 |
| --- | --- |
| 执行区间 | 2026-09-08 06:38:51～06:38:55 UTC，约北京时间 14:38 |
| 源码提交 | `35a348d951b43470d1dd6491a7d6522a5a83af96` |
| 工作区 | 相关已跟踪代码 diff 为空；Git 状态只有根目录的 CMakeLists.txt 未跟踪备份 |
| 源码指纹 | 48 个已跟踪输入文件逐项匹配该提交的 Git blob，不依赖 Windows 换行符形式 |
| Muduo 指纹 | 418 个准备后的文件匹配固定 zip 与两处 HttpResponse 修补 |
| 配置 | RelWithDebInfo，`-O2 -g -DNDEBUG -std=gnu++17` |
| Muduo 来源 | 生成的头文件搜索与链接参数指向 `build-linux-repro/deps/install`，不是 `/usr/local` 的旧库 |
| 系统 | 本轮记录 Linux 7.0.0-31、glibc 2.43、Python 3.14.4；不同于旧 ABBA 记录的内核版本 |
| 命令 | 15 个记录步骤按顺序完成，退出码均为 0 |
| CTest | 5/5 通过，原始日志总耗时 0.11 秒 |
| 冒烟 | 10 项检查通过，整个冒烟耗时 1.943 秒；不是故障恢复时延 |

服务二进制的运行时记录：

```text
c51f226ba0f04ef690c55e9071d2317db5db91f556ba31ee9bcf3402d33ccd09
```

指纹记录时间位于服务构建步骤完成后、冒烟启动前，build-report.json 的 binary.sha256 与 smoke_binary_sha256 相同。所匹配源码中的自动脚本还会在冒烟后复核二进制、在流程末尾复核源码清单，变化会阻止最终 PASS。

两个 Muduo 静态库也有构建时哈希记录。包中没有服务二进制、静态库或全部系统动态库本体；本次核对的是原始运行记录、路径、源码指纹及内部一致性，没有对缺失的二进制本体重新计算哈希，也不宣称二进制可逐字节重建。这次记录不追溯证明旧冒烟或旧 ABBA 四轮的测试时二进制身份。

源码清单共 49 项，其中额外一项是 `.gitignore` 忽略的 `src/server/main.cpp.bak`。它不匹配项目的 `*.cpp / *.h / *.cc` 源文件收集规则，不参与服务编译。包中只有该备份文件的哈希，没有内容；单独保留为未从内容核验的额外项，不算作 48 个已核验 Git 输入之一。

## 冒烟原始报告

报告位于包内 `run-gnt4rq_1/cluster/run-yg5iv6kc/report.json`。初始 Leader 为节点 1，强制退出后节点 2 成为 Leader；节点 1 使用完全相同的参数与配套数据目录重启。

最终三个节点均认可 Leader 2，任期均为 4，commit_index=last_applied=42，apply_lag=apply_inflight=0。节点 1 的退出码为 -9、-15，另两节点为 -15，分别对应测试主动终止与结束清理。

覆盖选主、分片和二进制请求、跨调度轮流水线、命名空间、DEL、Follower 写拒绝、32 并发连接、Leader 退出、剩余节点继续写入及旧节点恢复。原始 JSON 和 smoke.log 的 10 项 PASS 对应一致；本次没有压测吞吐数据，不加入旧 ABBA 统计。

## 保留的限制

- 新自动流程的增量 Linux 验收已完成；空构建目录的全量编译和干净操作系统复现尚未由此包证明。
- Docker 镜像构建仍未执行，网络分区、磁盘故障、整机掉电、长时间负载和线性一致性不在本次覆盖范围内。
- Muduo 仍有旧 CMake 最低策略版本的弃用提示，Muduo 与项目配置均有 CMP0167 开发者警告。它们没有使本轮配置失败；保留在日志中，不把“通过”写成“无警告”。可选 CURL 未找到，也未阻止本轮所用目标通过。
- 运行日志和指纹提升可追溯性，不是第三方独立运行或来源认证。

## 重新核验

在包含提交 `35a348d` 历史对象的 Git 仓库根目录执行：

```sh
python docs/benchmarks/verify_linux_validation.py
```

需要 Python 标准库和 Git。脚本验证压缩包哈希、25 个文件、命令时序、构建/冒烟指纹、CTest 与冒烟结果，并从固定 Git 提交读取 48 个源码 blob、在内存中重算 Muduo 修补清单。输出 JSON 的 status 应为 PASS，缺少该提交历史或出现数据差异时会失败退出。源码备份的原始内容和二进制本体不在核验范围内。

本批材料已完整归档，后续全量编译已由独立报告验证，无需重复这次增量测试。
