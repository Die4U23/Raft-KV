# Ubuntu 构建与验证流程

此流程固化了用户 Ubuntu 26.04 VM 上已验证的 Boost/Muduo 兼容修改，并在构建和测试时记录源码、依赖及服务二进制指纹。Windows 上的准备逻辑测试和可移植 C++ 回归已完成；2026-09-08 回传的 [Linux 自动流程原始证据](benchmarks/linux-workflow-validation.md)也已核验，已有构建目录上的增量流程、CTest 5/5 和真实冒烟 10 项通过。后续[空目录全量编译和测试](benchmarks/linux-fresh-validation.md)也已核验通过；干净系统复现及 Docker 镜像构建仍待验证。此前的[性能测试证据](benchmarks/ubuntu-2cpu-abba.md)属于独立的手工流程。

## 改动与依赖范围

- 项目要求 Boost >= 1.69，仅查找 thread 组件。Boost.System 自 1.69 起无需链接独立库，见 [Boost 1.69 官方发布说明](https://www.boost.org/doc/libs/1_69_0/libs/system/doc/html/system.html#changes_in_boost_1_69)。此举避免新 Boost 缺少 system 库导致配置失败。
- `scripts/prepare_muduo.py` 校验仓库 `third_party/muduo.zip` 的 SHA256，再在构建目录准备源码。HttpResponse.cc 的缓冲区从 32 改为 64 字节，Content-Length 格式从 `%zd` 改为 `%zu`；原始 zip 不变，保留上游版权与许可文件。
- 固定压缩包 SHA256：`910d21d1343164e9517f32a5b77025cfeed0f20aae2ae86dd1ed399a7b4d2192`。预处理不是下载器；换用别的归档会失败，需要单独审查升级。
- Muduo 使用 Release、关闭示例和可选 Protobuf 扩展。该开关只作用于 Muduo，Raft-KV 自身仍查找并使用 Protobuf。
- 向旧 Muduo 的配置传入 `CMAKE_POLICY_VERSION_MINIMUM=3.5`，用于 CMake 4 的兼容检查。该变量的作用见 [CMake 官方说明](https://cmake.org/cmake/help/latest/variable/CMAKE_POLICY_VERSION_MINIMUM.html)；旧 CMake 可能提示该变量未使用，不能因此忽略其他真实错误。
- 保留 Muduo 的 `-Werror`，修正具体问题而不整体关闭警告。归档仍含上游 `-march=native`，生成的库应在当前目标环境使用，不能承诺跨 CPU 型号分发。
- Muduo 安装到项目构建目录中的私有前缀；项目显式引用该前缀的静态库和头文件，避免误用此前 `/usr/local` 安装的 Muduo。

## 1. 安装系统依赖

以下用于 Ubuntu 26.04，需 C++17 编译器、CMake >= 3.16、Python >= 3.8。先确认 apt 软件源属于当前发行版；此前混用 focal/resolute 的问题不能通过强制覆盖头文件解决。本流程不会修改软件源，也不会自动安装软件包。

```bash
sudo apt-get update
sudo apt-get install -y \
  build-essential cmake python3 git \
  libboost-dev libboost-thread-dev \
  libprotobuf-dev protobuf-compiler \
  librocksdb-dev libgflags-dev libgoogle-glog-dev libssl-dev
```

系统依赖采用当前发行版软件包版本，尚未锁定为跨时间完全相同的依赖集合。构建报告记录实际查询结果；若需要可复现镜像，应进一步固定仓库快照及传递依赖。

## 2. 检查源码准备

在包含新脚本的仓库根目录执行：

```bash
python3 scripts/build_linux.py --prepare-only
```

预期状态为 `PREPARED`。脚本验证固定归档，准备补丁后的源码，并输出报告位置；此时 Linux 服务、CTest、冒烟状态均为 `UNRUN`。该步骤也可在 Windows 用 `python` 执行。

默认目录为 `build-linux-repro/`，与此前 `build-ubuntu-server/` 的服务、数据库和报告分开。已有准备目录中若出现修改过的或额外文件，脚本会停止，不覆盖用户修改。需要新环境时换目录，例如 `--build-dir build-linux-repro-2`，无需删除旧目录。

## 3. 构建并运行验证

```bash
python3 scripts/build_linux.py --jobs 2 --smoke
```

脚本依次执行：Muduo 配置、构建、私有安装，Raft-KV 的 RelWithDebInfo 构建，CTest，以及真实三节点冒烟。默认并行度为 2，可通过 `--jobs` 调整。省略 `--smoke` 时仍运行 CTest，但真实冒烟明确记录为 `UNRUN`。

脚本不调用 sudo，不启动压测客户端，不改变 `async_apply` 默认值或存储同步持久化语义。真实冒烟沿用 `tests/cluster_smoke.py` 的隔离目录和端口分配；只处理本次测试节点。

成功时退出码为 0，并输出 `Status: PASS` 和报告路径。失败时退出码非 0，终端给出失败步骤日志位置；此时先查看对应日志，不把未执行的后续测试当作通过。

## 4. 保存新构建证据

```text
build-linux-repro/
  deps/muduo-source/       # 固定归档与补丁生成的源文件
  deps/muduo-build/        # Muduo 构建目录
  deps/install/            # 私有头文件与静态库
  server/raft_kv_server    # 本次构建的服务
  reports/run-*/
    build-report.json
    server-CMakeCache.txt
    muduo-CMakeCache.txt
    flags.make / link.txt  # 生成器提供时保留
    *.log                 # 命令、工具版本、Git 状态、依赖查询、测试日志
    cluster/run-*/        # 使用 --smoke 时的原始报告与节点日志
```

`build-report.json` 包含：

- 源码输入逐文件 SHA256，以及准备后的 Muduo 源码清单与归档哈希；构建结束再次检查源码输入未变。
- 每条命令的参数数组、工作目录、开始/结束时间、退出码和日志位置。
- 编译完成后记录的服务二进制 SHA256，以及实际链接的两个 Muduo 静态库哈希；冒烟前后检查服务二进制未变。
- CTest 和真实冒烟的独立状态，避免把准备源码或可移植测试当成真实服务验证。

Git 可用时，报告目录还保存 HEAD、工作区状态和相关已跟踪代码修改；源码指纹涵盖 CMakeLists.txt、src、proto、tests、scripts 与固定 Muduo 归档，不只依赖 Git SHA。工具缺失、配置失败、测试失败会留下失败报告。该记录提高新测试的可追溯性，不声称构建是可重现二进制的 hermetic 构建；完整系统动态库和环境仍未固定。

## Docker 路径

`docker/dev.Dockerfile` 的 Muduo 步骤也复用同一个准备脚本、兼容参数和扩展开关。该镜像仍是原有 Ubuntu 20.04 多依赖开发环境，含本项目主服务不直接使用的其他库；本次未调整这些依赖，也未验证整个镜像构建成功。当前 Ubuntu 26.04 验证优先使用上面的原生流程。

## 本地检查记录

```sh
python tests/build_workflow_tests.py
cmake -S . -B build-portable -G Ninja -DRAFTKV_BUILD_SERVER=OFF -DCMAKE_BUILD_TYPE=Debug
cmake --build build-portable --parallel 2
ctest --test-dir build-portable --output-on-failure
```

准备/编排测试使用真实归档，检查补丁结果与已归档 VM 文件一致、重复运行不改写、保留本地编辑、拒绝错误归档，以及工具失败时记录 FAILED 而不误标测试成功。故障编排测试中的工具失败为主动模拟，不是实际 Linux 构建失败。

这些本地检查不能替代 Linux 链接、Muduo 调度或真实 RocksDB 的集成验证。第 3 步已有增量及空目录全量构建的用户回传报告完成核验，两轮均通过 CTest 和真实冒烟，详见[全量构建证据与边界](benchmarks/linux-fresh-validation.md)。系统依赖仍来自现有 VM，干净系统复现尚待完成。
