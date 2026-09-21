# 构建目录管理说明

## 推荐的构建目录

项目中应该只保留以下构建目录：

### 1. build-linux-repro/
**用途**: 官方 Linux 构建，用于生成可重现的二进制
**创建方式**:
```bash
python3 scripts/build_linux.py --jobs 2
```

### 2. build-portable/
**用途**: 可移植单元测试（不依赖 Muduo/RocksDB）
**创建方式**:
```bash
cmake -S . -B build-portable -DRAFTKV_BUILD_SERVER=OFF -DCMAKE_BUILD_TYPE=Debug
cmake --build build-portable
```

## 需要清理的目录

以下目录是临时构建，可以安全删除：
- `build/`
- `build2/`
- `build-tests/`
- `build-ubuntu-deps/`
- `build-ubuntu-server/`
- `build-ubuntu-unit/`
- `build-linux-fresh-*/`
- `build-linux-reconnect/`
- `build-test-quick/`

**清理命令**:
```bash
rm -rf build build2 build-tests build-ubuntu-* build-linux-fresh-* \
       build-linux-reconnect build-test-quick
```

**注意**: 如果遇到权限问题，可能需要使用：
```bash
chmod -R u+w build* && rm -rf build build2 ...
```

## .gitignore 配置

项目的 `.gitignore` 已配置为忽略所有 `build-*` 目录，但可以通过 `!` 来保留特定目录：

```gitignore
build*/
!build-linux-repro/
!build-portable/
```

## 构建最佳实践

1. **开发测试**: 使用 `build-portable`
   ```bash
   cmake -S . -B build-portable -DRAFTKV_BUILD_SERVER=OFF
   cmake --build build-portable
   ctest --test-dir build-portable --output-on-failure
   ```

2. **完整构建**: 使用 `build-linux-repro`
   ```bash
   python3 scripts/build_linux.py --jobs 2 --smoke
   ```

3. **临时测试**: 使用一次性目录
   ```bash
   cmake -S . -B build-temp -DRAFTKV_BUILD_SERVER=OFF
   cmake --build build-temp
   # 完成后删除
   rm -rf build-temp
   ```

## 当前状态

由于权限限制，某些构建目录可能无法自动清理。建议手动执行清理：

```bash
# 查看当前构建目录
ls -d build* 2>/dev/null

# 手动清理（根据需要调整权限）
chmod -R u+w build build2 build-tests
rm -rf build build2 build-tests build-ubuntu-* build-linux-fresh-* \
       build-linux-reconnect build-test-quick
```

## 总结

- ✅ 保留 `build-linux-repro/` - 官方构建
- ✅ 保留 `build-portable/` - 可移植测试
- ❌ 删除其他 `build-*` 目录 - 临时构建

---

**更新时间**: 2026-09-21
