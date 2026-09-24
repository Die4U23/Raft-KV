# Git 分支整理建议

## 当前分支状态

### 活跃分支 (保留)
- ✅ `main` - 主分支
- 📋 `feature/per-connection-queue` - 待创建 PR
- 📋 `short-term-improvements` - 待创建 PR

### 已合并分支 (可删除)
- ✅ `security-fixes-phase1` - 已合并到 main

### 旧分支 (可能已废弃)
- ⚠️ `raft-cluster`
- ⚠️ `raft-cluster-namespace`

## 建议的整理步骤

### 立即操作 (在 PR 创建后)

```bash
# 1. 等待 feature/per-connection-queue 和 short-term-improvements 合并后

# 2. 切换到 main 分支
git checkout main
git pull origin main

# 3. 删除已合并的本地分支
git branch -d security-fixes-phase1

# 4. 删除已合并的本地分支 (在 PR 合并后)
git branch -d feature/per-connection-queue
git branch -d short-term-improvements

# 5. 删除远程已合并的分支 (在 PR 合并后)
git push origin --delete security-fixes-phase1
git push origin --delete feature/per-connection-queue
git push origin --delete short-term-improvements
```

### 检查旧分支

```bash
# 查看旧分支的最后提交时间
git show raft-cluster --format="%ci" --quiet
git show raft-cluster-namespace --format="%ci" --quiet

# 如果确认不需要，删除它们
git branch -D raft-cluster
git branch -D raft-cluster-namespace
git push origin --delete raft-cluster
git push origin --delete raft-cluster-namespace
```

## 推荐的分支策略

### 日常开发流程

```
main (稳定分支)
  ↓
feature/xxx (功能分支)
  ↓
创建 PR
  ↓
Code Review
  ↓
合并到 main
  ↓
删除 feature/xxx 分支
```

### 分支命名规范

- `feature/xxx` - 新功能
- `fix/xxx` - Bug 修复
- `refactor/xxx` - 重构
- `docs/xxx` - 文档更新
- `test/xxx` - 测试相关

### 保持整洁的原则

1. ✅ 及时删除已合并的分支
2. ✅ 一个功能一个分支
3. ✅ PR 合并后立即删除
4. ✅ 定期清理旧分支

## 当前建议

### 现在可以做的

```bash
# 删除已合并的 security-fixes-phase1
git checkout main
git branch -d security-fixes-phase1
git push origin --delete security-fixes-phase1
```

### PR 合并后做的

```bash
# 等待 PR 合并后
git checkout main
git pull origin main

# 删除本地分支
git branch -d feature/per-connection-queue
git branch -d short-term-improvements

# 删除远程分支 (GitHub 可能自动删除，如果没有则手动删除)
git push origin --delete feature/per-connection-queue
git push origin --delete short-term-improvements
```

### 检查旧分支 (可选)

如果 `raft-cluster` 和 `raft-cluster-namespace` 不再需要：

```bash
# 删除本地
git branch -D raft-cluster raft-cluster-namespace

# 删除远程
git push origin --delete raft-cluster
git push origin --delete raft-cluster-namespace
```

## 最终理想状态

```
本地分支:
  * main

远程分支:
  * origin/main
```

---

**建议执行时间**: PR 合并后
