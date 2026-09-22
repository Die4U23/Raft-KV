# Architecture Fixes Summary

## Overview

This document summarizes all fixes applied to address issues identified in the architecture review (2026-09-22).

## Fixed Issues

### F1 (P1): ReadIndex accepting old ACKs ✅

**Problem**: AppendEntriesResponse handler accepted any current-term response for all unconfirmed read rounds, allowing old in-flight RPCs to satisfy new read requests.

**Fix**: 
- Modified `HandleAppendEntriesResponse` to bind each response to its specific read round using RPC ID matching
- Only responses from RPCs initiated by a specific round can confirm that round
- Location: `src/raft/raft_node.cc:296-326`

**Impact**: Eliminates stale reads from isolated old leaders using delayed responses.

**Commit**: `60ddf84`

---

### F2 (P1): Follower serving stale data ✅

**Problem**: When `linearizable_reads=true`, Followers would still serve local stale data instead of rejecting requests.

**Fix**: 
- Added leader check before ReadIndex path in GET command handler
- Non-leaders now return `MOVED` error with leader ID
- Location: `src/server/main.cpp:376-380`

**Impact**: Linearizable read guarantee now enforced cluster-wide.

**Commit**: `60ddf84`

---

### F3 (P1): Connection queue bypassing memory limits ✅

**Problem**: Commands moved from input buffer to unbounded per-connection queue, bypassing global memory limits.

**Fix**:
- Added `kMaxPerConnectionQueue` (1000 commands) and `kMaxPerConnectionQueueBytes` (4MB)
- Stop reading when limits reached, resume when space available
- Track `queued_bytes` per session
- Location: `src/server/main.cpp:203-219, 493-537`

**Impact**: Memory-safe under slow consumers or lost quorum scenarios.

**Commit**: `60ddf84`

---

### F4 (P1): Unknown commands hanging connection ✅

**Problem**: Invalid commands (e.g., `BOGUS`, malformed SET) would enter queue but have no execution path, leaving `executing=true` forever.

**Fix**:
- Added fallback error handler in `ExecuteNextCommand` for unrecognized commands
- All commands now complete exactly once
- Location: `src/server/main.cpp:455-458`

**Impact**: All commands complete, maintaining connection liveness.

**Commit**: `60ddf84`

---

### F5 (P2): Parameter error responses bypassing command order ✅

**Problem**: Invalid commands (wrong arg count) would send error via immediate `SendReply`, bypassing queue and breaking RESP protocol ordering.

**Fix**:
- Added `ERROR` command type for invalid commands
- All commands (valid and invalid) now enter queue to preserve order
- Error commands processed in-order through `ExecuteNextCommand`
- Location: `src/server/main.cpp:227-230, 346-349, 505-539`

**Impact**: RESP protocol ordering guaranteed - clients can reliably match responses in pipeline mode.

**Commit**: `673fbb6`

---

### F6 (P1): Test coverage gaps ✅

**Problem**: 
- `connection_order_tests` was documentation-only with 0 actual assertions
- CI only ran portable unit tests, not integration tests

**Fix**:
- Rewrote `connection_order_tests` with 4 real tests and 15 assertions
- Added `integration.yml` CI workflow to run cluster and integration tests
- Location: `tests/connection_order_tests.cpp`, `.github/workflows/integration.yml`

**Impact**: Real test coverage for command ordering semantics; CI now validates full system behavior.

**Commit**: `adb93eb`

---

### F7 (P2): Windows compilation error ✅

**Problem**: `std::max(1L, next_index_[peer_id] - 1)` failed on MinGW/GCC 13.2 due to type mismatch between `long` and `int64_t`.

**Fix**: Use explicit template parameter `std::max<int64_t>(1, ...)`

**Location**: `tests/replication_edge_cases_unit.cpp:96`

**Impact**: Portable tests now compile on Windows.

**Commit**: `673fbb6`

---

## Test Results

### C++ Unit Tests: 13/13 ✅
- `peer_manager_transport_tests`
- `protocol_tests`
- `core_tests`
- `storage_batch_tests`
- `async_executor_tests`
- `batch_flush_tests`
- `peer_retry_tests`
- `storage_failure_tests`
- `replication_logic_tests`
- `connection_order_tests` (now with 15 assertions, was 0)
- `replication_partition_tests`
- `readindex_tests`
- `replication_edge_cases_unit` (now compiles on Windows)

### Cluster Smoke Tests: All Pass ✅
- Three-node leader election
- Binary SET/GET pipeline
- Pipeline spanning 128-command batches
- Namespace isolation and switching
- DEL operations
- Follower write rejection
- 32 concurrent connections
- Leader crash recovery
- Node restart convergence

### Integration Tests: 10/10 ✅

**ReadIndex Integration Tests (4/4)**:
- Network partition handling
- Write-then-read consistency
- Concurrent reads
- Leader change

**Linearizable Read Tests (6/6)**:
- Default local read behavior
- Write-then-read consistency
- Concurrent reads consistency
- Follower read behavior
- Heartbeat batching efficiency
- Network partition prevents stale reads

---

## Architecture Review Status

| Issue | Priority | Status | Commit |
|-------|----------|--------|--------|
| F1: ReadIndex accepting old ACKs | P1 | ✅ Fixed | 60ddf84 |
| F2: Follower serving stale data | P1 | ✅ Fixed | 60ddf84 |
| F3: Connection queue bypassing limits | P1 | ✅ Fixed | 60ddf84 |
| F4: Unknown commands hanging | P1 | ✅ Fixed | 60ddf84 |
| F5: Error responses out of order | P2 | ✅ Fixed | 673fbb6 |
| F6: Test coverage gaps | P1 | ✅ Fixed | adb93eb |
| F7: Windows compilation error | P2 | ✅ Fixed | 673fbb6 |

**All identified architecture issues are now resolved.**

---

## Related Documents

- Architecture review: `docs/architecture-review-2026-09-22.md`
- ReadIndex design: `docs/readindex_design_checklist.md`
- Test report: `docs/test-report.md`

---

## Pull Requests

- PR #7: Initial ReadIndex implementation
- PR #9: P1 architecture fixes (F1-F4) - **SUPERSEDED**
- PR #10: Complete architecture fixes (F1-F7) - **CURRENT**
