#!/usr/bin/env python3
"""
集成测试：验证每连接命令队列功能
在真实的三节点集群上测试 SET → GET 顺序保证
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

from cluster_smoke import ThreeNodeCluster, redis_command
import time

def test_per_connection_queue():
    """测试每连接命令队列功能"""
    print("=" * 70)
    print("集成测试：每连接命令队列功能验证")
    print("=" * 70)

    cluster = ThreeNodeCluster(server_binary="./build-linux-repro/server/raft_kv_server")
    cluster.start()

    try:
        # 等待选举完成
        time.sleep(2)
        leader = cluster.wait_for_leader(timeout=10)
        print(f"\n✓ Leader 选举完成: Node {leader.node_id}")

        # 测试 1: 基本的 SET → GET 顺序
        print("\n测试 1: SET → GET 顺序保证")
        result = redis_command(leader, "SET", "test_key", "test_value")
        print(f"  SET 响应: {result}")

        result = redis_command(leader, "GET", "test_key")
        print(f"  GET 响应: {result}")
        assert result == b"test_value", f"Expected b'test_value', got {result}"
        print("  ✅ 通过: GET 读取到 SET 的值")

        # 测试 2: Pipeline 模式（一次发送多个命令）
        print("\n测试 2: Pipeline 模式命令顺序")
        conn = leader.connect()

        # 发送 pipeline 命令
        conn.sendall(b"*3\r\n$3\r\nSET\r\n$4\r\npipe\r\n$5\r\nfirst\r\n")
        conn.sendall(b"*2\r\n$3\r\nGET\r\n$4\r\npipe\r\n")
        conn.sendall(b"*3\r\n$3\r\nSET\r\n$4\r\npipe\r\n$6\r\nsecond\r\n")
        conn.sendall(b"*2\r\n$3\r\nGET\r\n$4\r\npipe\r\n")

        # 读取响应
        responses = []
        for _ in range(4):
            response = b""
            while not response.endswith(b"\r\n"):
                chunk = conn.recv(1024)
                if not chunk:
                    break
                response += chunk
            responses.append(response)

        conn.close()

        print(f"  响应 1 (SET first): {responses[0]}")
        print(f"  响应 2 (GET): {responses[1]}")
        print(f"  响应 3 (SET second): {responses[2]}")
        print(f"  响应 4 (GET): {responses[3]}")

        # 验证第一个 GET 返回 "first"，第二个 GET 返回 "second"
        assert b"first" in responses[1], f"Expected 'first' in {responses[1]}"
        assert b"second" in responses[3], f"Expected 'second' in {responses[3]}"
        print("  ✅ 通过: Pipeline 命令按顺序执行")

        # 测试 3: 混合读写命令
        print("\n测试 3: 混合读写命令顺序")
        redis_command(leader, "SET", "mixed", "value1")
        result1 = redis_command(leader, "GET", "mixed")
        redis_command(leader, "SET", "mixed", "value2")
        result2 = redis_command(leader, "GET", "mixed")

        print(f"  第一次 GET: {result1}")
        print(f"  第二次 GET: {result2}")

        assert result1 == b"value1", f"Expected b'value1', got {result1}"
        assert result2 == b"value2", f"Expected b'value2', got {result2}"
        print("  ✅ 通过: 混合命令顺序正确")

        # 测试 4: 并发连接（验证多连接仍然并发）
        print("\n测试 4: 多连接并发执行")
        import threading

        results = []
        def write_and_read(key, value):
            try:
                redis_command(leader, "SET", key, value)
                result = redis_command(leader, "GET", key)
                results.append((key, result))
            except Exception as e:
                results.append((key, f"ERROR: {e}"))

        threads = []
        for i in range(10):
            t = threading.Thread(target=write_and_read, args=(f"concurrent_{i}", f"value_{i}"))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        print(f"  并发写入并读取 10 个键")
        success = all(result == f"value_{i}".encode() for i, (key, result) in enumerate(results) if not isinstance(result, str))
        assert success, f"Some concurrent operations failed: {results}"
        print("  ✅ 通过: 多连接并发正常工作")

        print("\n" + "=" * 70)
        print("✅ 所有每连接命令队列测试通过！")
        print("=" * 70)

        return True

    except AssertionError as e:
        print(f"\n❌ 测试失败: {e}")
        return False
    except Exception as e:
        print(f"\n❌ 测试出错: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        cluster.stop()

if __name__ == "__main__":
    success = test_per_connection_queue()
    sys.exit(0 if success else 1)
