#!/usr/bin/env python3
"""
集成测试：验证每连接命令队列功能
"""

import sys
import os
import socket
import time

def send_resp_command(sock, *args):
    """发送 RESP 命令"""
    cmd = f"*{len(args)}\r\n"
    for arg in args:
        arg_str = str(arg)
        cmd += f"${len(arg_str)}\r\n{arg_str}\r\n"
    sock.sendall(cmd.encode())

def read_resp_response(sock, timeout=5.0):
    """读取 RESP 响应"""
    sock.settimeout(timeout)
    response = b""
    try:
        while True:
            chunk = sock.recv(4096)
            if not chunk:
                break
            response += chunk
            # 简单判断：响应完整性
            if response.startswith(b"+") or response.startswith(b"-"):
                if response.endswith(b"\r\n"):
                    break
            elif response.startswith(b"$"):
                # Bulk string
                lines = response.split(b"\r\n")
                if len(lines) >= 3 and lines[-1] == b"":
                    break
    except socket.timeout:
        pass
    return response.decode('utf-8', errors='ignore')

def test_single_connection_order(port=8080):
    """测试单连接命令顺序"""
    print("\n" + "=" * 70)
    print("测试 1: 单连接 SET → GET 顺序保证")
    print("=" * 70)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(("127.0.0.1", port))

    try:
        # SET 然后立即 GET
        send_resp_command(sock, "SET", "order_test", "value123")
        set_response = read_resp_response(sock)
        print(f"SET 响应: {set_response.strip()}")

        send_resp_command(sock, "GET", "order_test")
        get_response = read_resp_response(sock)
        print(f"GET 响应: {get_response.strip()}")

        if "value123" in get_response:
            print("✅ 通过: GET 读取到 SET 的值")
            return True
        else:
            print("❌ 失败: GET 未读取到 SET 的值")
            return False
    finally:
        sock.close()

def test_pipeline_order(port=8080):
    """测试 Pipeline 命令顺序"""
    print("\n" + "=" * 70)
    print("测试 2: Pipeline 模式命令顺序")
    print("=" * 70)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(("127.0.0.1", port))

    try:
        # 一次性发送多个命令
        pipeline = ""
        pipeline += "*3\r\n$3\r\nSET\r\n$8\r\npipeline\r\n$6\r\nvalue1\r\n"
        pipeline += "*2\r\n$3\r\nGET\r\n$8\r\npipeline\r\n"
        pipeline += "*3\r\n$3\r\nSET\r\n$8\r\npipeline\r\n$6\r\nvalue2\r\n"
        pipeline += "*2\r\n$3\r\nGET\r\n$8\r\npipeline\r\n"

        sock.sendall(pipeline.encode())

        # 读取所有响应
        responses = []
        for i in range(4):
            response = read_resp_response(sock)
            responses.append(response)
            print(f"响应 {i+1}: {response.strip()}")

        # 验证顺序
        if "value1" in responses[1] and "value2" in responses[3]:
            print("✅ 通过: Pipeline 命令按顺序执行")
            return True
        else:
            print("❌ 失败: Pipeline 命令顺序错误")
            return False
    finally:
        sock.close()

def test_mixed_commands(port=8080):
    """测试混合读写命令"""
    print("\n" + "=" * 70)
    print("测试 3: 混合读写命令顺序")
    print("=" * 70)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(("127.0.0.1", port))

    try:
        # SET → GET → SET → GET
        send_resp_command(sock, "SET", "mixed_key", "first")
        read_resp_response(sock)

        send_resp_command(sock, "GET", "mixed_key")
        get1 = read_resp_response(sock)
        print(f"第一次 GET: {get1.strip()}")

        send_resp_command(sock, "SET", "mixed_key", "second")
        read_resp_response(sock)

        send_resp_command(sock, "GET", "mixed_key")
        get2 = read_resp_response(sock)
        print(f"第二次 GET: {get2.strip()}")

        if "first" in get1 and "second" in get2:
            print("✅ 通过: 混合命令顺序正确")
            return True
        else:
            print("❌ 失败: 混合命令顺序错误")
            return False
    finally:
        sock.close()

def main():
    """运行所有测试"""
    print("=" * 70)
    print("每连接命令队列集成测试")
    print("=" * 70)
    print("\n注意: 请确保服务器正在运行")
    print("启动命令示例:")
    print("  ./build-linux-repro/server/raft_kv_server --node_id=0 \\")
    print("    --client_port=8080 --raft_port=9090 \\")
    print("    --peers=0:127.0.0.1:9090,1:127.0.0.1:9091,2:127.0.0.1:9092")
    print()

    # 等待用户确认或自动继续
    time.sleep(1)

    port = 8080
    if len(sys.argv) > 1:
        port = int(sys.argv[1])

    results = []

    try:
        results.append(test_single_connection_order(port))
        time.sleep(0.5)

        results.append(test_pipeline_order(port))
        time.sleep(0.5)

        results.append(test_mixed_commands(port))

    except ConnectionRefusedError:
        print(f"\n❌ 无法连接到服务器 127.0.0.1:{port}")
        print("请先启动服务器")
        return 1
    except Exception as e:
        print(f"\n❌ 测试出错: {e}")
        import traceback
        traceback.print_exc()
        return 1

    print("\n" + "=" * 70)
    print(f"测试结果: {sum(results)}/{len(results)} 通过")
    print("=" * 70)

    if all(results):
        print("✅ 所有测试通过！每连接命令队列工作正常。")
        return 0
    else:
        print("❌ 部分测试失败")
        return 1

if __name__ == "__main__":
    sys.exit(main())
