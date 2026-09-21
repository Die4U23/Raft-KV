#!/usr/bin/env python3
"""
测试每连接命令队列功能
验证同一连接上 SET -> GET 的顺序保证
"""

import socket
import time
import sys

def send_command(sock, *args):
    """发送 RESP 命令"""
    cmd = f"*{len(args)}\r\n"
    for arg in args:
        cmd += f"${len(arg)}\r\n{arg}\r\n"
    sock.sendall(cmd.encode())

def read_response(sock):
    """读取 RESP 响应"""
    response = b""
    while True:
        chunk = sock.recv(1024)
        if not chunk:
            break
        response += chunk
        # 简单判断：如果以 \r\n 结尾，认为响应完整
        if response.endswith(b"\r\n"):
            break
    return response.decode()

def test_set_then_get_order(host="127.0.0.1", port=8080):
    """测试 SET 后立即 GET 的顺序"""
    print("测试 1: SET 后立即 GET 的顺序保证")

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))

    try:
        # 发送 SET 命令
        send_command(sock, "SET", "test_key", "test_value")

        # 立即发送 GET 命令（不等待 SET 响应）
        send_command(sock, "GET", "test_key")

        # 读取 SET 响应
        set_response = read_response(sock)
        print(f"  SET 响应: {set_response.strip()}")

        # 读取 GET 响应
        get_response = read_response(sock)
        print(f"  GET 响应: {get_response.strip()}")

        # 验证 GET 返回了 SET 的值
        if "test_value" in get_response:
            print("  ✅ 通过：GET 读取到 SET 的值")
            return True
        else:
            print("  ❌ 失败：GET 未读取到 SET 的值")
            return False

    finally:
        sock.close()

def test_multiple_sets_then_get(host="127.0.0.1", port=8080):
    """测试多个 SET 后 GET"""
    print("\n测试 2: 多个 SET 后 GET")

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))

    try:
        # 发送三个 SET 命令
        send_command(sock, "SET", "key1", "value1")
        send_command(sock, "SET", "key2", "value2")
        send_command(sock, "SET", "key3", "value3")

        # 读取三个 SET 响应
        for i in range(3):
            response = read_response(sock)
            print(f"  SET {i+1} 响应: {response.strip()}")

        # 发送 GET 命令验证最后一个 SET
        send_command(sock, "GET", "key3")
        get_response = read_response(sock)
        print(f"  GET 响应: {get_response.strip()}")

        if "value3" in get_response:
            print("  ✅ 通过：GET 读取到最后一个 SET 的值")
            return True
        else:
            print("  ❌ 失败：GET 未读取到正确的值")
            return False

    finally:
        sock.close()

def test_set_ping_set_get(host="127.0.0.1", port=8080):
    """测试 SET -> PING -> SET -> GET 的顺序"""
    print("\n测试 3: SET -> PING -> SET -> GET 顺序")

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))

    try:
        # SET -> PING -> SET -> GET
        send_command(sock, "SET", "ping_test", "first")
        send_command(sock, "PING")
        send_command(sock, "SET", "ping_test", "second")
        send_command(sock, "GET", "ping_test")

        # 读取所有响应
        responses = []
        for i in range(4):
            response = read_response(sock)
            responses.append(response.strip())
            print(f"  响应 {i+1}: {response.strip()}")

        # 验证最后的 GET 返回 "second"
        if "second" in responses[-1]:
            print("  ✅ 通过：命令按顺序执行")
            return True
        else:
            print("  ❌ 失败：命令顺序错误")
            return False

    finally:
        sock.close()

def test_pipeline_commands(host="127.0.0.1", port=8080):
    """测试 pipeline 模式下的命令顺序"""
    print("\n测试 4: Pipeline 模式下的命令顺序")

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))

    try:
        # 一次性发送多个命令（pipeline）
        pipeline = ""
        pipeline += "*3\r\n$3\r\nSET\r\n$4\r\npipe\r\n$5\r\nstart\r\n"  # SET pipe start
        pipeline += "*2\r\n$3\r\nGET\r\n$4\r\npipe\r\n"  # GET pipe
        pipeline += "*3\r\n$3\r\nSET\r\n$4\r\npipe\r\n$3\r\nend\r\n"  # SET pipe end
        pipeline += "*2\r\n$3\r\nGET\r\n$4\r\npipe\r\n"  # GET pipe

        sock.sendall(pipeline.encode())

        # 读取所有响应
        responses = []
        for i in range(4):
            response = read_response(sock)
            responses.append(response.strip())
            print(f"  响应 {i+1}: {response.strip()}")

        # 验证：第一个 GET 应该返回 "start"，第二个 GET 应该返回 "end"
        if "start" in responses[1] and "end" in responses[3]:
            print("  ✅ 通过：Pipeline 命令按顺序执行")
            return True
        else:
            print("  ❌ 失败：Pipeline 命令顺序错误")
            return False

    finally:
        sock.close()

def main():
    if len(sys.argv) > 1:
        host = sys.argv[1]
    else:
        host = "127.0.0.1"

    if len(sys.argv) > 2:
        port = int(sys.argv[2])
    else:
        port = 8080

    print(f"连接到 {host}:{port}")
    print("=" * 60)

    results = []

    try:
        results.append(test_set_then_get_order(host, port))
        time.sleep(0.1)

        results.append(test_multiple_sets_then_get(host, port))
        time.sleep(0.1)

        results.append(test_set_ping_set_get(host, port))
        time.sleep(0.1)

        results.append(test_pipeline_commands(host, port))

    except ConnectionRefusedError:
        print(f"\n❌ 无法连接到服务器 {host}:{port}")
        print("请确保服务器正在运行")
        return 1
    except Exception as e:
        print(f"\n❌ 测试出错: {e}")
        import traceback
        traceback.print_exc()
        return 1

    print("\n" + "=" * 60)
    print(f"测试结果: {sum(results)}/{len(results)} 通过")

    if all(results):
        print("✅ 所有测试通过！每连接命令队列工作正常。")
        return 0
    else:
        print("❌ 部分测试失败")
        return 1

if __name__ == "__main__":
    sys.exit(main())
