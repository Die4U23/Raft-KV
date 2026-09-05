#!/usr/bin/env python3
"""Bounded asyncio RESP load client. --self-test never connects to a server."""
import argparse
import asyncio
from collections import Counter
import json
import math
from pathlib import Path
import platform
import random
import sys
import time
import unittest
from unittest.mock import patch
import uuid


class RespError(Exception):
    pass


def encode(*args):
    parts = [arg if isinstance(arg, bytes) else str(arg).encode() for arg in args]
    return b"*%d\r\n" % len(parts) + b"".join(b"$%d\r\n" % len(p) + p + b"\r\n" for p in parts)


async def read_reply(reader, depth=0):
    if depth > 16:
        raise ValueError("RESP nesting exceeds limit")
    prefix = await reader.readexactly(1)
    line = await reader.readuntil(b"\r\n")
    if len(line) > 1024:
        raise ValueError("RESP header exceeds limit")
    line = line[:-2]
    if prefix == b"+":
        return line
    if prefix == b"-":
        return RespError(line.decode(errors="replace"))
    if prefix == b":":
        return int(line)
    if prefix in (b"$", b"*"):
        size = int(line)
        if size == -1:
            return None
        if size < 0 or size > (8 * 1024 * 1024 if prefix == b"$" else 1024):
            raise ValueError("RESP length exceeds limit")
        if prefix == b"*":
            return [await read_reply(reader, depth + 1) for _ in range(size)]
        data = await reader.readexactly(size + 2)
        if data[-2:] != b"\r\n":
            raise ValueError("invalid bulk terminator")
        return data[:-2]
    raise ValueError("unknown RESP type")


async def exchange(reader, writer, payload, count, replies):
    writer.write(payload)
    await writer.drain()
    for _ in range(count):
        replies.append(await read_reply(reader))  # Preserve replies received before a timeout.


async def close(writer):
    if writer is not None:
        writer.close()
        try:
            await asyncio.wait_for(writer.wait_closed(), 1)
        except (OSError, asyncio.TimeoutError):
            pass


async def one(reader, writer, payload, timeout):
    replies = []
    await asyncio.wait_for(exchange(reader, writer, payload, 1, replies), timeout)
    return replies[0]


async def selected_connection(args):
    reader, writer = await asyncio.open_connection(args.host, args.port, limit=8192)
    try:
        reply = await one(reader, writer, encode("SELECT", args.namespace), args.timeout)
        if reply != b"OK":
            raise RuntimeError("SELECT failed: {!r}".format(reply))
        return reader, writer
    except BaseException:
        await close(writer)
        raise


def category(reply):
    message = str(reply).upper()
    if "MOVED " in message:
        return "moved"
    if "PROPOSAL LIMIT" in message or "OVERLOAD" in message or "BUSY" in message:
        return "overload"
    return "other"


def percentile(values, percent):
    if not values:
        return None
    ordered = sorted(values)
    index = (len(ordered) - 1) * percent / 100
    lower = math.floor(index)
    return ordered[lower] + (ordered[math.ceil(index)] - ordered[lower]) * (index - lower)


class Samples:
    def __init__(self, capacity=100000):
        self.values, self.count, self.capacity = [], 0, capacity
        self.rng = random.Random(0)

    def add(self, value):
        self.count += 1
        if len(self.values) < self.capacity:
            self.values.append(value)
        else:
            index = self.rng.randrange(self.count)
            if index < self.capacity:
                self.values[index] = value


async def benchmark(args):
    reader, writer = await asyncio.wait_for(asyncio.open_connection(args.host, args.port), args.timeout)
    try:
        info = await one(reader, writer, encode("INFO"), args.timeout)
        if not isinstance(info, bytes):
            raise RuntimeError("INFO failed: {!r}".format(info))
        fields = dict(line.split(":", 1) for line in info.decode().splitlines() if ":" in line)
        if fields.get("state") != "leader":
            raise RuntimeError("target is not leader (state={}, leader_id={}); supply its actual endpoint".format(
                fields.get("state"), fields.get("leader_id")))
    finally:
        await close(writer)
    workers = min(args.connections, args.requests)
    value = b"x" * args.value_size
    run_id = uuid.uuid4().hex
    keys = ["run-{}-worker-{}".format(run_id, index) for index in range(workers)]

    async def preload(index):
        reader, writer = await asyncio.wait_for(selected_connection(args), args.timeout)
        try:
            reply = await one(reader, writer, encode("SET", keys[index], value), args.timeout)
            if reply != b"OK":
                raise RuntimeError("preload failed for worker {}: {!r}".format(index, reply))
            return reader, writer
        except BaseException:
            await close(writer)
            raise

    prepared = await asyncio.gather(*(preload(i) for i in range(workers)), return_exceptions=True)
    active = [item if isinstance(item, tuple) else (None, None) for item in prepared]
    failures = [item for item in prepared if isinstance(item, BaseException)]
    if failures:
        await asyncio.gather(*(close(writer) for _, writer in active))
        raise RuntimeError("preload/setup failed; no measurement started: {}".format(failures[0]))
    samples = Samples()

    async def worker(index, offset, count):
        counts = Counter()
        set_command, get_command = encode("SET", keys[index], value), encode("GET", keys[index])
        for position in range(0, count, args.pipeline):
            size = min(args.pipeline, count - position)
            writes = [math.floor((offset + position + i + 1) * args.write_ratio) >
                      math.floor((offset + position + i) * args.write_ratio) for i in range(size)]
            payload = b"".join(set_command if write else get_command for write in writes)
            counts["attempted"] += size
            counts["writes"] += sum(writes)
            counts["reads"] += size - sum(writes)
            replies, error = [], None
            try:
                if active[index][1] is None:
                    active[index] = await asyncio.wait_for(selected_connection(args), args.timeout)
                reader, writer = active[index]
                started = time.perf_counter()
                await asyncio.wait_for(exchange(reader, writer, payload, size, replies), args.timeout)
                samples.add((time.perf_counter() - started) * 1000)
            except asyncio.TimeoutError:
                error = "timeout"
            except (OSError, EOFError):
                error = "connection"
            except (ValueError, RuntimeError, asyncio.LimitOverrunError):
                error = "other"
            for reply, write in zip(replies, writes):
                if isinstance(reply, RespError):
                    counts[category(reply)] += 1
                elif reply == (b"OK" if write else value):
                    counts["success"] += 1
                else:
                    counts["other"] += 1
            if error:
                counts[error] += size - len(replies)
                await close(active[index][1])
                active[index] = (None, None)  # Never reuse a stream with unmatched replies.
        return counts

    counts, offset, jobs = Counter(), 0, []
    for index in range(workers):
        count = args.requests // workers + (index < args.requests % workers)
        jobs.append(worker(index, offset, count))
        offset += count
    started = time.perf_counter()
    try:
        for result in await asyncio.gather(*jobs):
            counts.update(result)
        elapsed = time.perf_counter() - started
    finally:
        await asyncio.gather(*(close(writer) for _, writer in active))
    errors = {key: counts[key] for key in ("moved", "overload", "other", "timeout", "connection")}
    return {"status": "COMPLETED", "run_id": run_id, "server_info_before": fields,
            "client_python": platform.python_version(), "client_platform": platform.platform(),
            "configuration": {key: value for key, value in vars(args).items() if key not in ("output", "self_test")},
            "effective_connections": workers, "requested": args.requests, "attempted": counts["attempted"],
            "success": counts["success"], "errors": sum(errors.values()), "errors_by_category": errors,
            "reads_attempted": counts["reads"], "writes_attempted": counts["writes"],
            "elapsed_seconds": elapsed, "goodput_ops_per_second": counts["success"] / elapsed,
            "completed_batches": samples.count, "latency_sample_count": len(samples.values),
            "batch_latency_ms": {"p" + str(p): percentile(samples.values, p) for p in (50, 95, 99)},
            "latency_semantics": "batch send invocation to last reply, completed batches only; not per-op latency"}


class ClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_measurement_accounting(self):
        # Exercise the whole client against an in-process RESP model. This is
        # not a server performance measurement and never opens a socket.
        data = {}
        class Writer:
            def __init__(self, reader):
                self.reader, self.jobs = reader, []
            def write(self, payload):
                async def respond():
                    request = asyncio.StreamReader()
                    request.feed_data(payload)
                    request.feed_eof()
                    while not request.at_eof():
                        command = await read_reply(request)
                        op = command[0]
                        if op == b"INFO":
                            value = b"state:leader\r\nleader_id:10\r\n"
                            reply = b"$%d\r\n" % len(value) + value + b"\r\n"
                        elif op == b"SELECT":
                            reply = b"+OK\r\n"
                        elif op == b"SET":
                            data[command[1]] = command[2]
                            reply = b"+OK\r\n"
                        else:
                            value = data[command[1]]
                            reply = b"$%d\r\n" % len(value) + value + b"\r\n"
                        self.reader.feed_data(reply)
                self.jobs.append(asyncio.create_task(respond()))
            async def drain(self):
                pass
            def close(self):
                pass
            async def wait_closed(self):
                await asyncio.gather(*self.jobs)
                self.reader.feed_eof()
        async def connect(*_args, **_kwargs):
            reader = asyncio.StreamReader()
            return reader, Writer(reader)
        args = argparse.Namespace(host="unused", port=1, connections=3, requests=103,
                                  pipeline=8, value_size=16, write_ratio=0.5, timeout=5,
                                  namespace="bench", output=None, self_test=False)
        with patch("asyncio.open_connection", connect):
            report = await benchmark(args)
        self.assertEqual((report["attempted"], report["success"], report["errors"]), (103, 103, 0))
        self.assertEqual((report["writes_attempted"], report["reads_attempted"]), (51, 52))
        self.assertEqual(report["completed_batches"], 15)

    async def test_resp(self):
        reader = asyncio.StreamReader()
        payload = b"*6\r\n+OK\r\n:1\r\n$5\r\na\x00\r\nb\r\n$-1\r\n-ERR MOVED 2\r\n*0\r\n+PONG\r\n"
        async def feed():
            for byte in payload:
                reader.feed_data(bytes([byte]))
                await asyncio.sleep(0)
            reader.feed_eof()
        feeder = asyncio.create_task(feed())
        reply = await read_reply(reader)
        self.assertEqual(reply[:4], [b"OK", 1, b"a\x00\r\nb", None])
        self.assertEqual(category(reply[4]), "moved")
        self.assertEqual(reply[5], [])
        self.assertEqual(await read_reply(reader), b"PONG")
        await feeder

    async def test_truncated_reply(self):
        reader = asyncio.StreamReader()
        reader.feed_data(b"$4\r\na")
        reader.feed_eof()
        with self.assertRaises(asyncio.IncompleteReadError):
            await read_reply(reader)

    def test_helpers(self):
        self.assertEqual(encode("GET", "键"), b"*2\r\n$3\r\nGET\r\n$3\r\n\xe9\x94\xae\r\n")
        self.assertEqual(percentile([1, 2, 3, 4], 50), 2.5)
        self.assertAlmostEqual(percentile([1, 2, 3, 4], 95), 3.85)
        self.assertIsNone(percentile([], 99))
        sample = Samples(3)
        for i in range(100):
            sample.add(i)
        self.assertEqual((sample.count, len(sample.values)), (100, 3))
        self.assertEqual(category(RespError("ERR proposal limit reached; retry later")), "overload")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int)
    for name, default in (("connections", 32), ("requests", 10000), ("pipeline", 1), ("value-size", 128)):
        parser.add_argument("--" + name, type=int, default=default)
    parser.add_argument("--write-ratio", type=float, default=0.5)
    parser.add_argument("--timeout", type=float, default=5)
    parser.add_argument("--namespace", default="bench")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        result = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(ClientTests))
        return 0 if result.wasSuccessful() else 1
    if args.port is None or not 1 <= args.port <= 65535:
        parser.error("--port must be provided and be between 1 and 65535")
    if not (1 <= args.connections <= 1024 and 1 <= args.requests <= 1000000 and
            1 <= args.pipeline <= 1024 and 0 <= args.value_size <= 1048576 and
            0 <= args.write_ratio <= 1 and 0 < args.timeout <= 60):
        parser.error("limits: connections/pipeline 1..1024, requests 1..1000000, value-size 0..1048576, ratio 0..1, timeout (0,60]")
    if min(args.requests, args.connections * args.pipeline) * (args.value_size + 256) > 128 * 1024 * 1024:
        parser.error("estimated simultaneous request/reply payload exceeds 128 MiB; reduce concurrency, pipeline or value-size")
    if not (1 <= len(args.namespace) <= 63 and all(c.isascii() and (c.isalnum() or c in "_-") for c in args.namespace)):
        parser.error("namespace must contain 1..63 ASCII letters, digits, underscores or hyphens")
    try:
        report = asyncio.run(benchmark(args))
    except Exception as error:
        report = {"status": "SETUP_OR_CLIENT_FAILED", "error": str(error)}
    rendered = json.dumps(report, indent=2, ensure_ascii=False) + "\n"
    print(rendered, end="")
    if args.output:
        args.output.write_text(rendered, encoding="utf-8")
    return 0 if report.get("status") == "COMPLETED" and report["errors"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
