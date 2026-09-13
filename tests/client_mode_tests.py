"""Both client modes must preserve protocol, timeout and cancellation behavior."""
import argparse
import asyncio
import unittest
from unittest.mock import patch

from load_benchmark import benchmark, exchange, read_reply, RespError


class ModeTests(unittest.IsolatedAsyncioTestCase):
    async def test_fragmented_nested_binary_replies(self):
        for combined in (False, True):
            reader = asyncio.StreamReader()
            payload = b'*5\r\n+OK\r\n:2\r\n$5\r\na\x00\r\nb\r\n$-1\r\n-ERR BUSY\r\n+NEXT\r\n'
            async def feed():
                for byte in payload:
                    reader.feed_data(bytes([byte]))
                    await asyncio.sleep(0)
                reader.feed_eof()
            feeder = asyncio.create_task(feed())
            result = await read_reply(reader, combined_header=combined)
            self.assertEqual(result[:4], [b'OK', 2, b'a\x00\r\nb', None])
            self.assertIsInstance(result[4], RespError)
            self.assertEqual(str(result[4]), 'ERR BUSY')
            self.assertEqual(await read_reply(reader, combined_header=combined), b'NEXT')
            await feeder

    async def test_invalid_and_truncated_replies(self):
        cases = [b'$-2\r\n', b'*1025\r\n', b'$8388609\r\n', b'$1\r\nx!!',
                 b'?bad\r\n', b':' + b'x\r\n', b'+' + b'x' * 1023 + b'\r\n',
                 b'*1\r\n' * 18 + b'+OK\r\n', b'$4\r\nx', b'+OK\r', b'']
        for payload in cases:
            for combined in (False, True):
                with self.subTest(payload=payload[:20], combined=combined):
                    reader = asyncio.StreamReader()
                    reader.feed_data(payload)
                    reader.feed_eof()
                    with self.assertRaises((ValueError, asyncio.IncompleteReadError)):
                        await read_reply(reader, combined_header=combined)

    async def test_timeout_keeps_prefix_and_reconnects(self):
        # One successful reply then a stalled second reply in each batch.
        for mode in ('classic', 'combined-header'):
            streams = []
            class Writer:
                def __init__(self, reader): self.reader, self.closed = reader, False
                def write(self, _payload): self.reader.feed_data(b'+OK\r\n')
                async def drain(self): pass
                def close(self): self.closed = True
                async def wait_closed(self): pass
            async def connect(*_a, **_kw):
                reader = asyncio.StreamReader()
                writer = Writer(reader)
                streams.append(writer)
                return reader, writer
            async def one(_r, _w, payload, _timeout):
                return b'state:leader\r\n' if b'INFO' in payload else b'OK'
            args = argparse.Namespace(host='unused', port=1, connections=1, requests=4, pipeline=2,
                value_size=8, write_ratio=1, timeout=0.02, namespace='bench', client_mode=mode)
            with patch('load_benchmark.asyncio.open_connection', connect), patch('load_benchmark.one', one):
                result = await benchmark(args)
            self.assertEqual((result['attempted'], result['success'], result['errors']), (4, 2, 2))
            self.assertEqual(result['errors_by_category']['timeout'], 2)
            self.assertEqual(result['completed_batches'], 0)
            self.assertEqual(len(streams), 3)  # INFO, preload, replacement for the timed-out stream.
            self.assertTrue(all(w.closed for w in streams))

    async def test_external_cancellation_is_not_timeout(self):
        for combined in (False, True):
            sent = asyncio.Event()
            class Writer:
                def write(self, _payload): sent.set()
                async def drain(self): pass
            replies = []
            task = asyncio.create_task(asyncio.wait_for(
                exchange(asyncio.StreamReader(), Writer(), b'x', 1, replies, combined), 5))
            await sent.wait()
            task.cancel()
            with self.assertRaises(asyncio.CancelledError): await task
            self.assertEqual(replies, [])


if __name__ == '__main__':
    unittest.main(verbosity=2)
