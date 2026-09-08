"""Directed TCP relays for fault tests; only owned loopback connections are cut."""
import asyncio
import threading
import time


class RaftProxyMesh:
    def __init__(self, destinations):
        self.destinations = dict(destinations)
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self.loop.run_forever, daemon=True)
        self.servers = {}
        self.active = {}
        self.groups = {node: 0 for node in destinations}
        self.epoch = 0
        self.errors = []
        self.events = []
        self.stats = {}
        self.closed = False
        self.loop.set_exception_handler(lambda _, context: self.errors.append(str(context)))
        self.thread.start()
        try:
            self.ports = self.call(self._start())
        except BaseException:
            self.close()
            raise

    def call(self, coroutine):
        future = asyncio.run_coroutine_threadsafe(coroutine, self.loop)
        try:
            return future.result(timeout=5)
        except BaseException:
            future.cancel()
            raise

    async def _start(self):
        ports = {}
        for src in self.destinations:
            for dst, destination in self.destinations.items():
                if src == dst:
                    continue
                edge = (src, dst)
                self.stats[edge] = dict(accepted=0, refused=0, cut=0,
                                        request_bytes=0, reply_bytes=0)
                self.servers[edge] = await asyncio.start_server(
                    lambda reader, writer, edge=edge, destination=destination:
                    self._relay(edge, destination, reader, writer), '127.0.0.1', 0)
                ports[edge] = self.servers[edge].sockets[0].getsockname()[1]
        return ports

    def allowed(self, edge):
        return self.groups[edge[0]] == self.groups[edge[1]]

    async def _relay(self, edge, destination, reader, writer):
        task = asyncio.current_task()
        epoch = self.epoch
        writers = [writer]
        pumps = []
        self.active[task] = (edge, writers)
        try:
            if not self.allowed(edge):
                self.stats[edge]['refused'] += 1
                return
            self.stats[edge]['accepted'] += 1
            upstream, upstream_writer = await asyncio.wait_for(
                asyncio.open_connection('127.0.0.1', destination), timeout=1)
            writers.append(upstream_writer)
            if epoch != self.epoch or not self.allowed(edge):
                return

            async def pump(source, target, counter):
                while True:
                    data = await source.read(65536)
                    if not data:
                        return
                    target.write(data)
                    await target.drain()
                    self.stats[edge][counter] += len(data)

            pumps = [asyncio.create_task(pump(reader, upstream_writer, 'request_bytes')),
                     asyncio.create_task(pump(upstream, writer, 'reply_bytes'))]
            done, _ = await asyncio.wait(pumps, return_when=asyncio.FIRST_COMPLETED)
            for completed in done:
                completed.result()
        except (OSError, asyncio.TimeoutError):
            # Nodes may not have started yet, or may close a connection on term change.
            pass
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self.errors.append(repr(error))
        finally:
            for pump_task in pumps:
                pump_task.cancel()
            for stream in writers:
                stream.close()
            if pumps:
                await asyncio.gather(*pumps, return_exceptions=True)
            for stream in writers:
                try:
                    await asyncio.wait_for(stream.wait_closed(), timeout=1)
                except (OSError, asyncio.TimeoutError):
                    pass
            self.active.pop(task, None)

    async def _partition(self, groups):
        flattened = [node for group in groups for node in group]
        if len(flattened) != len(set(flattened)) or set(flattened) != set(self.destinations):
            raise ValueError('Partition must contain every node exactly once')
        self.groups = {node: group_id for group_id, group in enumerate(groups) for node in group}
        self.epoch += 1
        closing = []
        for task, (edge, writers) in list(self.active.items()):
            if not self.allowed(edge):
                self.stats[edge]['cut'] += 1
                for writer in writers:
                    writer.close()
                task.cancel()
                closing.append(task)
        if closing:
            await asyncio.gather(*closing, return_exceptions=True)
        self.events.append(dict(monotonic_seconds=time.monotonic(), groups=groups,
                                closed_connections=len(closing)))
        return self._snapshot()

    def partition(self, groups):
        return self.call(self._partition(groups))

    def _snapshot(self):
        return dict(edges={'{}->{}'.format(*edge): dict(stats) for edge, stats in self.stats.items()},
                    events=list(self.events), errors=list(self.errors), active_connections=len(self.active))

    async def _get_snapshot(self):
        return self._snapshot()

    def snapshot(self):
        return self.call(self._get_snapshot())

    async def _close(self):
        for server in self.servers.values():
            server.close()
        for server in self.servers.values():
            await server.wait_closed()
        tasks = list(self.active)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def close(self):
        if self.closed:
            return
        self.closed = True
        try:
            self.call(self._close())
        finally:
            self.loop.call_soon_threadsafe(self.loop.stop)
            self.thread.join(timeout=3)
            if self.thread.is_alive():
                raise RuntimeError('Proxy thread did not stop')
            self.loop.close()
