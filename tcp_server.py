import asyncio
import errno
import ipaddress
import logging
import random
import socket
import time
import pakbus
from shared_state import ClientInfo, now
from config import READ_CHUNK_SIZE, LOG_CHUNK_EVENTS, LOG_PAYLOADS, KEEPALIVE, KEEPALIVE_IDLE, KEEPALIVE_INTERVAL, KEEPALIVE_COUNT
from tap import StreamDecoder, decode_frame

logger = logging.getLogger('tcp_server')


def enable_keepalive(sock, idle, interval, count):
    """Turn on TCP keepalive so a peer that vanished without closing is detected."""
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    for name, value in (('TCP_KEEPIDLE', idle), ('TCP_KEEPINTVL', interval), ('TCP_KEEPCNT', count)):
        if hasattr(socket, name):
            sock.setsockopt(socket.IPPROTO_TCP, getattr(socket, name), value)
    if hasattr(socket, 'TCP_USER_TIMEOUT'):
        # Also bounds how long forwarded data may sit unacknowledged, which keepalive alone does not.
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_USER_TIMEOUT, (idle + interval * count) * 1000)


def is_dead_peer(exc):
    """True for the error the kernel reports when keepalive (or the user timeout) gives up."""
    return isinstance(exc, OSError) and exc.errno == errno.ETIMEDOUT


class Probe:
    """PakBus check state for one client."""

    def __init__(self, expected):
        self.expected = expected             # logger PakBus address from sites.json
        self.decoder = StreamDecoder()
        self.last_rx = self.last_check = time.monotonic()
        self.confirmed = False               # answered as the expected logger at least once
        self.asker = False                   # sent frames from another node (LoggerNet, scheduler)
        self.waiting = None                  # (kind, future) while a reply is expected
        self.tran = random.randint(1, 255)
        self.task = None


class TcpDashboardServer:
    def __init__(self, host, ports, command_port, state, trusted_networks,
                 keepalive=(KEEPALIVE_IDLE, KEEPALIVE_INTERVAL, KEEPALIVE_COUNT) if KEEPALIVE else None,
                 expected_pakbus=None, probe_node=None, probe_first=15, probe_interval=300,
                 probe_retry=30, probe_timeout=4, probe_attempts=3):
        self.host, self.ports, self.command_port, self.state = host, ports, command_port, state
        self.keepalive = keepalive
        self.expected_pakbus = expected_pakbus or (lambda port: None)
        self.probe_node = probe_node         # None disables the PakBus check
        self.probe_first, self.probe_interval, self.probe_retry = probe_first, probe_interval, probe_retry
        self.probe_timeout, self.probe_attempts = probe_timeout, probe_attempts
        self.probes = {}                     # writer -> Probe
        self.networks = [ipaddress.ip_network(x, strict=False) for x in trusted_networks]
        self.clients = {p: [] for p in ports}
        self.admission = {p: asyncio.Lock() for p in ports}
        self.servers = []
        self.loop = None
        self.stopping = asyncio.Event()
        self.tasks = set()
        self.command_tasks = set()
        self.closing = set()
        self.forward_locks = {p: asyncio.Lock() for p in ports}

    @staticmethod
    def peer(writer):
        address = writer.get_extra_info('peername')
        return f'{address[0]}:{address[1]}' if isinstance(address, tuple) and len(address) >= 2 else 'unknown'

    @staticmethod
    def ip(writer):
        address = writer.get_extra_info('peername')
        return str(address[0]) if isinstance(address, tuple) and address else 'unknown'

    def event(self, port, level, message):
        self.state.event(port, level, message)
        getattr(logger, level.lower(), logger.info)(message)

    def _track(self, coroutine, group=None):
        task = asyncio.create_task(coroutine)
        target = self.tasks if group is None else group
        target.add(task)
        task.add_done_callback(target.discard)
        return task

    async def disconnect_client(self, port, writer, reason):
        if writer in self.closing:
            return
        self.closing.add(writer)
        peer = self.peer(writer)
        # Remove first so concurrent forwarding cannot target this writer.
        self.clients[port] = [c for c in self.clients[port] if c is not writer]
        self.state.remove_client(port, writer)
        self.probes.pop(writer, None)
        # The reason goes to the event log only: a data connection carries raw PakBus and
        # must never receive bytes that did not come from its peer.
        try:
            writer.close()
            await asyncio.wait_for(writer.wait_closed(), timeout=2)
        except (Exception, asyncio.CancelledError):
            pass
        finally:
            self.closing.discard(writer)
            self.event(port, 'INFO', f'Disconnected {peer} on port {port}. Reason: {reason}')

    async def handle_client(self, reader, writer, port):
        peer, ip = self.peer(writer), self.ip(writer)
        admitted = False
        reaped = False
        try:
            self._enable_keepalive(port, writer, peer)
            async with self.admission[port]:
                duplicates = [c for c in self.clients[port] if self.ip(c) == ip]
                if duplicates:
                    self.state.increment(port, 'duplicate_ip_count')
                    self.event(port, 'WARNING', f'Duplicate IP detected: {ip} on port {port}. Disconnecting existing clients from this IP.')
                    for old in duplicates:
                        await self.disconnect_client(port, old, 'Duplicate IP detected.')
                if len(self.clients[port]) >= 2:
                    self.state.increment(port, 'reject_count')
                    self.event(port, 'WARNING', f'Connection limit reached on port {port}. Rejecting {peer}')
                    await self.disconnect_client(port, writer, 'Connection limit reached (max 2).')
                    return
                self.clients[port].append(writer)
                timestamp = now()
                self.state.add_client(port, ClientInfo(writer, peer, ip, writer.get_extra_info('peername')[1], timestamp, timestamp))
                admitted = True
                self.event(port, 'INFO', f'Client connected on port {port}: {peer} (total {len(self.clients[port])})')
                self._start_probe(port, writer, peer)
            while not reader.at_eof() and writer in self.clients[port]:
                data = await reader.read(READ_CHUNK_SIZE)
                if not data:
                    break
                read_at = time.time()
                self.state.traffic(port, writer, received=len(data))
                if writer in self.probes:
                    self._probe_observe(port, writer, peer, data)
                if LOG_CHUNK_EVENTS:
                    self.event(port, 'INFO', f'Data received on port {port} from {peer} -> {data.decode(errors="ignore").strip()}' if LOG_PAYLOADS else f'Received {len(data)} bytes on port {port} from {peer}')
                forwarded = False
                recipients = []
                async with self.forward_locks[port]:
                    for other in list(self.clients[port]):
                        if writer not in self.clients[port] or other is writer or other not in self.clients[port] or other in self.closing:
                            continue
                        try:
                            other.write(data)
                            await asyncio.wait_for(other.drain(), timeout=10)
                            self.state.traffic(port, other, sent=len(data))
                            forwarded = True
                            recipients.append(self.peer(other))
                            if LOG_CHUNK_EVENTS:
                                self.event(port, 'INFO', f'Forwarded {len(data)} bytes from {peer} to {self.peer(other)} on port {port}')
                        except Exception as exc:
                            self.event(port, 'ERROR', f'Forwarding error on port {port} to {self.peer(other)}: {exc}')
                            await self.disconnect_client(port, other, 'Forwarding error')
                try:
                    # Read-only observation for the dashboard's traffic monitor.
                    self.state.tap.chunk(port, writer, peer, data, recipients, read_at)
                except Exception:
                    pass
                if not forwarded and LOG_CHUNK_EVENTS:
                    self.event(port, 'INFO', f'No other client connected on port {port}. Data not forwarded.')
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if is_dead_peer(exc):
                reaped = True
                if self.keepalive:
                    idle, interval, count = self.keepalive
                    self.event(port, 'WARNING', f'Dead connection reaped on port {port}: {peer} stopped answering TCP keepalive (~{idle + interval * count}s without a reply)')
                else:
                    self.event(port, 'WARNING', f'Dead connection on port {port}: {peer} timed out')
            else:
                self.event(port, 'ERROR', f'Error on port {port} with {peer}: {exc}')
        finally:
            probe = self.probes.get(writer)
            if probe and probe.task and probe.task is not asyncio.current_task():
                probe.task.cancel()
            if admitted and writer in self.clients[port]:
                await self.disconnect_client(port, writer, ('Dead connection (keepalive timeout)' if self.keepalive else 'Dead connection (timed out)') if reaped else 'Client disconnected')
            elif not admitted and not writer.is_closing():
                writer.close()

    def _enable_keepalive(self, port, writer, peer):
        if not self.keepalive:
            return
        try:
            enable_keepalive(writer.get_extra_info('socket'), *self.keepalive)
        except Exception as exc:              # never refuse a client over this
            self.event(port, 'WARNING', f'Could not enable TCP keepalive for {peer} on port {port}: {exc}')

    # -- PakBus logger check ------------------------------------------------------------------
    # The Pi opens a session exactly as LoggerNet does (ring, Hello as node 4094, finished) with
    # a client that is alone and silent on its port. A reply from the port's expected PakBus
    # address proves that client is the logger and that it is alive. It never runs while
    # another client is connected, and it holds the port's admission lock while waiting for a
    # reply so a client arriving mid-check cannot receive the logger's answer.

    def _start_probe(self, port, writer, peer):
        if self.probe_node is None:
            return
        try:
            expected = self.expected_pakbus(port)
        except Exception:
            expected = None
        if expected is None:
            return
        probe = self.probes[writer] = Probe(expected)
        probe.task = self._track(self._probe_loop(port, writer, peer, probe))

    def _probe_observe(self, port, writer, peer, data):
        """Look at a chunk the client sent (read-only; forwarding is unchanged)."""
        probe = self.probes.get(writer)
        try:
            probe.last_rx = time.monotonic()
            for kind, body in probe.decoder.feed(data):
                if kind != 'frame':
                    continue
                info = decode_frame(body)
                if not info.get('signature_ok'):
                    continue
                source = info.get('src_node', info.get('src_phys'))
                if source != probe.expected:
                    if not probe.asker and source is not None:
                        probe.asker = True
                        self.state.set_role(port, writer, 'asker', f'PakBus node {source}')
                    continue
                if probe.waiting:
                    kind_wanted, future = probe.waiting
                    to_us = info.get('dst_node', info.get('dst_phys')) == self.probe_node
                    if to_us and not future.done() and (
                            (kind_wanted == 'ready' and 'msg_type' not in info and info.get('link_state') == 'ready') or
                            (kind_wanted == 'hello' and info.get('msg_type') == pakbus.HELLO_RESPONSE and info.get('tran') == probe.tran) or
                            (kind_wanted == 'offline' and 'msg_type' not in info)):
                        future.set_result(True)
                self._logger_answered(port, writer, peer, probe)
        except Exception:                    # the check must never disturb forwarding
            pass

    def _logger_answered(self, port, writer, peer, probe):
        if not probe.confirmed:
            probe.confirmed = True
            self.event(port, 'INFO', f'{peer} on port {port} answered as PakBus node {probe.expected}: logger confirmed')
        self.state.set_role(port, writer, 'logger', f'PakBus node {probe.expected}', checked_at=now())

    async def _probe_loop(self, port, writer, peer, probe):
        failures = 0
        try:
            while writer in self.clients[port] and not probe.asker and not self.stopping.is_set():
                wait = self.probe_retry if failures else self.probe_interval if probe.confirmed else self.probe_first
                delay = max(probe.last_rx, probe.last_check) + wait - time.monotonic()
                if delay > 0:
                    await asyncio.sleep(delay)
                    continue
                result = await self._probe_once(port, writer, peer, probe)
                probe.last_check = time.monotonic()
                if result is None:           # not alone, or became an asker: nothing to judge
                    continue
                if result or time.monotonic() - probe.last_rx < wait:
                    failures = 0
                    continue
                failures += 1
                if failures < self.probe_attempts:
                    continue
                if probe.confirmed:
                    self.event(port, 'WARNING', f'Logger {peer} on port {port} did not answer {failures} PakBus checks; disconnecting dead connection')
                    await self.disconnect_client(port, writer, 'Logger stopped answering PakBus Hello')
                else:
                    self.event(port, 'WARNING', f'{peer} on port {port} did not answer a PakBus Hello to node {probe.expected}; not checking it again')
                    self.state.set_role(port, writer, None, f'did not answer PakBus node {probe.expected}')
                return
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.event(port, 'ERROR', f'PakBus check error on port {port} with {peer}: {exc}')

    async def _probe_once(self, port, writer, peer, probe):
        """One ring/Hello/finished exchange. True = answered, False = no answer, None = skipped."""
        async with self.admission[port]:
            if writer not in self.clients[port] or len(self.clients[port]) != 1 or probe.asker:
                return None
            node, dst = self.probe_node, probe.expected
            if not await self._probe_exchange(port, writer, peer, probe, 'ready', pakbus.ring(dst, node)):
                return False
            probe.tran = probe.tran % 255 + 1
            if not await self._probe_exchange(port, writer, peer, probe, 'hello', pakbus.hello_command(dst, node, probe.tran)):
                return False
            await self._probe_exchange(port, writer, peer, probe, 'offline', pakbus.finished(dst, node))
            return True

    async def _probe_exchange(self, port, writer, peer, probe, kind, frame):
        future = asyncio.get_running_loop().create_future()
        probe.waiting = (kind, future)
        try:
            writer.write(frame)
            await asyncio.wait_for(writer.drain(), timeout=self.probe_timeout)
            try:
                self.state.tap.chunk(port, self, 'Pi (PakBus check)', frame, [peer])
            except Exception:
                pass
            await asyncio.wait_for(future, timeout=self.probe_timeout)
            return True
        except (asyncio.TimeoutError, OSError):
            return False
        finally:
            probe.waiting = None

    async def handle_command(self, reader, writer):
        peer = self.peer(writer)
        try:
            try:
                allowed = any(ipaddress.ip_address(self.ip(writer)) in net for net in self.networks)
            except ValueError:
                allowed = False
            if not allowed:
                logger.warning('Rejected command connection from %s', peer)
                return
            data = await asyncio.wait_for(reader.read(1024), timeout=10)
            if not data:
                return
            message = data.decode(errors='ignore').strip()
            if message.startswith('RESET='):
                try:
                    port = int(message.split('=', 1)[1])
                except ValueError:
                    response = b'Invalid port number.\n'
                else:
                    if port in self.clients:
                        self.event(port, 'INFO', f'Command server request from {peer} to reset port {port}')
                        await self.reset_port(port, 'Disconnected by RESET command.')
                        response = f'Port {port} reset successfully.\n'.encode()
                    else:
                        response = f'Invalid port: {port}\n'.encode()
            else:
                response = b'Invalid command. Use RESET=[port]\n'
            writer.write(response)
            await asyncio.wait_for(writer.drain(), timeout=3)
        except asyncio.TimeoutError:
            logger.warning('Command connection timeout: %s', peer)
        except Exception:
            logger.exception('Command connection error: %s', peer)
        finally:
            writer.close()
            try:
                await asyncio.wait_for(writer.wait_closed(), timeout=2)
            except Exception:
                pass

    async def reset_port(self, port, reason='Port reset', record_reset=True):
        if port not in self.clients:
            raise ValueError('Invalid port')
        async with self.admission[port]:
            for client in list(self.clients[port]):
                await self.disconnect_client(port, client, reason)
            if record_reset:
                self.event(port, 'INFO', f'Port {port} reset complete')

    async def start(self):
        self.loop = asyncio.get_running_loop()
        try:
            for port in self.ports:
                server = await asyncio.start_server(lambda r, w, p=port: self._track(self.handle_client(r, w, p)), self.host, port)
                self.servers.append(server)
                self.state.listening(port, True)
                self.event(port, 'INFO', f'TCP server is listening on {self.host}:{port}')
            command = await asyncio.start_server(lambda r, w: self._track(self.handle_command(r, w), self.command_tasks), self.host, self.command_port)
            self.servers.append(command)
            logger.info('Command server is listening on %s:%s', self.host, self.command_port)
        except Exception:
            await self.stop()
            raise

    async def stop(self):
        self.stopping.set()
        for server in self.servers:
            server.close()
        for server in self.servers:
            await server.wait_closed()
        self.servers.clear()
        for port in self.ports:
            await self.reset_port(port, 'Server shutdown')
            self.state.listening(port, False)
        for task in list(self.tasks | self.command_tasks):
            task.cancel()
        if self.tasks or self.command_tasks:
            await asyncio.gather(*(self.tasks | self.command_tasks), return_exceptions=True)
        logger.info('All servers shut down.')
