import asyncio
import ipaddress
import logging
import time
from datetime import timedelta
from shared_state import ClientInfo, now
from config import READ_CHUNK_SIZE, LOG_CHUNK_EVENTS, LOG_PAYLOADS

logger = logging.getLogger('tcp_server')

class TcpDashboardServer:
    def __init__(self, host, ports, command_port, state, trusted_networks):
        self.host, self.ports, self.command_port, self.state = host, ports, command_port, state
        self.networks = [ipaddress.ip_network(x, strict=False) for x in trusted_networks]
        self.clients = {p: [] for p in ports}
        self.admission = {p: asyncio.Lock() for p in ports}
        self.servers = []
        self.loop = None
        self.stopping = asyncio.Event()
        self.tasks = set()
        self.scheduled_task = None
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
        try:
            if not writer.is_closing():
                writer.write(reason.encode() + b'\n')
                await asyncio.wait_for(writer.drain(), timeout=2)
        except (Exception, asyncio.CancelledError):
            pass
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
        try:
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
            while not reader.at_eof() and writer in self.clients[port]:
                data = await reader.read(READ_CHUNK_SIZE)
                if not data:
                    break
                read_at = time.time()
                self.state.traffic(port, writer, received=len(data))
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
            self.event(port, 'ERROR', f'Error on port {port} with {peer}: {exc}')
        finally:
            if admitted and writer in self.clients[port]:
                await self.disconnect_client(port, writer, 'Client disconnected')
            elif not admitted and not writer.is_closing():
                writer.close()

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

    async def timed_disconnect(self):
        while not self.stopping.is_set():
            current = now()
            target = current.replace(minute=30, second=0, microsecond=0)
            if target <= current:
                target += timedelta(hours=1)
            try:
                await asyncio.wait_for(self.stopping.wait(), timeout=(target-current).total_seconds())
                return
            except asyncio.TimeoutError:
                pass
            logger.info('30-minute mark reached. Disconnecting all clients.')
            for port in self.ports:
                if self.stopping.is_set():
                    return
                await self.reset_port(port, 'Disconnected at 30-minute mark by server policy.')
                self.event(port, 'INFO', '30-minute policy disconnect executed.')

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
        self.scheduled_task = asyncio.create_task(self.timed_disconnect())

    async def stop(self):
        self.stopping.set()
        for server in self.servers:
            server.close()
        for server in self.servers:
            await server.wait_closed()
        self.servers.clear()
        if self.scheduled_task:
            self.scheduled_task.cancel()
            await asyncio.gather(self.scheduled_task, return_exceptions=True)
            self.scheduled_task = None
        for port in self.ports:
            await self.reset_port(port, 'Server shutdown')
            self.state.listening(port, False)
        for task in list(self.tasks | self.command_tasks):
            task.cancel()
        if self.tasks or self.command_tasks:
            await asyncio.gather(*(self.tasks | self.command_tasks), return_exceptions=True)
        logger.info('All servers shut down.')
