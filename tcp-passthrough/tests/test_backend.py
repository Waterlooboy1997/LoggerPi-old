
"""Run: cd /opt/tcp-server && python3 -m unittest discover -s tests -v"""
import asyncio
import sys
import unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from shared_state import SharedState
from tcp_server import TcpDashboardServer

class BackendTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.state = SharedState([0])
        self.server = TcpDashboardServer('127.0.0.1', [0], 0, self.state, ['127.0.0.0/8'])
        await self.server.start()
        self.port = self.server.servers[0].sockets[0].getsockname()[1]
        self.command = self.server.servers[1].sockets[0].getsockname()[1]
        self.connections = []

    async def asyncTearDown(self):
        for r,w in self.connections:
            w.close()
        await self.server.stop()

    async def connect(self, source):
        pair = await asyncio.open_connection('127.0.0.1', self.port, local_addr=(source,0))
        self.connections.append(pair)
        await asyncio.sleep(.02)
        return pair

    async def test_bidirectional_and_reset(self):
        a, aw = await self.connect('127.0.0.2')
        b, bw = await self.connect('127.0.0.3')
        aw.write(b'abc\x00\xff'); await aw.drain()
        self.assertEqual(await asyncio.wait_for(b.readexactly(5), 2), b'abc\x00\xff')
        bw.write(b'xyz'); await bw.drain()
        self.assertEqual(await asyncio.wait_for(a.readexactly(3), 2), b'xyz')
        self.assertEqual(len(self.server.clients[0]), 2)
        await self.server.reset_port(0, 'Port reset')
        self.assertEqual(len(self.server.clients[0]), 0)

    async def test_duplicate_ip_evicts_old(self):
        await self.connect('127.0.0.2')
        await self.connect('127.0.0.2')
        self.assertEqual(len(self.server.clients[0]), 1)
        self.assertEqual(self.state.snapshot(0)['duplicate_ip_count'], 1)

    async def test_capacity_rejects_third(self):
        await self.connect('127.0.0.2')
        await self.connect('127.0.0.3')
        await self.connect('127.0.0.4')
        self.assertEqual(len(self.server.clients[0]), 2)
        self.assertEqual(self.state.snapshot(0)['reject_count'], 1)

    async def test_command_protocol(self):
        await self.connect('127.0.0.2')
        reader,writer = await asyncio.open_connection('127.0.0.1', self.command)
        writer.write(b'RESET=0\n'); await writer.drain()
        self.assertEqual(await asyncio.wait_for(reader.readline(), 3), b'Port 0 reset successfully.\n')
        writer.close(); await writer.wait_closed()
        self.assertEqual(len(self.server.clients[0]), 0)

    async def test_no_automatic_disconnect_task(self):
        # Clients are only dropped by a peer, keepalive, or a manual reset (dashboard or port 622).
        self.assertFalse(hasattr(self.server, 'timed_disconnect'))
        background = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        self.assertEqual(background, [])

    async def assert_closed_silently(self, reader):
        self.assertEqual(await asyncio.wait_for(reader.read(), 2), b'', 'no text may be written to a data socket')

    async def test_rejected_client_gets_no_text(self):
        await self.connect('127.0.0.2'); await self.connect('127.0.0.3')
        third, _ = await self.connect('127.0.0.4')
        await self.assert_closed_silently(third)

    async def test_duplicate_ip_eviction_gets_no_text(self):
        old, _ = await self.connect('127.0.0.2')
        await self.connect('127.0.0.2')
        await self.assert_closed_silently(old)
        self.assertTrue(any('Reason: Duplicate IP detected.' in e['message'] for e in self.state.get_events(0)))

    async def test_reset_gets_no_text(self):
        a, _ = await self.connect('127.0.0.2')
        await self.server.reset_port(0, 'Disconnected by GUI reset action.')
        await self.assert_closed_silently(a)
