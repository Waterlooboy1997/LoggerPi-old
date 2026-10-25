"""TCP keepalive on accepted data sockets and logging of reaped (dead) peers."""
import asyncio
import errno
import socket
import sys
import unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config import parse_positive_int
from shared_state import SharedState
from tcp_server import TcpDashboardServer, enable_keepalive, is_dead_peer


class KeepaliveCase(unittest.IsolatedAsyncioTestCase):
    keepalive = (20, 5, 3)

    async def asyncSetUp(self):
        self.state = SharedState([0])
        self.server = TcpDashboardServer('127.0.0.1', [0], 0, self.state, ['127.0.0.0/8'], keepalive=self.keepalive)
        await self.server.start()
        self.port = self.server.servers[0].sockets[0].getsockname()[1]
        self.conns = []

    async def asyncTearDown(self):
        for r, w in self.conns:
            w.close()
        await self.server.stop()

    async def connect(self, source):
        pair = await asyncio.open_connection('127.0.0.1', self.port, local_addr=(source, 0))
        self.conns.append(pair)
        await asyncio.sleep(.03)
        return pair

    def messages(self):
        return [e['message'] for e in self.state.get_events(0, 0, 500)]


class KeepaliveTests(KeepaliveCase):
    @unittest.skipUnless(hasattr(socket, 'TCP_KEEPIDLE'), 'Linux keepalive options')
    async def test_accepted_sockets_have_keepalive(self):
        await self.connect('127.0.0.2')
        sock = self.server.clients[0][0].get_extra_info('socket')
        self.assertEqual(sock.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE), 1)
        self.assertEqual(sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE), 20)
        self.assertEqual(sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL), 5)
        self.assertEqual(sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT), 3)
        if hasattr(socket, 'TCP_USER_TIMEOUT'):
            self.assertEqual(sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_USER_TIMEOUT), 35000)

    async def test_dead_peer_is_reaped_and_logged(self):
        class DeadReader:
            def at_eof(self):
                return False

            async def read(self, n):
                raise TimeoutError(errno.ETIMEDOUT, 'Connection timed out')
        # A real writer stands in for the modem connection that went silent.
        _, writer = await self.connect('127.0.0.2')
        await self.server.handle_client(DeadReader(), writer, 0)
        self.assertTrue(any('Dead connection reaped' in m and '~35s' in m for m in self.messages()))
        self.assertTrue(any('Reason: Dead connection (keepalive timeout)' in m for m in self.messages()))

    def test_dead_peer_classification(self):
        self.assertTrue(is_dead_peer(TimeoutError(errno.ETIMEDOUT, 'timed out')))
        self.assertFalse(is_dead_peer(ConnectionResetError(errno.ECONNRESET, 'reset')))
        self.assertFalse(is_dead_peer(ValueError('x')))

    def test_parse_positive_int(self):
        self.assertEqual(parse_positive_int('KEEPALIVE_IDLE', None, 60), 60)
        self.assertEqual(parse_positive_int('KEEPALIVE_IDLE', '30', 60), 30)
        for bad in ('0', '-5', 'soon'):
            with self.assertRaises(ValueError):
                parse_positive_int('KEEPALIVE_IDLE', bad, 60)

    def test_enable_keepalive_on_plain_socket(self):
        with socket.socket() as s:
            enable_keepalive(s, 30, 10, 3)
            self.assertEqual(s.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE), 1)


class KeepaliveFailureTests(KeepaliveCase):
    keepalive = (-1, -1, -1)      # rejected by the kernel

    async def test_keepalive_failure_does_not_refuse_client(self):
        a, aw = await self.connect('127.0.0.2'); b, _ = await self.connect('127.0.0.3')
        aw.write(b'ok'); await aw.drain()
        self.assertEqual(await asyncio.wait_for(b.readexactly(2), 2), b'ok')
        self.assertTrue(any('Could not enable TCP keepalive' in m for m in self.messages()))


if __name__ == '__main__':
    unittest.main()
