"""PakBus logger check: the Pi opens a LoggerNet-style session with a silent client and treats
a reply from the port's PakBus address as proof that the client is the logger and alive."""
import asyncio
import socket
import sys
import unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import pakbus
from config import parse_switch
from shared_state import SharedState
from tap import StreamDecoder, decode_frame
from tcp_server import TcpDashboardServer

LOGGER = 353
PI = 4094
# The logger's own link-verification Hello from the real port 353 capture (node 353 -> 4094).
LOGGER_OWN_HELLO = bytes.fromhex('bdaffe71610ffe0161097900 01ffff202dbd'.replace(' ', ''))
SCHEDULER_HELLO = bytes.fromhex('bd9134580201340802090100020708154abd')   # node 2050 -> 308


class PacketTests(unittest.TestCase):
    def test_packets_match_loggernet_capture(self):
        # Byte-for-byte what LoggerNet sent to the port 358 and 353 loggers.
        self.assertEqual(pakbus.ring(358, 4094).hex(), 'bd91660ffe9dcfbd')
        self.assertEqual(pakbus.hello_command(358, 4094, 184).hex(), 'bda1666ffe01660ffe09b8010200 3c8adbbd'.replace(' ', ''))
        self.assertEqual(pakbus.finished(358, 4094).hex(), 'bdb1663ffe01660ffe2dfebd')
        self.assertEqual(pakbus.ring(353, 4094).hex(), 'bd91610ffed7e7bd')
        self.assertEqual(pakbus.hello_command(353, 4094, 108).hex(), 'bda1616ffe01610ffe096c0102003c3c8ebd')
        self.assertEqual(pakbus.finished(353, 4094).hex(), 'bdb1613ffe01610ffe4057bd')

    def test_sync_bytes_inside_a_packet_are_quoted(self):
        frame = pakbus.hello_command(353, 4094, 0xBD)
        self.assertNotIn(b'\xbd', frame[1:-1])
        self.assertIn(b'\xbc\xdd', frame)
        info = [decode_frame(v) for k, v in StreamDecoder().feed(frame) if k == 'frame'][0]
        self.assertTrue(info['signature_ok'])
        self.assertEqual((info['msg_type'], info['tran'], info['src_node'], info['dst_node']), (0x09, 0xBD, 4094, 353))

    def test_switches(self):
        self.assertTrue(parse_switch('PAKBUS_PROBE', None, 'on'))
        self.assertFalse(parse_switch('KEEPALIVE', None, 'off'))
        self.assertTrue(parse_switch('KEEPALIVE', 'ON', 'off'))
        with self.assertRaises(ValueError):
            parse_switch('KEEPALIVE', 'maybe', 'off')


class FakeLogger:
    """Answers ring, Hello and finished the way the CR350 in the captures does."""

    def __init__(self, reader, writer, node=LOGGER):
        self.reader, self.writer, self.node = reader, writer, node
        self.answer = True
        self.delay = 0
        self.received = []                     # decoded frames the Pi sent
        self.task = asyncio.create_task(self.run())

    async def run(self):
        decoder = StreamDecoder()
        while data := await self.reader.read(4096):
            for kind, body in decoder.feed(data):
                if kind != 'frame':
                    continue
                info = decode_frame(body)
                self.received.append(info)
                if not self.answer:
                    continue
                await asyncio.sleep(self.delay)
                src = info.get('src_node', info.get('src_phys'))
                if info.get('link_state') == 'ring' and 'msg_type' not in info:
                    reply = pakbus.frame(pakbus.link_header(pakbus.LINK_READY, src, self.node))
                elif info.get('msg_type') == pakbus.HELLO_COMMAND:
                    reply = pakbus.frame(pakbus.header(pakbus.LINK_READY, src, self.node, priority=2)
                                         + bytes([pakbus.HELLO_RESPONSE, info['tran'], 0, 2, 0xFF, 0xFF]))
                elif info.get('link_state') == 'finished':
                    reply = pakbus.frame(pakbus.link_header(pakbus.LINK_OFFLINE, src, self.node))
                else:
                    continue
                self.writer.write(reply)
                await self.writer.drain()

    def kinds(self):
        return [f.get('msg_type', f.get('link_state')) for f in self.received]


class ProbeCase(unittest.IsolatedAsyncioTestCase):
    options = {'pakbus_address': lambda port: LOGGER, 'probe_node': PI, 'probe_first': .1,
               'probe_interval': .4, 'probe_retry': .1, 'probe_timeout': .2, 'probe_attempts': 3}

    async def asyncSetUp(self):
        self.state = SharedState([0])
        self.server = TcpDashboardServer('127.0.0.1', [0], 0, self.state, ['127.0.0.0/8'], **self.options)
        await self.server.start()
        self.port = self.server.servers[0].sockets[0].getsockname()[1]
        self.conns, self.fakes = [], []

    async def asyncTearDown(self):
        for r, w in self.conns:
            w.close()
        await self.server.stop()
        for fake in self.fakes:
            fake.task.cancel()

    async def connect(self, source):
        pair = await asyncio.open_connection('127.0.0.1', self.port, local_addr=(source, 0))
        self.conns.append(pair)
        await asyncio.sleep(.03)
        return pair

    async def logger(self, source='127.0.0.2'):
        fake = FakeLogger(*await self.connect(source))
        self.fakes.append(fake)
        return fake

    async def assert_receives_nothing(self, reader, seconds=.6):
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(reader.read(1), seconds)

    def messages(self):
        return [e['message'] for e in self.state.get_events(0, 0, 500)]

    def clients(self):
        return {c['ip']: c for c in self.state.snapshot(0)['clients']}


class ProbeTests(ProbeCase):
    async def test_silent_logger_is_confirmed_like_loggernet_would(self):
        fake = await self.logger()
        await asyncio.sleep(.35)
        self.assertEqual(fake.kinds()[:3], ['ring', 0x09, 'finished'])
        self.assertTrue(all(f.get('src_node', f.get('src_phys')) == PI for f in fake.received))
        client = self.clients()['127.0.0.2']
        self.assertEqual((client['role'], client['role_detail']), ('logger', 'PakBus node 353'))
        self.assertIsNotNone(client['checked_at'])
        self.assertTrue(any('answered the PakBus check (node 353): logger confirmed' in m for m in self.messages()))

    async def test_logger_that_stops_answering_is_disconnected(self):
        fake = await self.logger()
        await asyncio.sleep(.35)
        fake.answer = False
        await asyncio.wait_for(fake.task, 5)          # ends when the Pi closes the socket
        self.assertEqual(len(self.server.clients[0]), 0)
        self.assertTrue(any('did not answer 3 PakBus checks' in m for m in self.messages()))
        self.assertTrue(any('Reason: Logger stopped answering PakBus Hello' in m for m in self.messages()))

    async def test_unconfirmed_client_that_never_answers_is_left_alone(self):
        fake = await self.logger()
        fake.answer = False
        await asyncio.sleep(1.5)
        self.assertEqual(len(self.server.clients[0]), 1)
        self.assertEqual(fake.kinds(), ['ring', 'ring', 'ring'])      # three tries, then it stops
        self.assertTrue(any('not checking it again' in m for m in self.messages()))

    async def test_nothing_is_sent_while_another_client_is_connected(self):
        a, _ = await self.connect('127.0.0.2')
        b, _ = await self.connect('127.0.0.3')
        await self.assert_receives_nothing(a)
        await self.assert_receives_nothing(b, .1)

    async def test_answer_from_any_node_counts(self):
        # No check of the logger's ID: a different node answering the Hello is still a logger.
        fake = FakeLogger(*await self.connect('127.0.0.2'), node=999)
        self.fakes.append(fake)
        await asyncio.sleep(.35)
        client = self.clients()['127.0.0.2']
        self.assertEqual((client['role'], client['role_detail']), ('logger', 'PakBus node 999'))

    async def test_client_that_keeps_sending_is_not_checked(self):
        r, w = await self.connect('127.0.0.2')
        for _ in range(12):                           # e.g. LoggerNet or the scheduler mid-session
            w.write(SCHEDULER_HELLO); await w.drain()
            await asyncio.sleep(.05)
        await self.assert_receives_nothing(r, .05)

    async def test_client_arriving_mid_check_never_sees_the_logger_reply(self):
        fake = await self.logger()
        fake.delay = .1
        while not fake.received:
            await asyncio.sleep(.01)
        pc, pcw = await self.connect('127.0.0.3')     # arrives while the Pi waits for "ready"
        await asyncio.sleep(.5)
        self.assertEqual(len(self.server.clients[0]), 2)
        fake.writer.write(b'data'); await fake.writer.drain()
        self.assertEqual(await asyncio.wait_for(pc.readexactly(4), 2), b'data')   # only real traffic


class OwnTrafficTests(ProbeCase):
    options = {**ProbeCase.options, 'probe_first': 30}

    async def test_only_an_answer_to_the_check_marks_a_logger(self):
        r, w = await self.connect('127.0.0.2')
        w.write(LOGGER_OWN_HELLO); await w.drain(); await asyncio.sleep(.05)
        self.assertIsNone(self.clients()['127.0.0.2']['role'])
        await self.assert_receives_nothing(r, .2)


class ProbeOffTests(ProbeCase):
    options = {'pakbus_address': lambda port: LOGGER}

    async def test_off_sends_nothing(self):
        r, _ = await self.connect('127.0.0.2')
        await self.assert_receives_nothing(r)
        self.assertIsNone(self.clients()['127.0.0.2']['role'])


class NoAddressTests(ProbeCase):
    options = {**ProbeCase.options, 'pakbus_address': lambda port: None}

    async def test_port_without_pakbus_address_is_not_checked(self):
        r, _ = await self.connect('127.0.0.2')
        await self.assert_receives_nothing(r)


class KeepaliveDefaultTests(ProbeCase):
    async def test_tcp_keepalive_is_off_by_default(self):
        await self.connect('127.0.0.2')
        sock = self.server.clients[0][0].get_extra_info('socket')
        self.assertEqual(sock.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE), 0)


if __name__ == '__main__':
    unittest.main()
