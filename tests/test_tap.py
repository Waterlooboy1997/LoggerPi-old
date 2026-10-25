"""Traffic monitor tests. Packet fixtures were generated with PyCampbellCR1000's own
PakBus class (src node 2050 -> dst node 308, logger 308 replying), so they are genuine
on-the-wire frames: sync 0xBD, 0xBC quoting and a valid signature."""
import asyncio
import re
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from shared_state import SharedState
from tap import StreamDecoder, TrafficTap, decode_frame
from tcp_server import TcpDashboardServer
from web_dashboard import create_app

ATTENTION = bytes.fromhex('bdbdbdbdbdbd')
HELLO_CMD = bytes.fromhex('bd9134580201340802090100020708154abd')
HELLO_RESP = bytes.fromhex('bda802913408020134890700020708afaabd')
GETSETTINGS = bytes.fromhex('bda1349802013408020f02e955bd')
CLOCK = bytes.fromhex('bda134980211340802170300000000000000000000218abd')
BYE = bytes.fromhex('bdb1341802013408020d00dc67bd')


def frames(data):
    return [decode_frame(v) for k, v in StreamDecoder().feed(data) if k == 'frame']


class DecoderTests(unittest.TestCase):
    def test_hello_command(self):
        f = frames(HELLO_CMD)[0]
        self.assertTrue(f['signature_ok'])
        self.assertEqual((f['src_node'], f['dst_node'], f['proto'], f['msg_type'], f['tran']), (2050, 308, 0, 0x09, 1))
        self.assertEqual(f['link_state'], 'ring')
        self.assertIn('Hello command', f['summary'])

    def test_hello_response_direction(self):
        f = frames(HELLO_RESP)[0]
        self.assertTrue(f['signature_ok'])
        self.assertEqual((f['src_node'], f['dst_node'], f['msg_type'], f['tran']), (308, 2050, 0x89, 7))
        self.assertIn('Hello response', f['summary'])

    def test_other_messages(self):
        self.assertIn('Get settings command', frames(GETSETTINGS)[0]['summary'])
        self.assertIn('Clock command', frames(CLOCK)[0]['summary'])
        self.assertEqual(frames(CLOCK)[0]['proto'], 1)
        self.assertIn('Bye', frames(BYE)[0]['summary'])
        self.assertEqual(frames(BYE)[0]['link_state'], 'finished')

    def test_corruption_flags_bad_signature(self):
        bad = bytearray(HELLO_CMD); bad[10] ^= 0x01
        self.assertFalse(frames(bytes(bad))[0]['signature_ok'])
        self.assertIn('BAD SIGNATURE', frames(bytes(bad))[0]['summary'])

    def test_stream_split_across_chunks_and_attention(self):
        d = StreamDecoder(); stream = ATTENTION + HELLO_CMD + HELLO_RESP
        events = []
        for i in range(0, len(stream), 3):
            events += d.feed(stream[i:i + 3])
        kinds = [k for k, _ in events]
        self.assertEqual(kinds.count('frame'), 2)
        self.assertEqual([v for k, v in events if k == 'sync'], [7])   # 6 attention + frame-opening 0xBD

    def test_back_to_back_frames_do_not_report_sync(self):
        events = StreamDecoder().feed(HELLO_CMD + GETSETTINGS)
        self.assertEqual([k for k, _ in events], ['frame', 'frame'])

    def test_non_pakbus_is_not_misreported(self):
        f = frames(b'\xbdhi\xbd')[0]
        self.assertIn('not a PakBus', f['summary'])


class RelayTapTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.state = SharedState([0])
        self.server = TcpDashboardServer('127.0.0.1', [0], 0, self.state, ['127.0.0.0/8'])
        await self.server.start()
        self.port = self.server.servers[0].sockets[0].getsockname()[1]
        self.conns = []

    async def asyncTearDown(self):
        for r, w in self.conns:
            w.close()
        await self.server.stop()

    async def connect(self, source):
        pair = await asyncio.open_connection('127.0.0.1', self.port, local_addr=(source, 0))
        self.conns.append(pair); await asyncio.sleep(.03); return pair

    async def test_capture_is_off_by_default_and_never_alters_bytes(self):
        a, aw = await self.connect('127.0.0.2'); b, bw = await self.connect('127.0.0.3')
        aw.write(HELLO_CMD); await aw.drain()
        self.assertEqual(await asyncio.wait_for(b.readexactly(len(HELLO_CMD)), 2), HELLO_CMD)
        self.assertEqual(self.state.tap.read(0)['entries'], [])           # nothing stored while off

        self.state.tap.start(0, 5)
        aw.write(ATTENTION + HELLO_CMD); await aw.drain()
        self.assertEqual(await asyncio.wait_for(b.readexactly(6 + len(HELLO_CMD)), 2), ATTENTION + HELLO_CMD)
        bw.write(HELLO_RESP); await bw.drain()
        self.assertEqual(await asyncio.wait_for(a.readexactly(len(HELLO_RESP)), 2), HELLO_RESP)
        await asyncio.sleep(.05)
        data = [e for e in self.state.tap.read(0)['entries'] if e['kind'] == 'data']
        self.assertEqual(len(data), 2)
        self.assertTrue(data[0]['forwarded'])
        self.assertTrue(data[0]['from'].startswith('127.0.0.2:'))
        self.assertTrue(data[0]['to'][0].startswith('127.0.0.3:'))
        self.assertTrue(any('Hello command' in f['summary'] for f in data[0]['frames']))
        self.assertTrue(any('Hello response' in f['summary'] for f in data[1]['frames']))
        self.assertEqual(data[1]['hex'].replace(' ', ''), HELLO_RESP.hex())

    async def test_unforwarded_traffic_and_events_are_visible(self):
        self.state.tap.start(0, 5)
        a, aw = await self.connect('127.0.0.2')
        aw.write(HELLO_CMD); await aw.drain(); await asyncio.sleep(.05)
        entries = self.state.tap.read(0)['entries']
        self.assertTrue(any(e['kind'] == 'event' and 'Client connected' in e['message'] for e in entries))
        sent = [e for e in entries if e['kind'] == 'data'][0]
        self.assertFalse(sent['forwarded'])
        self.assertIn('NOT FORWARDED', self.state.tap.as_text(0))

    async def test_tap_failure_cannot_break_forwarding(self):
        self.state.tap.start(0, 5)
        self.state.tap._append = lambda *a, **k: 1 / 0
        a, aw = await self.connect('127.0.0.2'); b, bw = await self.connect('127.0.0.3')
        aw.write(b'payload'); await aw.drain()
        self.assertEqual(await asyncio.wait_for(b.readexactly(7), 2), b'payload')


class TapUnitTests(unittest.TestCase):
    def test_expiry_stop_and_limits(self):
        tap = TrafficTap([1, 2])
        tap.start(1, 1); self.assertTrue(tap.active(1))
        tap._until[1] = 0; self.assertFalse(tap.active(1))
        self.assertFalse(tap.read(1)['active'])
        for p in range(10, 18): tap._entries[p] = []; tap.start(p, 1)
        with self.assertRaises(ValueError): tap.start(2, 1)

    def test_truncation_keeps_true_length(self):
        tap = TrafficTap([1]); tap.start(1)
        tap.chunk(1, object(), 'a:1', b'x' * 2000, [])
        e = tap.read(1)['entries'][0]
        self.assertEqual(e['length'], 2000); self.assertTrue(e['truncated'])
        self.assertEqual(len(e['hex'].split()), 512)


class ApiTests(unittest.TestCase):
    def setUp(self):
        import tempfile
        from site_config import SiteConfig
        self.dir = tempfile.TemporaryDirectory(); self.addCleanup(self.dir.cleanup)
        self.state = SharedState([300, 305])
        sites = SiteConfig(Path(self.dir.name) / 'sites.json')
        self.client = create_app(SimpleNamespace(clients=dict.fromkeys([300, 305])), self.state,
                                 SimpleNamespace(snapshot=lambda: {}), ['127.0.0.0/8'], sites).test_client()
        self.token = re.search(r'name="control-token" content="([^"]+)"', self.client.get('/').text)[1]

    def test_start_read_download_stop_and_security(self):
        h = {'X-Control-Token': self.token}
        self.assertEqual(self.client.post('/api/ports/300/tap/start').status_code, 403)
        self.assertEqual(self.client.post('/api/ports/999/tap/start', headers=h).status_code, 404)
        self.assertEqual(self.client.post('/api/ports/300/tap/bogus', headers=h).status_code, 404)
        self.assertFalse(self.client.get('/api/ports/300/tap').json['active'])
        self.assertEqual(self.client.post('/api/ports/300/tap/start', headers=h, json={'minutes': 5}).status_code, 200)
        self.state.tap.chunk(300, object(), '1.2.3.4:5', HELLO_CMD, ['6.7.8.9:1'])
        body = self.client.get('/api/ports/300/tap').json
        self.assertTrue(body['active'])
        self.assertEqual(sum(e['kind'] == 'data' for e in body['entries']), 1)
        text = self.client.get('/api/ports/300/tap.txt')
        self.assertIn('Hello command', text.get_data(as_text=True))
        self.assertIn('attachment', text.headers['Content-Disposition'])
        self.assertEqual(self.client.get('/api/ports/305/tap').json['entries'], [])   # other ports unaffected
        self.client.post('/api/ports/300/tap/stop', headers=h)
        self.assertFalse(self.client.get('/api/ports/300/tap').json['active'])
        self.assertEqual(self.client.get('/api/ports/300/tap', environ_overrides={'REMOTE_ADDR': '192.0.2.1'}).status_code, 403)


if __name__ == '__main__':
    unittest.main()
