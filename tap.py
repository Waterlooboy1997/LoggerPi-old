"""Live traffic tap: observe what crosses a data port without changing it.

Everything here is read-only with respect to forwarding. Bytes are copied into a
bounded buffer *after* they have been read, and the tap never writes to a socket.
Capture is off by default, per port, and expires automatically.

PakBus decoding is best-effort and informational. Framing, header layout and the
signature algorithm follow PyCampbellCR1000's pakbus.py (sync byte 0xBD, 0xBC
quoting, four 16-bit header words, signature residue of zero).
"""
import threading
import time
from collections import deque
from datetime import datetime, timezone

SYNC = 0xBD
QUOTE = 0xBC

MAX_ENTRIES_PER_PORT = 3000
MAX_STORED_BYTES = 512          # bytes kept per chunk; true length is always recorded
SYNC_REPORT_MIN = 3             # a closing+opening 0xBD pair (2) is normal framing
MAX_FRAME_BUFFER = 70000        # discard a runaway unframed stream beyond this
DEFAULT_MINUTES = 10
MAX_MINUTES = 60
MAX_TAPPED_PORTS = 8

# (HiProtoCode, MsgType) -> name. Only types present in PyCampbellCR1000 are named;
# anything else is shown numerically rather than guessed.
MESSAGE_NAMES = {
    (0, 0x09): 'Hello command', (0, 0x89): 'Hello response', (0, 0x0E): 'Hello request',
    (0, 0x0D): 'Bye', (0, 0x0F): 'Get settings command', (0, 0x8F): 'Get settings response',
    (0, 0x81): 'Delivery failure',
    (1, 0x09): 'Collect data command', (1, 0x89): 'Collect data response',
    (1, 0x17): 'Clock command', (1, 0x97): 'Clock response',
    (1, 0x18): 'Programming statistics command', (1, 0x98): 'Programming statistics response',
    (1, 0x1C): 'File download command', (1, 0x9C): 'File download response',
    (1, 0x1D): 'File upload command', (1, 0x9D): 'File upload response',
    (1, 0xA1): 'Please wait',
}
LINK_STATES = {0x9: 'ring', 0xA: 'ready', 0xB: 'finished', 0x8: 'off-line'}


def compute_signature(buff, seed=0xAAAA):
    sig = seed
    for x in buff:
        j = sig
        sig = (sig << 1) & 0x1FF
        if sig >= 0x100:
            sig += 1
        sig = ((((sig + (j >> 8) + x) & 0xFF) | (j << 8))) & 0xFFFF
    return sig


def unquote(data):
    return data.replace(b'\xBC\xDD', b'\xBD').replace(b'\xBC\xDC', b'\xBC')


def decode_frame(body):
    """Decode one frame body (between sync bytes, still quoted). Returns a dict."""
    packet = unquote(body)
    info = {'bytes': len(packet)}
    info['signature_ok'] = len(packet) >= 2 and compute_signature(packet) == 0
    payload = packet[:-2] if len(packet) >= 2 else b''
    if len(payload) < 8:
        info['summary'] = f'short frame ({len(packet)} bytes, not a PakBus packet?)'
        return info
    w0, w1, w2, w3 = (int.from_bytes(payload[i:i + 2], 'big') for i in (0, 2, 4, 6))
    link = w0 >> 12
    info.update(
        link_state=LINK_STATES.get(link, f'0x{link:X}'), dst_phys=w0 & 0xFFF,
        src_phys=w1 & 0xFFF, expect_more=w1 >> 14, priority=(w1 >> 12) & 3,
        proto=w2 >> 12, dst_node=w2 & 0xFFF, hops=w3 >> 12, src_node=w3 & 0xFFF)
    route = f"node {info['src_node']}→{info['dst_node']}"
    if info['src_phys'] != info['src_node'] or info['dst_phys'] != info['dst_node']:
        route += f" (phys {info['src_phys']}→{info['dst_phys']})"
    if len(payload) < 10:
        info['summary'] = f'{route}, header only'
        return info
    msg_type, tran = payload[8], payload[9]
    info.update(msg_type=msg_type, tran=tran)
    name = MESSAGE_NAMES.get((info['proto'], msg_type))
    proto_name = {0: 'PakCtrl', 1: 'BMP5'}.get(info['proto'], f"proto {info['proto']}")
    label = name or f'{proto_name} msg 0x{msg_type:02X}'
    extra = ''
    rest = payload[10:]
    if (info['proto'], msg_type) == (0, 0x81) and rest:
        extra = f' error code {rest[0]}'
    elif (info['proto'], msg_type) == (1, 0xA1) and len(rest) >= 3:
        extra = f' wait {int.from_bytes(rest[1:3], "big")}s'
    elif (info['proto'], msg_type) in ((0, 0x09), (0, 0x89)) and len(rest) >= 4:
        extra = f' verify interval {int.from_bytes(rest[2:4], "big")}s'
    elif info['proto'] == 1 and msg_type >= 0x80 and rest:
        extra = f' response code {rest[0]}'
    sig = '' if info['signature_ok'] else ' [BAD SIGNATURE]'
    info['summary'] = f'{label}, {route}, tran {tran}, {len(rest)} payload B{extra}{sig}'
    return info


class StreamDecoder:
    """Splits one direction of one connection into sync runs and frames."""

    def __init__(self):
        self.buffer = bytearray()
        self.sync_run = 0

    def feed(self, data):
        events = []
        for byte in data:
            if byte == SYNC:
                if self.buffer:
                    events.append(('frame', bytes(self.buffer)))
                    self.buffer.clear()
                    self.sync_run = 1
                else:
                    self.sync_run += 1
            else:
                if self.sync_run >= SYNC_REPORT_MIN:
                    events.append(('sync', self.sync_run))
                self.sync_run = 0
                self.buffer.append(byte)
                if len(self.buffer) > MAX_FRAME_BUFFER:
                    events.append(('overflow', len(self.buffer)))
                    self.buffer.clear()
        return events


def _iso(ts):
    return datetime.fromtimestamp(ts, timezone.utc).isoformat(timespec='milliseconds')


def _printable(data):
    return ''.join(chr(b) if 32 <= b < 127 else '.' for b in data)


class TrafficTap:
    def __init__(self, ports):
        self._lock = threading.RLock()
        self._until = {}                     # port -> monotonic expiry
        self._entries = {p: deque(maxlen=MAX_ENTRIES_PER_PORT) for p in ports}
        self._decoders = {}                  # (port, id(writer)) -> StreamDecoder
        self._seq = 0
        self._last_time = {}
        self._dropped = {p: 0 for p in ports}

    # -- control -----------------------------------------------------------
    def start(self, port, minutes=DEFAULT_MINUTES):
        minutes = max(1, min(MAX_MINUTES, int(minutes)))
        with self._lock:
            self._expire()
            if port not in self._until and len(self._until) >= MAX_TAPPED_PORTS:
                raise ValueError(f'At most {MAX_TAPPED_PORTS} ports can be captured at once')
            self._until[port] = time.monotonic() + minutes * 60
        return minutes

    def stop(self, port):
        with self._lock:
            self._until.pop(port, None)
            for key in [k for k in self._decoders if k[0] == port]:
                del self._decoders[key]

    def clear(self, port):
        with self._lock:
            self._entries[port].clear()
            self._dropped[port] = 0

    def active(self, port):
        until = self._until.get(port)
        if until is None:
            return False
        if time.monotonic() >= until:
            self.stop(port)
            return False
        return True

    def _expire(self):
        now = time.monotonic()
        for port in [p for p, u in self._until.items() if now >= u]:
            self.stop(port)

    # -- recording (called from the forwarding path; must never raise) ------
    def forget(self, port, writer):
        if not self._until:
            return
        try:
            with self._lock:
                self._decoders.pop((port, id(writer)), None)
        except Exception:
            pass

    def _append(self, port, entry):
        self._seq += 1
        entry['id'] = self._seq
        queue = self._entries[port]
        if len(queue) == queue.maxlen:
            self._dropped[port] += 1
        queue.append(entry)

    def event(self, port, level, message):
        if not self._until:
            return
        try:
            if not self.active(port):
                return
            with self._lock:
                ts = time.time()
                self._append(port, {'kind': 'event', 'time': _iso(ts), 'level': level, 'message': message})
        except Exception:                    # the tap must never disturb connection handling
            pass

    def chunk(self, port, writer, source, data, recipients, read_at=None):
        """One read from `source`. `recipients` lists the peers it was forwarded to."""
        if not self._until:
            return
        try:
            if not self.active(port):
                return
            ts = read_at or time.time()
            with self._lock:
                decoder = self._decoders.setdefault((port, id(writer)), StreamDecoder())
                frames = []
                for kind, value in decoder.feed(data):
                    if kind == 'frame':
                        frames.append(decode_frame(value))
                    elif kind == 'sync':
                        frames.append({'summary': f'attention/sync ×{value} (0xBD)'})
                    else:
                        frames.append({'summary': f'unframed data exceeded {value} bytes; discarded'})
                if decoder.sync_run >= SYNC_REPORT_MIN and not decoder.buffer:
                    frames.append({'summary': f'attention/sync ×{decoder.sync_run} (0xBD)'})
                    decoder.sync_run = 1
                delta = ts - self._last_time.get(port, ts)
                self._last_time[port] = ts
                kept = data[:MAX_STORED_BYTES]
                self._append(port, {
                    'kind': 'data', 'time': _iso(ts), 'delta': round(delta, 3), 'from': source,
                    'to': list(recipients), 'forwarded': bool(recipients), 'length': len(data),
                    'truncated': len(data) > len(kept), 'hex': kept.hex(' '), 'ascii': _printable(kept),
                    'frames': frames, 'pending_bytes': len(decoder.buffer)})
        except Exception:                    # the tap must never disturb forwarding
            pass

    # -- reading -------------------------------------------------------------
    def read(self, port, since=0, limit=500):
        with self._lock:
            self._expire()
            until = self._until.get(port)
            remaining = max(0, int(until - time.monotonic())) if until else 0
            entries = [dict(e) for e in self._entries[port] if e['id'] > since][-limit:]
            return {'active': until is not None, 'seconds_remaining': remaining,
                    'dropped': self._dropped[port], 'entries': entries}

    def as_text(self, port):
        lines = []
        for e in self.read(port, 0, MAX_ENTRIES_PER_PORT)['entries']:
            if e['kind'] == 'event':
                lines.append(f"{e['time']}  [{e['level']}] {e['message']}")
                continue
            to = ', '.join(e['to']) if e['forwarded'] else 'NOT FORWARDED (no other client)'
            lines.append(f"{e['time']}  +{e['delta']:.3f}s  {e['from']} -> {to}  {e['length']} B")
            for f in e['frames']:
                lines.append(f"    {f['summary']}")
            lines.append(f"    hex: {e['hex']}{' ...(truncated)' if e['truncated'] else ''}")
            if any(c != '.' for c in e['ascii']):
                lines.append(f"    txt: {e['ascii']}")
        return '\n'.join(lines) + '\n'
