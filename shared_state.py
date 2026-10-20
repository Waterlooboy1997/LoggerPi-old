import threading
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from config import MAX_EVENTS_PER_PORT

def now():
    return datetime.now(timezone.utc)

def iso(dt):
    return dt.isoformat() if dt else None

@dataclass
class ClientInfo:
    writer: object
    peer: str
    ip: str
    port: int
    connected_at: datetime
    last_activity: datetime
    bytes_in: int = 0
    bytes_out: int = 0

@dataclass
class PortState:
    port: int
    clients: list = field(default_factory=list)
    reject_count: int = 0
    duplicate_ip_count: int = 0
    last_event: str = ''
    last_event_time: object = None
    recent_events: deque = field(default_factory=lambda: deque(maxlen=MAX_EVENTS_PER_PORT))
    listening: bool = False

class SharedState:
    def __init__(self, ports):
        self.lock = threading.RLock()
        self.ports = {p: PortState(p) for p in ports}
        self.events = deque(maxlen=2000)
        self.sequence = 0
        self.total_in = 0
        self.total_out = 0

    def event(self, port, level, message):
        with self.lock:
            self.sequence += 1
            timestamp = now()
            entry = {'id': self.sequence, 'port': port, 'level': level, 'message': message, 'time': iso(timestamp)}
            self.events.append(entry)
            if port in self.ports:
                state = self.ports[port]
                state.recent_events.append(entry)
                state.last_event = message
                state.last_event_time = timestamp

    def listening(self, port, value):
        with self.lock:
            self.ports[port].listening = value

    def increment(self, port, attribute):
        with self.lock:
            state = self.ports[port]
            setattr(state, attribute, getattr(state, attribute) + 1)

    def add_client(self, port, client):
        with self.lock:
            self.ports[port].clients.append(client)

    def remove_client(self, port, writer):
        with self.lock:
            state = self.ports[port]
            state.clients = [c for c in state.clients if c.writer is not writer]

    def traffic(self, port, writer, received=0, sent=0):
        with self.lock:
            self.total_in += received
            self.total_out += sent
            for client in self.ports[port].clients:
                if client.writer is writer:
                    client.last_activity = now()
                    client.bytes_in += received
                    client.bytes_out += sent
                    break

    def clear_events(self, port):
        with self.lock:
            state = self.ports[port]
            state.recent_events.clear()
            state.last_event = ''
            state.last_event_time = None
            self.events = deque((e for e in self.events if e['port'] != port), maxlen=2000)

    def snapshot(self, port=None):
        with self.lock:
            states = [self.ports[port]] if port is not None else self.ports.values()
            result = []
            for s in states:
                result.append({'port': s.port, 'listening': s.listening, 'reject_count': s.reject_count,
                               'duplicate_ip_count': s.duplicate_ip_count, 'last_event': s.last_event,
                               'last_event_time': iso(s.last_event_time),
                               'clients': [{'peer': c.peer, 'ip': c.ip, 'source_port': c.port,
                                            'connected_at': iso(c.connected_at), 'last_activity': iso(c.last_activity),
                                            'bytes_in': c.bytes_in, 'bytes_out': c.bytes_out} for c in s.clients]})
            return result[0] if port is not None else result

    def get_events(self, port=None, since=0, limit=150):
        with self.lock:
            source = self.ports[port].recent_events if port is not None else self.events
            return [dict(e) for e in source if e['id'] > since][-limit:]

    def totals(self):
        with self.lock:
            return {'bytes_in': self.total_in, 'bytes_out': self.total_out}
