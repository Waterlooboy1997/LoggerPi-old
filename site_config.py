"""Persistent callback-port labels; independent of TCP forwarding state."""
import copy
import json
import os
from pathlib import Path
import tempfile
import threading
import unicodedata

SEED_PATH = Path(__file__).resolve().with_name('sites.json')


def validate_name(value):
    if not isinstance(value, str):
        raise ValueError('Site must be a string')
    if any(unicodedata.category(c) == 'Cc' for c in value):
        raise ValueError('Site must not contain control characters')
    value = value.strip()
    if len(value) > 100:
        raise ValueError('Site must be at most 100 characters')
    return value


class SiteConfig:
    def __init__(self, path=None):
        self.path = Path(path or os.getenv('SITE_CONFIG_PATH') or SEED_PATH)
        self.lock = threading.RLock()
        source = self.path if self.path.exists() else SEED_PATH
        try:
            self.entries = json.loads(source.read_text(encoding='utf-8'))
            if not isinstance(self.entries, list):
                raise ValueError('Expected a list of site entries')
            ports = set()
            for entry in self.entries:
                if not isinstance(entry, dict):
                    raise ValueError('Each site entry must be an object')
                entry['site'] = validate_name(entry.get('site'))
                port = entry.get('callback_port')
                if port is not None:
                    if type(port) is not int or not 1 <= port <= 65535 or port in ports:
                        raise ValueError('Callback ports must be unique integers from 1 to 65535')
                    ports.add(port)
                pakbus = entry.get('pakbus')
                if pakbus is not None and (type(pakbus) is not int or pakbus < 0):
                    raise ValueError('PakBus must be a non-negative integer or null')
        except (ValueError, OSError) as exc:
            raise ValueError(f'Cannot load site configuration {source}: {exc}') from exc
        if source != self.path:
            self._write(self.entries)

    def _write(self, entries):
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=self.path.parent,
                                             prefix='.sites-', suffix='.tmp', delete=False) as output:
                temporary = output.name
                json.dump(entries, output, indent=2, ensure_ascii=False)
                output.write('\n')
                output.flush()
                os.fsync(output.fileno())
            os.replace(temporary, self.path)
        finally:
            if temporary and os.path.exists(temporary):
                os.unlink(temporary)

    def names(self):
        with self.lock:
            return {entry['callback_port']: entry['site'] for entry in self.entries
                    if entry.get('callback_port') is not None}

    def set_site(self, port, name):
        name = validate_name(name)
        with self.lock:
            updated = copy.deepcopy(self.entries)
            entry = next((e for e in updated if e.get('callback_port') == port), None)
            if entry is None:
                updated.append({'site': name, 'callback_port': port, 'pakbus': None})
            else:
                entry['site'] = name
            self._write(updated)
            self.entries = updated
        return name
