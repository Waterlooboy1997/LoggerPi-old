"""Durable administration, configuration and work queue (no network calls)."""
from contextlib import contextmanager
import copy
import csv
import json
import os
from pathlib import Path
import re
import secrets
import sqlite3
import time
from datetime import datetime
from zoneinfo import ZoneInfo
import xml.etree.ElementTree as ET

MAX_CONNECTIONS = 4
SAFE = re.compile(r'^[A-Za-z0-9_-]+$')
# A manual history download runs until done or stopped; this only guards a hung worker.
HISTORY_MAX_SECONDS = 24 * 3600
# Settings from the old window/budget model. They are dropped on load and never saved again.
OBSOLETE = ('always_online', 'window_start', 'window_end', 'hourly_attempt_window_minutes',
            'collection_interval_seconds', 'backfill_enabled', 'backfill_budget_seconds',
            'backfill_slice_seconds', 'backfill_record_limit', 'pakbus_physical_address',
            'pakbus_source_physical_address')


def defaults(site):
    site = copy.deepcopy(site)
    if 'hourly_attempts' not in site:
        # Keep roughly the old behaviour: retries every N seconds until the old closing minute.
        window = site.get('hourly_attempt_window_minutes', 10)
        spacing = site.get('retry_interval_seconds', 60)
        try:
            site['hourly_attempts'] = max(1, min(60, int(window * 60 // max(1, spacing))))
        except (TypeError, ValueError):
            site['hourly_attempts'] = 10
    for key in OBSOLETE:
        site.pop(key, None)
    site.setdefault('id', site.get('name', ''))
    site.setdefault('label', site.get('name', ''))
    site.setdefault('enabled', False)
    site.setdefault('timezone', 'Australia/Brisbane')
    site.setdefault('retry_interval_seconds', 60)
    site.setdefault('timeout_seconds', 10)
    site.setdefault('pakbus_source', 2050)
    site.setdefault('recent_budget_seconds', 60)
    site.setdefault('overlap_seconds', 3600)
    site.setdefault('tables', ['Measurement'])
    site.setdefault('port', 0)
    site.setdefault('pakbus_address', None)
    site.setdefault('expected_serial', '')
    site.setdefault('host', '')
    site.setdefault('gateway', '')
    return site


def issues(site):
    errors = []
    if not SAFE.fullmatch(site.get('name', '')):
        errors.append('Use letters, numbers, underscores or hyphens for site code.')
    try:
        ZoneInfo(site['timezone'])
    except Exception:
        errors.append('Unknown timezone.')
    if not site.get('tables') or any(not SAFE.fullmatch(t) for t in site.get('tables', [])):
        errors.append('Specify safe table names.')
    if len(set(site.get('tables', []))) != len(site.get('tables', [])):
        errors.append('Tables must be unique.')
    numbers_ok = True
    for key, low, high in [('timeout_seconds', 1, 60), ('retry_interval_seconds', 10, 3600),
                           ('hourly_attempts', 1, 60), ('recent_budget_seconds', 5, 600),
                           ('overlap_seconds', 0, 86400), ('pakbus_source', 0, 4095)]:
        v = site.get(key)
        if isinstance(v, bool) or not isinstance(v, (int, float)) or not low <= v <= high:
            errors.append(f'{key} must be between {low} and {high}.')
            numbers_ok = False
    if numbers_ok and (site['hourly_attempts'] - 1) * site['retry_interval_seconds'] >= 3600:
        errors.append('Every hourly attempt must start within the hour: reduce attempts or the time between them.')
    if not site.get('host') or any(c.isspace() for c in str(site.get('host', ''))):
        errors.append('Host is required and cannot contain spaces.')
    for key, high in [('port', 65535), ('pakbus_address', 4095)]:
        v = site.get(key)
        if isinstance(v, bool) or not isinstance(v, int) or not (1 if key == 'port' else 0) <= v <= high:
            errors.append(f'{key} is missing or invalid.')
    if not str(site.get('expected_serial', '')).strip():
        errors.append('Expected logger serial is required.')
    v = site.get('security_code')
    if v is not None and (not isinstance(v, int) or not 0 <= v <= 65535):
        errors.append('security_code is invalid.')
    if not isinstance(site.get('enabled'), bool):
        errors.append('enabled must be true or false.')
    return errors


def routine_schedule(site, now=None):
    """Automatic collection starts at the top of each hour in the logger timezone.

    Failed attempts retry within the same hour; any automatic work for an hour ends
    when the next hour starts.
    """
    now = time.time() if now is None else now
    local = datetime.fromtimestamp(now, ZoneInfo(site['timezone']))
    cycle = local.replace(minute=0, second=0, microsecond=0).timestamp()
    return {'cycle': cycle, 'deadline': cycle + 3600, 'next': cycle + 3600}


class Control:
    def __init__(self, state='state'):
        self.root = Path(state).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.path = self.root / 'admin.sqlite3'
        with self.connect() as db:
            db.executescript('''
            CREATE TABLE IF NOT EXISTS kv(key TEXT PRIMARY KEY,value TEXT);
            CREATE TABLE IF NOT EXISTS revisions(id INTEGER PRIMARY KEY,stamp REAL,config TEXT);
            CREATE TABLE IF NOT EXISTS jobs(id INTEGER PRIMARY KEY,site TEXT,kind TEXT,tab TEXT,
              state TEXT,created REAL,started REAL,finished REAL,deadline REAL,snapshot TEXT,
              override_window INTEGER DEFAULT 0,message TEXT DEFAULT '',received INTEGER DEFAULT 0,
              pid INTEGER,cancel INTEGER DEFAULT 0);
            CREATE TABLE IF NOT EXISTS streams(site TEXT,tab TEXT,recent TEXT,baseline TEXT,
              history TEXT,history_done INTEGER DEFAULT 0,PRIMARY KEY(site,tab));
            CREATE TABLE IF NOT EXISTS budget(site TEXT,bucket INTEGER,seconds REAL DEFAULT 0,
              records INTEGER DEFAULT 0,PRIMARY KEY(site,bucket));
            CREATE INDEX IF NOT EXISTS jobs_stream ON jobs(site,kind,tab,id DESC);
            CREATE INDEX IF NOT EXISTS jobs_state ON jobs(state,site);
            CREATE INDEX IF NOT EXISTS jobs_success ON jobs(site,kind,state,finished DESC);
            CREATE INDEX IF NOT EXISTS jobs_attempt ON jobs(site,kind,tab,started DESC);
            CREATE TABLE IF NOT EXISTS heartbeat(id INTEGER PRIMARY KEY,stamp REAL);
            ''')
            columns={row[1] for row in db.execute('PRAGMA table_info(jobs)')}
            for name,definition in [('automatic','INTEGER DEFAULT 0'),('scheduled_for','REAL'),('phase',"TEXT DEFAULT ''"),('phase_updated','REAL')]:
                if name not in columns:db.execute(f'ALTER TABLE jobs ADD COLUMN {name} {definition}')
            if not db.execute("SELECT 1 FROM kv WHERE key='session_secret'").fetchone():
                db.execute('INSERT INTO kv VALUES(?,?)', ('session_secret', secrets.token_hex(32)))

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.path, timeout=15)
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA journal_mode=WAL')
        try:
            with db:
                yield db
        finally:
            db.close()

    def get(self, key, fallback=None):
        with self.connect() as db:
            row = db.execute('SELECT value FROM kv WHERE key=?', (key,)).fetchone()
        return json.loads(row[0]) if row and key not in ('password_hash', 'session_secret') else row[0] if row else fallback

    def set(self, key, value):
        if key not in ('password_hash', 'session_secret'):
            value = json.dumps(value)
        with self.connect() as db:
            db.execute('INSERT OR REPLACE INTO kv VALUES(?,?)', (key, value))

    def config(self):
        config=self.get('config', {'revision': 0, 'max_connections': 4,
                                  'output_directory': str(self.root / 'collected'), 'sites': []})
        config['max_connections']=MAX_CONNECTIONS
        config['sites']=[defaults(site) for site in config['sites']]
        return config

    def save_config(self, config, revision, membership=False):
        """Save settings. Adding or removing sites needs membership=True (see add_site/remove_site)."""
        config = copy.deepcopy(config)
        if config.get('max_connections',MAX_CONNECTIONS)!=MAX_CONNECTIONS:
            raise ValueError('Concurrent connections are fixed at 4.')
        config['max_connections']=MAX_CONNECTIONS
        out = Path(config['output_directory']).resolve()
        reference = (Path(__file__).parent / 'reference_data').resolve()
        if out == reference or reference in out.parents:
            raise ValueError('The reference_data folder cannot be used for collection output.')
        names, ids = set(), set()
        for site in config['sites']:
            if site['name'] in names or site['id'] in ids:
                raise ValueError('Duplicate site code or identifier.')
            names.add(site['name']); ids.add(site['id'])
            if not SAFE.fullmatch(site['name']) or not SAFE.fullmatch(site['id']):
                raise ValueError('Unsafe site name or identifier.')
            errors = issues(site)
            non_connection = [e for e in errors if not e.startswith(('Host is required', 'port is missing', 'pakbus_address is missing', 'Expected logger serial'))]
            if non_connection or (errors and site['enabled']):
                raise ValueError('; '.join(non_connection or errors))
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute("SELECT value FROM kv WHERE key='config'").fetchone()
            current = json.loads(row[0]) if row else {'revision': 0}
            if current['revision'] != revision:
                raise ValueError('Configuration changed elsewhere. Reload before saving.')
            if not membership and {s['id'] for s in current.get('sites', [])} != ids:
                raise ValueError('Sites can only be added or removed with Add site or Remove site.')
            config['revision'] = revision + 1
            config['output_directory'] = str(out)
            encoded = json.dumps(config)
            db.execute("INSERT OR REPLACE INTO kv VALUES('config',?)", (encoded,))
            db.execute('INSERT INTO revisions(stamp,config) VALUES(?,?)', (time.time(), encoded))
            # Obsolete queued work must not use an old configuration.
            db.execute("UPDATE jobs SET state='cancelled',message='Configuration changed',finished=? WHERE state='queued'", (time.time(),))
        temp = self.root / 'settings.json.tmp'
        # SQLite is authoritative; this mirror supports the original CLI.
        try:
            temp.write_text(json.dumps(config, indent=2), encoding='utf-8')
            os.replace(temp, self.root / 'settings.json')
        except OSError:
            pass
        return config

    def add_site(self, site, confirm):
        """Add one new site. It always starts with routine collection off."""
        site = defaults(dict(site, enabled=False))
        site['id'] = site['label'] = site['name']
        if confirm != site['name']:
            raise ValueError('Type the new site code exactly to confirm adding it.')
        config = self.config()
        if any(s['name'].lower() == site['name'].lower() or s['id'] == site['id'] for s in config['sites']):
            raise ValueError(f"A site called {site['name']} already exists.")
        config['sites'].append(site)
        return self.save_config(config, config['revision'], membership=True)

    def remove_site(self, identifier, confirm, revision):
        """Remove one site's configuration. Collected files and journals are kept."""
        config = self.config()
        site = next((s for s in config['sites'] if s['id'] == identifier), None)
        if site is None:
            raise ValueError('Unknown site.')
        if confirm != site['name']:
            raise ValueError(f"Type {site['name']} exactly to confirm removal.")
        if site['enabled']:
            raise ValueError('Turn off routine collection before removing this site.')
        with self.connect() as db:
            if db.execute("SELECT 1 FROM jobs WHERE site=? AND state IN ('queued','running')", (identifier,)).fetchone():
                raise ValueError('Stop this site\'s current work before removing it.')
        config['sites'] = [s for s in config['sites'] if s['id'] != identifier]
        return self.save_config(config, revision, membership=True)

    def enqueue(self, site, kind, tab=None, override=False, automatic=False, scheduled_for=None):
        if kind not in ('recent', 'backfill', 'test'):
            raise ValueError('Unknown action.')
        if issues(site) or (not site.get('enabled') and automatic):
            raise ValueError('Review connection details first; routine collection must be enabled for automatic work.')
        if kind == 'backfill' and not self.stream(site['id'], tab)['baseline']:
            raise ValueError('Run Collect now once first, so history knows where recent data starts.')
        if tab and tab not in site['tables']:
            raise ValueError('Unknown table.')
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            existing = db.execute("SELECT id FROM jobs WHERE site=? AND kind=? AND tab IS ? AND state IN ('queued','running')", (site['id'], kind, tab)).fetchone()
            if existing:
                return existing[0]
            snapshot = dict(site, output_directory=self.config()['output_directory'])
            cur = db.execute("INSERT INTO jobs(site,kind,tab,state,created,snapshot,override_window,automatic,scheduled_for) VALUES(?,?,?,'queued',?,?,?,?,?)",
                             (site['id'], kind, tab, time.time(), json.dumps(snapshot), int(override), int(automatic), scheduled_for))
            return cur.lastrowid

    def jobs(self, limit=100):
        with self.connect() as db:
            return [dict(r) for r in db.execute('SELECT * FROM jobs ORDER BY id DESC LIMIT ?', (limit,))]

    def stream(self, site, table):
        with self.connect() as db:
            row = db.execute('SELECT * FROM streams WHERE site=? AND tab=?', (site, table)).fetchone()
        return dict(row) if row else {'recent': None, 'baseline': None, 'history': None, 'history_done': 0}

    def checkpoint(self, job, stamp=None, count=0, complete=False, coverage=None, baseline=None):
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            db.execute('INSERT OR IGNORE INTO streams(site,tab) VALUES(?,?)', (job['site'], job['tab']))
            if job['kind'] == 'recent':
                if baseline:
                    db.execute('UPDATE streams SET baseline=COALESCE(baseline,?) WHERE site=? AND tab=?', (baseline, job['site'], job['tab']))
                cursor = coverage if complete else stamp
                if cursor:
                    db.execute('UPDATE streams SET recent=? WHERE site=? AND tab=? AND (recent IS NULL OR recent<?)', (cursor, job['site'], job['tab'], cursor))
            elif job['kind'] == 'backfill':
                cursor = coverage if complete else stamp
                if cursor:
                    db.execute('UPDATE streams SET history=? WHERE site=? AND tab=? AND (history IS NULL OR history<?)', (cursor, job['site'], job['tab'], cursor))
                if complete and coverage:
                    db.execute('UPDATE streams SET history_done=(baseline IS NOT NULL AND history>=baseline) WHERE site=? AND tab=?', (job['site'], job['tab']))
            db.execute('UPDATE jobs SET received=received+? WHERE id=?', (count, job['id']))

    def progress(self, identifier, phase, message=None):
        if phase not in ('Connecting','Connected','Downloading','Failed'):
            raise ValueError('Unknown worker phase.')
        with self.connect() as db:
            if message is None:
                db.execute("UPDATE jobs SET phase=?,phase_updated=? WHERE id=? AND state='running'",(phase,time.time(),identifier))
            else:
                db.execute("UPDATE jobs SET phase=?,phase_updated=?,message=? WHERE id=? AND state='running'",(phase,time.time(),message,identifier))

    def status(self):
        from site_status import last_records,summarize,short_error
        config=self.config()
        now=time.time()
        records=last_records(config['output_directory'])
        routines={s['id']:routine_schedule(s,now) for s in config['sites']}
        errors={s['id']:issues(s) for s in config['sites']}
        summaries={}
        with self.connect() as db:
            streams=[dict(r) for r in db.execute('SELECT * FROM streams')]
            heartbeat=db.execute('SELECT stamp FROM heartbeat WHERE id=1').fetchone()
            for site in config['sites']:
                identifier=site['id']
                row=db.execute('SELECT id,kind,tab,state,message,automatic,scheduled_for FROM jobs WHERE site=? ORDER BY id DESC LIMIT 1',(identifier,)).fetchone()
                latest=dict(row) if row else None
                retrying=False
                if latest and latest['automatic'] and latest['scheduled_for']==routines[identifier]['cycle']:
                    used=db.execute("SELECT COUNT(*) FROM jobs WHERE site=? AND tab IS ? AND kind='recent' AND automatic=1 AND scheduled_for=? AND state IN ('failed','limited','interrupted')",(identifier,latest['tab'],latest['scheduled_for'])).fetchone()[0]
                    retrying=used<site['hourly_attempts']
                active=[dict(r) for r in db.execute("SELECT id,tab,kind,state,phase,received FROM jobs WHERE site=? AND state IN ('queued','running') ORDER BY state DESC,id",(identifier,))]
                tables=[]
                primary='Measurement' if 'Measurement' in site['tables'] else site['tables'][0]
                primary_success=None
                for table in site['tables']:
                    row=db.execute("SELECT id,state,message,finished FROM jobs WHERE site=? AND tab=? AND kind='recent' ORDER BY id DESC LIMIT 1",(identifier,table)).fetchone()
                    success=db.execute("SELECT finished FROM jobs WHERE site=? AND tab=? AND kind='recent' AND state='success' ORDER BY finished DESC LIMIT 1",(identifier,table)).fetchone()
                    item=dict(row) if row else {'state':'not_collected','message':''}
                    if item['state'] in ('failed','limited','interrupted'):item['message']=short_error(item['message'])
                    item.update(name=table,last_success=success[0] if success else None,last_record=records.get((site['name'],table),{}).get('timestamp'))
                    tables.append(item)
                    if table==primary:primary_success=success[0] if success else None
                summaries[identifier]=summarize(site,latest,tables,active,errors[identifier],retrying,records,primary_success)
        jobs=self.jobs()
        for job in jobs:job.pop('snapshot',None)
        return {'config':redact(config),'jobs':jobs,'streams':streams,'sites':summaries,
                'scheduler_online':bool(heartbeat and now-heartbeat[0]<10),'now':now,
                'issues':errors,'routines':routines}


def redact(value):
    value = copy.deepcopy(value)
    for site in value.get('sites', []):
        if 'security_code' in site:
            site['has_security_code'] = bool(site.pop('security_code'))
    return value


def backup_preview(data, reference):
    """Read embedded XML as data; never execute archived content or restore files."""
    if len(data) > 20 * 1024 * 1024:
        raise ValueError('Backup exceeds 20 MB.')
    start = data.find(b'<?xml')
    end = data.find(b'</lgrnet-config>', start)
    if start < 0 or end < 0:
        raise ValueError('No LoggerNet configuration found.')
    xml = data[start:end+len(b'</lgrnet-config>')]
    if b'<!DOCTYPE' in xml.upper() or b'<!ENTITY' in xml.upper():
        raise ValueError('XML entities are not supported.')
    root = ET.fromstring(xml)
    stack, result = [], []
    reference = Path(reference)
    folders = {p.name.upper(): p for p in reference.iterdir() if p.is_dir()} if reference.exists() else {}
    for device in root.find('devices'):
        level = int(device.get('level', 0))
        stack = stack[:level]
        stack.append(device)
        logger = device.find('class-bmp5-datalogger')
        if logger is None:
            continue
        name = device.get('name', '')
        folder = folders.get(name.upper())
        if folder is None:
            station = logger.get('reported-station-name', '')
            folder = folders.get(station.upper())
        endpoint = next((e for ancestor in reversed(stack) for e in ancestor.iter('setting') if e.get('host') and e.get('port')), None)
        site = defaults({'name': name, 'label': name, 'id': name})
        notes = ['PakBus destination requires confirmation; TCP port is not assumed to be its address.']
        if endpoint is not None:
            site.update(host=endpoint.get('host'), port=int(endpoint.get('port')))
        if folder:
            paths = list(folder.glob('*_Measurement.dat'))
            if paths:
                with paths[0].open(encoding='utf-8-sig', newline='') as handle:
                    header = next(csv.reader(handle))
                if len(header) >= 4 and header[0] == 'TOA5':
                    site.update(expected_serial=header[3], model=header[2], name=folder.name, id=folder.name)
            site['reference_folder'] = folder.name
        else:
            notes.append('No exact reference-folder mapping; review site identity.')
        schedule = next((e for e in device.iter('setting') if e.get('prim-retry-interval')), None)
        if schedule is not None:
            site['retry_interval_seconds'] = min(3600, max(10, int(schedule.get('prim-retry-interval', '300000'))//1000))
        tables = [e.get('name') for e in device.iter('collect-area')
                  if any(x.get('id') == '2' and x.text == '1' for x in e.iter('setting'))]
        if tables:
            site['tables'] = tables
        site['gateway'] = f"{site['host']}:{site['port']}" if site['host'] else ''
        site['routing_path'] = '/'.join(d.get('name', '') for d in stack[:-1])
        site['import_notes'] = notes
        result.append(site)
    return redact({'sites': result})['sites']
