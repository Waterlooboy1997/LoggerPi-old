"""Small, resumable Campbell logger collector. Python 3.9+."""
import argparse
import ast
import csv
import hashlib
import io
import json
import logging
import math
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timedelta
from decimal import Context, ROUND_HALF_UP
from zoneinfo import ZoneInfo
from contextlib import contextmanager

LOG = logging.getLogger("collector")


def text(value):
    if isinstance(value, bytes):
        return value.decode("utf-8")
    value = str(value)
    if value.startswith(("b'", 'b"')):
        try:
            return ast.literal_eval(value).decode("utf-8")
        except (ValueError, SyntaxError, AttributeError):
            pass
    return value


def clean(value):
    if isinstance(value, dict):
        return {text(k): clean(v) for k, v in value.items()}
    if isinstance(value, (tuple, list)):
        return [clean(v) for v in value]
    if isinstance(value, (bytes, datetime, timedelta)):
        return text(value)
    if isinstance(value, float) and not math.isfinite(value):
        return "NAN" if math.isnan(value) else ("INF" if value > 0 else "-INF")
    return value


def quote(value):
    return '"' + str(value).replace('"', '""') + '"'


def number(value, dtype):
    if dtype == 'ASCII':
        return quote(metadata_text(value))
    if isinstance(value, str):
        return quote(value)
    if isinstance(value, float) and not math.isfinite(value):
        return quote(clean(value))
    if isinstance(value, bool):
        return str(int(value))
    if isinstance(value, int):
        return str(value)
    if dtype == 'FP2' and value in (8191.0, -8191.0, -8190.0):
        # Reserved FP2 bit patterns decoded as ordinary numbers by library 0.4.
        return quote({8191.0:'INF', -8191.0:'-INF', -8190.0:'NAN'}[value])
    precision = 7 if dtype.startswith("IEEE4") else 16 if dtype.startswith("IEEE8") else 15
    if dtype.startswith(('IEEE4', 'IEEE8')):
        # LoggerNet rounds exact halfway values away from zero, unlike Python g.
        value = float(Context(prec=precision, rounding=ROUND_HALF_UP).create_decimal_from_float(value))
    return format(value, f".{precision}g")


def metadata_text(value):
    # pycampbellcr1000 0.4 unpacks one-byte ASCIIZ strings to integer bytes.
    return chr(value) if isinstance(value, int) and 0 <= value <= 255 else text(value)


def make_schema(site, table, definition, stats):
    fields = []
    for fld in definition['Fields']:
        name = metadata_text(fld['FieldName'])
        dimension = fld['Dimension'] if fld['FieldType'] != 'ASCII' else 1
        if dimension != 1:
            raise ValueError(f"Array field {name}: explicit TOA5 array mapping required")
        fields.append({'name': name, 'type': text(fld['FieldType']),
                       'units': metadata_text(fld['Units']), 'processing': metadata_text(fld['Processing'])})
    osver = text(stats['OSVer'])
    model = site.get('model') or osver.split('.')[0]
    header = ['TOA5', site['name'], model, text(stats['SerialNbr']), osver,
              text(stats['ProgName']), text(stats['ProgSig']), table]
    return {'header': header, 'fields': fields, 'table_signature': definition['Signature']}


def header_lines(schema):
    fields = schema['fields']
    rows = [schema['header'], ['TIMESTAMP', 'RECORD'] + [f['name'] for f in fields],
            ['TS', 'RN'] + [f['units'] for f in fields],
            ['', ''] + [f['processing'] for f in fields]]
    return [','.join(quote(v) for v in row) for row in rows]


def record_line(row, schema):
    row = {text(k): v for k, v in row.items()}
    timestamp = row.get('Datetime', row.get('TIMESTAMP'))
    if isinstance(timestamp, datetime):
        timestamp = timestamp.isoformat(' ')
    rec = int(row.get('RecNbr', row.get('RECORD')))
    values = [quote(timestamp), str(rec)]
    for fld in schema['fields']:
        values.append(number(row[fld['name']], fld['type']))
    return ','.join(values)


@contextmanager
def site_lock(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a+b') as handle:
        if handle.seek(0, 2) == 0:
            handle.write(b'0')
            handle.flush()
        handle.seek(0)
        if os.name == 'nt':
            import msvcrt
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            yield
        finally:
            handle.seek(0)
            if os.name == 'nt':
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(handle, fcntl.LOCK_UN)


class Store:
    def __init__(self, root):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.root / 'collector.sqlite3', timeout=30)
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.executescript('''
        CREATE TABLE IF NOT EXISTS generations(id INTEGER PRIMARY KEY, site TEXT, tab TEXT,
          fingerprint TEXT, schema_json TEXT, created TEXT);
        CREATE TABLE IF NOT EXISTS records(gen INTEGER, stamp TEXT, rec INTEGER, line TEXT,
          raw TEXT, PRIMARY KEY(gen,stamp,rec));
        CREATE TABLE IF NOT EXISTS conflicts(gen INTEGER, stamp TEXT, rec INTEGER, line TEXT,
          raw TEXT, UNIQUE(gen,stamp,rec,line));
        CREATE TABLE IF NOT EXISTS progress(gen INTEGER PRIMARY KEY, stamp TEXT, rec INTEGER);
        ''')

    def generation(self, site, table, schema):
        encoded = json.dumps(schema, sort_keys=True)
        fp = hashlib.sha256(encoded.encode()).hexdigest()
        previous = self.db.execute('SELECT id,fingerprint FROM generations WHERE site=? AND tab=? ORDER BY id DESC LIMIT 1', (site, table)).fetchone()
        if previous and previous[1] == fp:
            return previous[0]
        with self.db:
            cur = self.db.execute('INSERT INTO generations(site,tab,fingerprint,schema_json,created) VALUES(?,?,?,?,?)',
                                  (site, table, fp, encoded, datetime.now().isoformat()))
        return cur.lastrowid

    def reset_generation(self, gen):
        site, tab, fp, schema = self.db.execute('SELECT site,tab,fingerprint,schema_json FROM generations WHERE id=?', (gen,)).fetchone()
        with self.db:
            cur = self.db.execute('INSERT INTO generations(site,tab,fingerprint,schema_json,created) VALUES(?,?,?,?,?)', (site, tab, fp, schema, datetime.now().isoformat()))
        return cur.lastrowid

    def cursor(self, gen):
        return self.db.execute('SELECT stamp,rec FROM progress WHERE gen=?', (gen,)).fetchone()

    def accept(self, gen, rows, schema):
        with self.db:
            for row in rows:
                row = {text(k): v for k, v in row.items()}
                stamp = str(row.get('Datetime', row.get('TIMESTAMP')))
                rec = int(row.get('RecNbr', row.get('RECORD')))
                line = record_line(row, schema)
                raw = json.dumps(clean(row), sort_keys=True)
                old = self.db.execute('SELECT line FROM records WHERE gen=? AND stamp=? AND rec=?', (gen, stamp, rec)).fetchone()
                if old and old[0] != line:
                    self.db.execute('INSERT OR IGNORE INTO conflicts VALUES(?,?,?,?,?)', (gen, stamp, rec, line, raw))
                    LOG.warning('Conflicting record preserved: generation=%s timestamp=%s record=%s', gen, stamp, rec)
                else:
                    self.db.execute('INSERT OR IGNORE INTO records VALUES(?,?,?,?,?)', (gen, stamp, rec, line, raw))
                self.db.execute('INSERT INTO progress VALUES(?,?,?) ON CONFLICT(gen) DO UPDATE SET stamp=excluded.stamp,rec=excluded.rec WHERE excluded.stamp > progress.stamp OR (excluded.stamp=progress.stamp AND excluded.rec>progress.rec)', (gen, stamp, rec))

    def import_existing(self, path, gen, schema):
        if self.db.execute('SELECT 1 FROM generations WHERE site=(SELECT site FROM generations WHERE id=?) AND tab=(SELECT tab FROM generations WHERE id=?) AND id<>? LIMIT 1', (gen,gen,gen)).fetchone():
            return
        if not path.exists() or self.db.execute('SELECT 1 FROM records WHERE gen=? LIMIT 1', (gen,)).fetchone():
            return
        with path.open(newline='', encoding='utf-8') as handle:
            reader = csv.reader(handle)
            headers = [next(reader) for _ in range(4)]
            expected = list(csv.reader(header_lines(schema)))
            if headers != expected:
                raise ValueError(f'{path}: existing headers differ; archive or explicitly migrate before takeover')
            with self.db:
                for values in reader:
                    if not values:
                        continue
                    if len(values) != len(schema['fields']) + 2:
                        raise ValueError(f'{path}: malformed row')
                    stamp, rec = values[0], int(values[1])
                    # Preserve existing text exactly; no float roundtrip during takeover.
                    line = ','.join([quote(stamp), str(rec)] + [quote(v) if v in ('NAN','INF','-INF') or f['type']=='ASCII' else v for v,f in zip(values[2:],schema['fields'])])
                    raw = json.dumps({'imported': values})
                    old = self.db.execute('SELECT line FROM records WHERE gen=? AND stamp=? AND rec=?',(gen,stamp,rec)).fetchone()
                    if old and old[0] != line:
                        self.db.execute('INSERT OR IGNORE INTO conflicts VALUES(?,?,?,?,?)',(gen,stamp,rec,line,raw))
                        LOG.warning('Conflicting imported record preserved: %s %s',stamp,rec)
                    else:
                        self.db.execute('INSERT OR IGNORE INTO records VALUES(?,?,?,?,?)', (gen, stamp, rec, line, raw))
                last = self.db.execute('SELECT stamp,rec FROM records WHERE gen=? ORDER BY stamp DESC,rec DESC LIMIT 1', (gen,)).fetchone()
                if last:
                    self.db.execute('INSERT OR REPLACE INTO progress VALUES(?,?,?)', (gen, *last))

    def export(self, site, table):
        gens = self.db.execute('SELECT id,schema_json FROM generations WHERE site=? AND tab=? ORDER BY id', (site,table)).fetchall()
        for gen, encoded in gens:
            schema = json.loads(encoded)
            suffix = '' if gen == gens[-1][0] else f'.generation-{gen}'
            path = self.root / f'{site}_{table}{suffix}.dat'
            temp = path.with_suffix(path.suffix + '.tmp')
            with temp.open('w', newline='', encoding='utf-8') as handle:
                for line in header_lines(schema):
                    handle.write(line + '\r\n')
                for (line,) in self.db.execute('SELECT line FROM records WHERE gen=? ORDER BY stamp,rec', (gen,)):
                    handle.write(line + '\r\n')
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp, path)
        metadata = self.root / f'{site}_{table}.schemas.json'
        temp = metadata.with_suffix('.tmp')
        temp.write_text(json.dumps([{'generation': gen, 'schema': json.loads(s)} for gen,s in gens], indent=2), encoding='utf-8')
        os.replace(temp, metadata)


def collect(site, root, start=None, end=None, all_history=False, inspect_only=False, factory=None, limits=None, checkpoint=None, progress=None):
    progress = progress or (lambda phase: None)
    limits = limits or {}
    checkpoint = checkpoint or (lambda **kwargs: None)
    if factory is None:
        from connection import SingleAttemptCR1000
        factory = SingleAttemptCR1000.from_url
    root = Path(root)
    with site_lock(root / (site['name'] + '.lock')):
        device = None
        store = Store(root)
        try:
            progress('Connecting')
            device = factory(f"tcp:{site['host']}:{site['port']}", timeout=site.get('timeout_seconds', 3),
                             dest=site['pakbus_address'], src=site.get('pakbus_source', 2050),
                             dest_addr=site.get('pakbus_physical_address'),
                             src_addr=site.get('pakbus_source_physical_address'),
                             security_code=site.get('security_code', 0))
            logger_time = device.gettime()
            stats = clean(device.getprogstat())
            if str(stats['SerialNbr']) != str(site['expected_serial']):
                raise ValueError(f"Logger identity mismatch: expected {site['expected_serial']}, received {stats['SerialNbr']}")
            drift = (logger_time - datetime.now(ZoneInfo(site.get('timezone','Australia/Brisbane'))).replace(tzinfo=None)).total_seconds()
            LOG.info('%s clock drift %.1fs; correction disabled', site['name'], drift)
            progress('Connected')
            definitions = device.table_def
            metadata = {'identity': stats, 'logger_time': str(logger_time), 'drift_seconds': drift, 'tables': clean(definitions)}
            (root / f"{site['name']}.metadata.json").write_text(json.dumps(metadata, indent=2), encoding='utf-8')
            if inspect_only:
                print(json.dumps(metadata, indent=2))
                return
            for table in site['tables']:
                definition = next(d for d in definitions if text(d['Header']['TableName']) == table)
                schema = make_schema(site, table, definition, stats)
                gen = store.generation(site['name'], table, schema)
                store.import_existing(root / f"{site['name']}_{table}.dat", gen, schema)
                previous = None if limits.get('fresh_only') else store.cursor(gen)
                begin = datetime(1990,1,1) if all_history else start
                if begin is None:
                    begin = datetime.fromisoformat(previous[0]) - timedelta(seconds=site.get('overlap_seconds',3600)) if previous else logger_time - timedelta(hours=1)
                finish = end or logger_time
                if begin > finish:
                    raise ValueError('Collection start is after logger/end time; investigate clock rollback')
                checkpoint(baseline=str(begin))
                count = 0
                complete = True
                coverage = finish
                seek_history = limits.get('history_seek', False)
                exported_at = time.monotonic()
                last_seen = None
                last_gen = None
                fingerprint = store.db.execute('SELECT fingerprint FROM generations WHERE id=?',(gen,)).fetchone()[0]
                boundaries = store.db.execute('SELECT g.id,(SELECT MIN(stamp) FROM records WHERE gen=g.id) FROM generations g WHERE site=? AND tab=? AND fingerprint=? ORDER BY g.id',(site['name'],table,fingerprint)).fetchall()
                progress('Downloading')
                for batch in device.get_data_generator(table, begin, finish):
                    rows = [{text(k):v for k,v in row.items()} for row in batch]
                    pending = []
                    if rows and seek_history:
                        coverage = min(finish, datetime.fromisoformat(str(rows[0]['Datetime'])) + timedelta(days=1))
                        seek_history = False
                    stopped = False
                    for row in rows:
                        if datetime.fromisoformat(str(row['Datetime'])) > coverage:
                            stopped = True
                            break
                        if count >= limits.get('max_records', float('inf')):
                            complete = False
                            stopped = True
                            break
                        stamp, rec = str(row['Datetime']), int(row['RecNbr'])
                        known = store.db.execute('SELECT r.gen FROM records r JOIN generations g ON g.id=r.gen WHERE g.site=? AND g.tab=? AND g.fingerprint=? AND r.stamp=? AND r.rec=? ORDER BY r.gen DESC LIMIT 1', (site['name'],table,fingerprint,stamp,rec)).fetchone()
                        dated = [(identifier, first) for identifier, first in boundaries if first is not None]
                        earlier = [identifier for identifier, first in dated if first <= stamp]
                        target = known[0] if known else earlier[-1] if earlier else dated[0][0] if dated else gen
                        if target != gen:
                            store.accept(gen, pending, schema)
                            if pending:
                                checkpoint(stamp=str(pending[-1]['Datetime']), count=len(pending))
                            pending = []
                            gen = target
                        baseline = last_seen if last_gen == gen else store.cursor(gen)
                        if not known and baseline and stamp > baseline[0] and rec < baseline[1]:
                            store.accept(gen, pending, schema)
                            if pending:
                                checkpoint(stamp=str(pending[-1]['Datetime']), count=len(pending))
                            pending = []
                            store.export(site['name'], table)
                            gen = store.reset_generation(gen)
                            boundaries.append((gen, stamp))
                            LOG.warning('%s %s record reset: generation %s', site['name'],table,gen)
                        pending.append(row)
                        last_seen = (stamp, rec)
                        last_gen = gen
                        count += 1
                    store.accept(gen, pending, schema)
                    if pending:
                        checkpoint(stamp=str(pending[-1]['Datetime']), count=len(pending))
                    if stopped:
                        break
                    if time.monotonic() - exported_at >= 30:
                        store.export(site['name'],table)
                        exported_at = time.monotonic()
                        LOG.info('%s %s received=%s cursor=%s',site['name'],table,count,store.cursor(gen))
                store.export(site['name'],table)
                checkpoint(complete=complete, coverage=str(coverage) if complete else None)
                LOG.info('%s %s received=%s cursor=%s',site['name'],table,count,store.cursor(gen))
                time.sleep(site.get('inter_table_delay_seconds',0))
        finally:
            store.db.close()
            if device is not None:
                try:
                    device.bye()
                finally:
                    device.pakbus.link.close()


def load_config(path):
    config = json.loads(Path(path).read_text(encoding='utf-8'))
    names = set()
    for site in config['sites']:
        name = site['name']
        if name in names or not name or any(c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-' for c in name):
            raise ValueError('Site names must be unique safe filename components')
        names.add(name)
        for table in site['tables']:
            if not table or any(c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-' for c in table):
                raise ValueError('Unsafe table name')
        for key in ('collection_interval_seconds','retry_interval_seconds','timeout_seconds','max_run_seconds'):
            if site.get(key, 1) <= 0:
                raise ValueError(f'{key} must be positive')
    return config


def run_workers(args, config):
    selected = [s for s in config['sites'] if not args.site or s['name']==args.site]
    if not selected:
        raise ValueError('No matching site')
    due = {s['name']: time.monotonic()+s.get('startup_delay_seconds',0) for s in selected}
    active, done = {}, set()
    failed = False
    try:
        while len(done) < len(selected) or args.command == 'serve':
            now = time.monotonic()
            for site in selected:
                name = site['name']
                if name in active:
                    proc, launched = active[name]
                    expired = now-launched > (getattr(args,'max_run_seconds',None) or site.get('max_run_seconds',300))
                    if expired:
                        proc.kill()
                        proc.wait()
                    status = proc.poll()
                    if status is not None:
                        failed |= status != 0
                        del active[name]
                        done.add(name)
                        due[name] = now + site.get('collection_interval_seconds',3600) if status==0 else now + site.get('retry_interval_seconds',300)
                        LOG.info('%s worker exit=%s timeout=%s',name,status,expired)
                elif now >= due[name] and (args.command=='serve' or name not in done):
                    command = [sys.executable, str(Path(__file__).resolve()), '_worker', '--config',str(Path(args.config).resolve()),'--site',name,'--output',str(Path(args.output or config['output_directory']).resolve())]
                    for flag in ('start','end'):
                        if getattr(args,flag):
                            command += ['--'+flag,getattr(args,flag)]
                    if args.all:
                        command.append('--all')
                    if args.command=='inspect':
                        command.append('--inspect-only')
                    active[name] = (subprocess.Popen(command),now)
            if active or args.command=='serve':
                time.sleep(.2)
    finally:
        for proc,_ in active.values():
            proc.terminate()
            proc.wait()
    return int(failed)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['once','serve','inspect','_worker'])
    parser.add_argument('--config',default='settings.json')
    parser.add_argument('--state',default='state',help='Administration state for serve mode')
    parser.add_argument('--site')
    parser.add_argument('--output')
    parser.add_argument('--max-run-seconds',type=float,help='Override per-site wall time limit, e.g. 3600 for backfill')
    parser.add_argument('--start')
    parser.add_argument('--end')
    parser.add_argument('--all',action='store_true',help='Read all records still retained by logger')
    parser.add_argument('--inspect-only',action='store_true',help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.max_run_seconds is not None and args.max_run_seconds <= 0:
        parser.error('--max-run-seconds must be positive')
    if args.all and args.start:
        parser.error('--all and --start are mutually exclusive')
    for value in (args.start, args.end):
        if value and datetime.fromisoformat(value).tzinfo is not None:
            parser.error('Use logger-local timestamps without a UTC offset')
    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s')
    logging.getLogger('pycampbellcr1000').setLevel(logging.WARNING)
    logging.getLogger('pylink').setLevel(logging.WARNING)
    try:
        if args.command == 'serve':
            from scheduler import Scheduler
            from control import Control, defaults
            control = Control(args.state)
            if not control.config()['revision']:
                config = load_config(args.config)
                migrated = control.config()
                migrated['sites'] = [defaults(dict(s, enabled=False)) for s in config['sites']]
                control.save_config(migrated, 0)
            supervisor = Scheduler(control)
            with site_lock(control.root / 'scheduler.lock'):
                supervisor.recover()
                try:
                    while True:
                        supervisor.tick()
                        time.sleep(1)
                except KeyboardInterrupt:
                    pass
                finally:
                    supervisor.close()
            return 0
        config = load_config(args.config)
        if args.command=='_worker':
            site = next(s for s in config['sites'] if s['name']==args.site)
            collect(site,args.output or config['output_directory'],
                    datetime.fromisoformat(args.start) if args.start else None,
                    datetime.fromisoformat(args.end) if args.end else None,args.all,args.inspect_only)
            return 0
        return run_workers(args, config)
    except Exception:
        LOG.exception('Collection failed')
        return 1


if __name__ == '__main__':
    sys.exit(main())
