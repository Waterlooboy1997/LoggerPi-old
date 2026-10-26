"""A single deadline-supervised job; configuration is a durable launch snapshot."""
import argparse
from datetime import datetime, timedelta
import json
import logging
import sys
import os
import threading
import time
from control import Control
import app
from connection import CONNECTION_TIMEOUT_SECONDS, CONNECTION_TIMEOUT_EXIT


def run(state, identifier):
    control = Control(state)
    with control.connect() as db:
        job = dict(db.execute('SELECT * FROM jobs WHERE id=?', (identifier,)).fetchone())
    remaining = max(0, job['deadline'] - time.time())
    watchdog = threading.Timer(remaining, lambda: os._exit(124))
    watchdog.daemon = True
    watchdog.start()
    site = json.loads(job['snapshot'])
    kind = job['kind']
    if job['tab']:
        site['tables'] = [job['tab']]
    root = site['output_directory']
    # Resume/export independently from a recent cursor: history must not move it.
    stream = control.stream(site['id'], job['tab'])
    start, end = None, None
    limits = {'fresh_only': True}
    if kind == 'recent' and stream['recent']:
        start = datetime.fromisoformat(stream['recent']) - timedelta(seconds=site['overlap_seconds'])
    if kind == 'backfill':
        if not stream['baseline']:
            raise ValueError('Collect recent records before starting backfill.')
        start = datetime.fromisoformat(stream['history']) - timedelta(seconds=1) if stream['history'] else datetime(1990,1,1)
        # One continuous download up to where recent collection began; each batch checkpoints.
        end = datetime.fromisoformat(stream['baseline'])
    connection_watchdog=None
    def progress(phase):
        nonlocal connection_watchdog
        if phase=='Connecting':
            connection_watchdog=threading.Timer(CONNECTION_TIMEOUT_SECONDS,lambda: os._exit(CONNECTION_TIMEOUT_EXIT))
            connection_watchdog.daemon=True
            connection_watchdog.start()
        elif phase=='Connected' and connection_watchdog is not None:
            connection_watchdog.cancel()
        control.progress(identifier,phase)
    try:
        app.collect(site, root, start, end, inspect_only=kind == 'test', limits=limits,
                    checkpoint=lambda **kwargs: control.checkpoint(job, **kwargs),progress=progress)
    finally:
        if connection_watchdog is not None:connection_watchdog.cancel()
        watchdog.cancel()


if __name__ == '__main__':
    p=argparse.ArgumentParser(); p.add_argument('--state',required=True);p.add_argument('--job',type=int,required=True)
    args=p.parse_args()
    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s')
    logging.getLogger('pylink').setLevel(logging.WARNING)
    logging.getLogger('pycampbellcr1000').setLevel(logging.WARNING)
    try:
        run(args.state,args.job)
    except Exception as exc:
        from site_status import short_error
        Control(args.state).progress(args.job,'Failed',short_error(exc))
        logging.exception('Job failed; committed batches are retained')
        sys.exit(1)
