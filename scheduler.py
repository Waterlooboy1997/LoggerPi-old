"""Window-aware queue supervisor. No network collection runs inside the web app."""
import argparse
from datetime import datetime
import json
import logging
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from app import site_lock
from connection import CONNECTION_TIMEOUT_EXIT
from control import MAX_CONNECTIONS, Control, availability, routine_schedule, issues

LOG=logging.getLogger('scheduler')


class Scheduler:
    def __init__(self, control, popen=subprocess.Popen):
        self.control=control
        self.popen=popen
        self.active={}
        self.orphans=[]

    def recover(self):
        with self.control.connect() as db:
            self.orphans=[dict(j) for j in db.execute("SELECT * FROM jobs WHERE state='running' AND deadline>?",(time.time(),))]
            # PID reuse can point at an unrelated process. Do not kill old PIDs.
            # Worker deadlines and per-site locks protect data if an orphan remains.
            db.execute("UPDATE jobs SET state='interrupted',finished=?,message='Scheduler restarted; resume from checkpoint' WHERE state='running'",(time.time(),))

    def tick(self, now=None):
        now=time.time() if now is None else now
        self.orphans=[j for j in self.orphans if j['deadline']+2>now]
        config=self.control.config()
        sites={s['id']:s for s in config['sites']}
        with self.control.connect() as db:
            db.execute('INSERT OR REPLACE INTO heartbeat VALUES(1,?)',(now,))
        for identifier,(proc,handle,job) in list(self.active.items()):
            with self.control.connect() as db:
                cancel=db.execute('SELECT cancel FROM jobs WHERE id=?',(identifier,)).fetchone()[0]
            expired=now>=job['deadline']
            if cancel or expired:
                proc.kill();proc.wait()
            code=proc.poll()
            if code is None:
                continue
            handle.close()
            with self.control.connect() as db:
                worker_message=db.execute('SELECT message FROM jobs WHERE id=?',(identifier,)).fetchone()[0]
            message='Cancelled; committed records retained' if cancel else 'Window/budget deadline; committed records retained' if expired else 'Completed' if code==0 else worker_message or 'Connection or collection failed; see job log'
            state='cancelled' if cancel else 'limited' if expired else 'success' if code==0 else 'failed'
            if code==CONNECTION_TIMEOUT_EXIT and not cancel:
                state='failed'
                message='Connection failed after 20 seconds'
            with self.control.connect() as db:
                db.execute('UPDATE jobs SET state=?,finished=?,message=? WHERE id=?',(state,now,message,identifier))
                if job['kind']=='backfill':
                    received=db.execute('SELECT received FROM jobs WHERE id=?',(identifier,)).fetchone()[0]
                    db.execute('INSERT INTO budget(site,bucket,seconds,records) VALUES(?,?,?,?) ON CONFLICT(site,bucket) DO UPDATE SET seconds=seconds+excluded.seconds,records=records+excluded.records',
                               (job['site'],int(job['started']//3600),max(0,now-job['started']),received))
            del self.active[identifier]
        for site in sites.values():
            if not site['enabled'] or issues(site):
                continue
            window=availability(site,now)
            if not window['open']:
                continue
            routine=routine_schedule(site,now)
            if routine['open']:
                for table in site['tables']:
                    with self.control.connect() as db:
                        pending=bool(db.execute("SELECT 1 FROM jobs WHERE site=? AND tab=? AND kind='recent' AND state IN ('queued','running') LIMIT 1",(site['id'],table)).fetchone())
                        row=db.execute("SELECT * FROM jobs WHERE site=? AND tab=? AND kind='recent' AND automatic=1 AND scheduled_for=? ORDER BY id DESC LIMIT 1",(site['id'],table,routine['cycle'])).fetchone()
                        last=dict(row) if row else None
                    due=last is None
                    if last and last['state'] not in ('success','queued','running','cancelled','skipped'):
                        started=last['started'] or last['created']
                        interval=site['retry_interval_seconds']
                        next_slot=routine['cycle']+(int((started-routine['cycle'])//interval)+1)*interval
                        due=now>=max(next_slot,last['finished'] or now)
                    if due and not pending:
                        self.control.enqueue(site,'recent',table,automatic=True,scheduled_for=routine['cycle'])
            # History is explicitly enabled. The baseline is fixed by the first recent job.
            if site['backfill_enabled']:
                for table in site['tables']:
                    stream=self.control.stream(site['id'],table)
                    if stream['recent'] and stream['baseline'] and not stream['history_done']:
                        self.control.enqueue(site,'backfill',table)
        queued=[j for j in self.control.jobs(10000) if j['state']=='queued']
        candidates=[]
        for job in queued:
            site=sites.get(job['site'])
            if not site or (not site['enabled'] and (job.get('automatic') or job['kind']=='backfill')):
                continue
            window=availability(site,now)
            if job.get('automatic'):
                until=job['scheduled_for']+site.get('hourly_attempt_window_minutes',10)*60
                if now>=until:
                    with self.control.connect() as db:
                        db.execute("UPDATE jobs SET state='skipped',finished=?,message='Hourly attempt period ended; next attempt at the next hour' WHERE id=?",(now,job['id']))
                    continue
            if not window['open'] and not job['override_window']:
                continue
            # Deadline first, then oldest successful collection, then FIFO.
            with self.control.connect() as db:
                success=db.execute("SELECT finished FROM jobs WHERE site=? AND state='success' AND kind='recent' ORDER BY finished DESC LIMIT 1",(site['id'],)).fetchone()
            candidates.append((job,site,window,success[0] if success else 0))
        candidates.sort(key=lambda item:(2 if item[0]['kind']=='backfill' else 1 if item[0].get('automatic') else 0, item[2]['deadline'] if not item[1]['always_online'] else float('inf'),item[3],item[0]['created']))
        recent_waiting=any(j['kind']!='backfill' for j,s,w,last in candidates)
        for job,site,window,last in candidates:
            if len(self.active)+len(self.orphans)>=MAX_CONNECTIONS:
                break
            if any(j['site']==site['id'] for p,h,j in self.active.values()) or any(j['site']==site['id'] for j in self.orphans):
                continue
            gateway=site.get('gateway') or f"{site['host']}:{site['port']}"
            if any(json.loads(j['snapshot']).get('_gateway')==gateway for p,h,j in self.active.values()) or any(json.loads(j['snapshot']).get('_gateway')==gateway for j in self.orphans):
                continue
            limit=site['recent_budget_seconds']
            if job['kind']=='backfill':
                if recent_waiting or any(j['kind']!='backfill' for p,h,j in self.active.values()):
                    continue
                with self.control.connect() as db:
                    budget=db.execute('SELECT seconds,records FROM budget WHERE site=? AND bucket=?',(site['id'],int(now//3600))).fetchone()
                seconds,records=budget if budget else (0,0)
                if seconds>=site['backfill_budget_seconds'] or records>=site['backfill_record_limit']:
                    continue
                limit=min(site['backfill_slice_seconds'],site['backfill_budget_seconds']-seconds)
                stream=self.control.stream(site['id'],job['tab'])
                if stream['history_done'] or not stream['baseline'] or not site['backfill_enabled']:
                    with self.control.connect() as db:
                        db.execute("UPDATE jobs SET state='cancelled',finished=?,message='History paused, complete or awaiting recent baseline' WHERE id=?",(now,job['id']))
                    continue
            if job.get('automatic'):
                limit=min(limit,job['scheduled_for']+site.get('hourly_attempt_window_minutes',10)*60-now)
            if not site['always_online'] and not job['override_window']:
                limit=min(limit,window['deadline']-now)
            if limit<5:
                continue
            snapshot=json.loads(job['snapshot']);snapshot['_gateway']=gateway
            logdir=self.control.root/'logs';logdir.mkdir(exist_ok=True)
            handle=(logdir/f"job-{job['id']}.log").open('ab')
            with self.control.connect() as db:
                changed=db.execute("UPDATE jobs SET state='running',started=?,deadline=?,snapshot=? WHERE id=? AND state='queued'",(now,now+limit,json.dumps(snapshot),job['id'])).rowcount
            if not changed:
                handle.close();continue
            try:
                proc=self.popen([sys.executable,str(Path(__file__).with_name('worker.py')),'--state',str(self.control.root),'--job',str(job['id'])],stdout=handle,stderr=subprocess.STDOUT)
            except OSError as exc:
                handle.close()
                with self.control.connect() as db:
                    db.execute("UPDATE jobs SET state='failed',finished=?,message=? WHERE id=?",(now,str(exc),job['id']))
                continue
            job.update(started=now,deadline=now+limit,snapshot=json.dumps(snapshot))
            self.active[job['id']]=(proc,handle,job)
            with self.control.connect() as db:
                db.execute('UPDATE jobs SET pid=? WHERE id=?',(proc.pid,job['id']))

    def close(self):
        for proc,handle,job in self.active.values():
            proc.terminate();proc.wait();handle.close()
        self.active.clear()
        with self.control.connect() as db:
            db.execute("UPDATE jobs SET state='interrupted',finished=?,message='Scheduler stopped; checkpoints retained' WHERE state='running'",(time.time(),))
            db.execute('DELETE FROM heartbeat')


def main():
    p=argparse.ArgumentParser();p.add_argument('--state',default='state');a=p.parse_args()
    control=Control(a.state)
    scheduler=Scheduler(control)
    def stop(*args):raise KeyboardInterrupt
    signal.signal(signal.SIGTERM,stop)
    with site_lock(control.root/'scheduler.lock'):
        scheduler.recover()
        try:
            while True:
                scheduler.tick();time.sleep(1)
        except KeyboardInterrupt:
            pass
        finally:
            scheduler.close()


if __name__=='__main__':main()
