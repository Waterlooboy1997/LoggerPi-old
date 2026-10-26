"""Hourly queue supervisor. No network collection runs inside the web app."""
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
from control import HISTORY_MAX_SECONDS, MAX_CONNECTIONS, Control, routine_schedule, issues

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
            message='Cancelled; committed records retained' if cancel else 'Time limit reached; committed records retained' if expired else 'Completed' if code==0 else worker_message or 'Connection or collection failed; see job log'
            state='cancelled' if cancel else 'limited' if expired else 'success' if code==0 else 'failed'
            if code==CONNECTION_TIMEOUT_EXIT and not cancel:
                state='failed'
                message='Connection failed after 20 seconds'
            with self.control.connect() as db:
                db.execute('UPDATE jobs SET state=?,finished=?,message=? WHERE id=?',(state,now,message,identifier))
            del self.active[identifier]
        for site in sites.values():
            if not site['enabled'] or issues(site):
                continue
            routine=routine_schedule(site,now)
            for table in site['tables']:
                with self.control.connect() as db:
                    pending=bool(db.execute("SELECT 1 FROM jobs WHERE site=? AND tab=? AND kind='recent' AND state IN ('queued','running') LIMIT 1",(site['id'],table)).fetchone())
                    attempts=[dict(r) for r in db.execute("SELECT * FROM jobs WHERE site=? AND tab=? AND kind='recent' AND automatic=1 AND scheduled_for=? ORDER BY id",(site['id'],table,routine['cycle']))]
                if pending:
                    continue
                last=attempts[-1] if attempts else None
                if last is None:
                    # After a restart or enabling mid-hour, only start if this hour's attempts would still be running.
                    due=now<routine['cycle']+site['hourly_attempts']*site['retry_interval_seconds']
                elif last['state'] in ('failed','limited','interrupted'):
                    # The first attempt is on the hour; each failure waits the configured spacing.
                    failures=sum(j['state'] in ('failed','limited','interrupted') for j in attempts)
                    due=failures<site['hourly_attempts'] and now>=(last['finished'] or now)+site['retry_interval_seconds']
                else:
                    due=False
                if due:
                    self.control.enqueue(site,'recent',table,automatic=True,scheduled_for=routine['cycle'])
        queued=[j for j in self.control.jobs(10000) if j['state']=='queued']
        candidates=[]
        for job in queued:
            site=sites.get(job['site'])
            if not site or (not site['enabled'] and job.get('automatic')):
                continue
            if job.get('automatic') and now>=job['scheduled_for']+3600:
                with self.control.connect() as db:
                    db.execute("UPDATE jobs SET state='skipped',finished=?,message='Hour ended before a connection was free; next attempt at the next hour' WHERE id=?",(now,job['id']))
                continue
            # Manual first, then automatic, then history; oldest successful collection, then FIFO.
            with self.control.connect() as db:
                success=db.execute("SELECT finished FROM jobs WHERE site=? AND state='success' AND kind='recent' ORDER BY finished DESC LIMIT 1",(site['id'],)).fetchone()
            candidates.append((job,site,success[0] if success else 0))
        candidates.sort(key=lambda item:(2 if item[0]['kind']=='backfill' else 1 if item[0].get('automatic') else 0,item[2],item[0]['created']))
        for job,site,last in candidates:
            if len(self.active)+len(self.orphans)>=MAX_CONNECTIONS:
                break
            if any(j['site']==site['id'] for p,h,j in self.active.values()) or any(j['site']==site['id'] for j in self.orphans):
                continue
            gateway=site.get('gateway') or f"{site['host']}:{site['port']}"
            if any(json.loads(j['snapshot']).get('_gateway')==gateway for p,h,j in self.active.values()) or any(json.loads(j['snapshot']).get('_gateway')==gateway for j in self.orphans):
                continue
            limit=site['recent_budget_seconds']
            if job['kind']=='backfill':
                # Manual history download: runs until finished or stopped.
                limit=HISTORY_MAX_SECONDS
                if self.control.stream(site['id'],job['tab'])['history_done']:
                    with self.control.connect() as db:
                        db.execute("UPDATE jobs SET state='success',finished=?,message='History already downloaded' WHERE id=?",(now,job['id']))
                    continue
            if job.get('automatic'):
                limit=min(limit,job['scheduled_for']+3600-now)
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
