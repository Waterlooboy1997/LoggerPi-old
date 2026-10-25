import copy
from datetime import datetime
import json
from pathlib import Path
import tempfile
import unittest
from zoneinfo import ZoneInfo
from unittest.mock import patch
from werkzeug.security import generate_password_hash
from control import Control, defaults, availability, backup_preview, issues
from scheduler import Scheduler
from web import create_app
import app as collector
from test_collector import FakeDevice, fixture


def epoch(hour,minute,second=0):
    return datetime(2026,10,25,hour,minute,second,tzinfo=ZoneInfo('Australia/Brisbane')).timestamp()


def site(name='ROS',**kw):
    base=dict(name=name,host='localhost',port=364,pakbus_address=364,expected_serial='13437',enabled=True)
    base.update(kw)
    return defaults(base)


class Base(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.c=Control(self.temp.name)
    def tearDown(self):self.temp.cleanup()
    def config(self,sites):
        conf=self.c.config();conf['sites']=sites
        return self.c.save_config(conf,conf['revision'])


class WindowTests(unittest.TestCase):
    def test_groups_and_guards(self):
        for name in ['ROS','MRM','WFC','BRD','JRD']:self.assertTrue(site(name)['always_online'])
        self.assertEqual(site('FDU')['window_end'],30)
        self.assertEqual(site('BEH')['window_end'],15)
        s=site('BEH')
        self.assertTrue(availability(s,epoch(2,0,0))['open'])
        self.assertTrue(availability(s,epoch(2,0,10))['open'])
        self.assertTrue(availability(s,epoch(2,14,39))['open'])
        self.assertFalse(availability(s,epoch(2,14,40))['open'])
        self.assertEqual(availability(s,epoch(2,20))['next'],epoch(3,0,0))


class ConfigTests(Base):
    def test_revision_drafts_secrets_and_reference_guard(self):
        s=defaults({'name':'UNRESOLVED'})
        saved=self.config([s]);self.assertEqual(saved['revision'],1)
        with self.assertRaisesRegex(ValueError,'changed elsewhere'):self.c.save_config(saved,0)
        conf=copy.deepcopy(saved);conf['sites'][0]['enabled']=True
        with self.assertRaises(ValueError):self.c.save_config(conf,1)
        conf=copy.deepcopy(saved);conf['output_directory']='reference_data'
        with self.assertRaisesRegex(ValueError,'reference_data'):self.c.save_config(conf,1)
        self.assertTrue(self.c.status()['issues']['UNRESOLVED'])

    def test_independent_cursors(self):
        self.config([site()]);j=self.c.enqueue(site(),'recent','Measurement')
        job=next(jb for jb in self.c.jobs() if jb['id']==j)
        self.c.checkpoint(job,baseline='2026-10-24 00:00:00')
        self.c.checkpoint(job,stamp='2026-10-25 00:00:00',count=3)
        h=self.c.enqueue(site(),'backfill','Measurement');history=next(jb for jb in self.c.jobs() if jb['id']==h)
        self.c.checkpoint(history,stamp='2024-01-01 00:00:00',count=2)
        stream=self.c.stream('ROS','Measurement')
        self.assertEqual(stream['recent'],'2026-10-25 00:00:00')
        self.assertEqual(stream['history'],'2024-01-01 00:00:00')
        self.assertFalse(stream['history_done'])

    def test_backup_as_data(self):
        xml=b'''<?xml version="1.0"?><lgrnet-config><devices><device level="0" name="port"><setting host="test" port="364"/></device><device level="1" name="ROS"><class-bmp5-datalogger/><collect-area name="Measurement"><setting id="2">1</setting></collect-area></device></devices></lgrnet-config>'''
        s=backup_preview(b'archive'+xml,Path(self.temp.name))[0]
        self.assertFalse(s['enabled']);self.assertEqual(s['port'],364)
        self.assertIsNone(s['pakbus_address'])
        self.assertTrue(s['always_online'])
        with self.assertRaises(ValueError):backup_preview(b'bad',self.temp.name)


class FakeProc:
    next_pid=100
    def __init__(self,*a,**kw):
        self.code=None;self.pid=FakeProc.next_pid;FakeProc.next_pid+=1
    def poll(self):return self.code
    def kill(self):self.code=-9
    def terminate(self):self.code=-15
    def wait(self):return self.code


class SchedulerTests(Base):
    def make(self,sites):
        self.config(sites)
        return Scheduler(self.c,popen=FakeProc)
    def test_capacity_deadline_and_cancel(self):
        sch=self.make([site('BEH'),site('BSL',port=365),site('URO',port=366)])
        now=epoch(2,1)
        with patch('control.time.time',return_value=now):sch.tick(now)
        self.assertEqual(len(sch.active),3)
        self.assertTrue(all(j['deadline']<=now+60 for p,h,j in sch.active.values()))
        sch.tick(now+61)
        self.assertTrue(any(j['state']=='limited' for j in self.c.jobs()))
        sch.close()
    def test_window_queue_and_shared_gateway(self):
        sch=self.make([site('BEH',gateway='radio'),site('BSL',gateway='radio')])
        j=self.c.enqueue(site('BEH',gateway='radio'),'test')
        sch.tick(epoch(2,30));self.assertEqual(len(sch.active),0)
        with patch('control.time.time',return_value=epoch(3,1)):sch.tick(epoch(3,1))
        self.assertEqual(len(sch.active),1)
        sch.close()
    def test_recent_before_history_and_budget(self):
        s=site(backfill_enabled=True);sch=self.make([s])
        recent=self.c.enqueue(s,'recent','Measurement')
        job=next(j for j in self.c.jobs() if j['id']==recent)
        self.c.checkpoint(job,baseline='2026-10-24 00:00:00')
        self.c.checkpoint(job,complete=True,coverage='2026-10-25 00:00:00')
        self.c.enqueue(s,'backfill','Measurement')
        now=epoch(2,1)
        with patch('control.time.time',return_value=now):sch.tick(now)
        self.assertEqual(next(iter(sch.active.values()))[2]['kind'],'recent')
        sch.close()
    def test_history_hourly_budget_and_restart_reservation(self):
        s=site(backfill_enabled=True);sch=self.make([s]);now=epoch(2,1)
        j=self.c.enqueue(s,'recent','Measurement',automatic=True,scheduled_for=epoch(2,0));job=next(jb for jb in self.c.jobs() if jb['id']==j)
        self.c.checkpoint(job,baseline='2026-10-24 00:00:00')
        self.c.checkpoint(job,complete=True,coverage='2026-10-25 00:00:00')
        with self.c.connect() as db:
            db.execute("UPDATE jobs SET state='success',finished=? WHERE id=?",(now,j))
            db.execute('INSERT INTO budget VALUES(?,?,90,0)',('ROS',int(now//3600)))
        self.c.enqueue(s,'backfill','Measurement')
        sch.tick(now);self.assertEqual(len(sch.active),0)
        with self.c.connect() as db:
            db.execute("UPDATE jobs SET state='running',started=?,deadline=? WHERE kind='backfill'",(now,now+45))
        with patch('scheduler.time.time',return_value=now):sch.recover()
        self.assertEqual(len(sch.orphans),1)
        sch.tick(now+1);self.assertEqual(len(sch.active),0)
        sch.close()

    def test_cancel_and_window_closing_deadline(self):
        s=site('BEH',window_end=5);sch=self.make([s]);now=epoch(2,4,20)
        with patch('control.time.time',return_value=now):sch.tick(now)
        job=next(iter(sch.active.values()))[2]
        self.assertEqual(job['deadline'],epoch(2,4,40))
        with self.c.connect() as db:db.execute('UPDATE jobs SET cancel=1 WHERE id=?',(job['id'],))
        sch.tick(now+1)
        self.assertEqual(self.c.jobs()[0]['state'],'cancelled')
        sch.close()

    def test_twenty_sites_progress_without_starvation(self):
        sites=[site('SITE'+str(i),port=1000+i) for i in range(20)]
        sch=self.make(sites);now=epoch(2,1)
        with patch('control.time.time',return_value=now):sch.tick(now)
        first={j['site'] for p,h,j in sch.active.values()}
        self.assertEqual(len(first),4)
        for p,h,j in sch.active.values():p.code=0
        with patch('control.time.time',return_value=now+1):sch.tick(now+1)
        second={j['site'] for p,h,j in sch.active.values()}
        self.assertEqual(len(second),4)
        self.assertFalse(first & second)
        self.assertEqual(len([j for j in self.c.jobs() if j['state']=='queued']),12)
        sch.close()

    def test_failure_retry_limit(self):
        s=site('BEH');sch=self.make([s])
        for minute in range(10):
            now=epoch(2,minute)
            with patch('control.time.time',return_value=now):sch.tick(now)
            self.assertEqual(len(sch.active),1)
            next(iter(sch.active.values()))[0].code=1
            sch.tick(now+1)
            sch.tick(now+59)
            self.assertEqual(len(sch.active),0)
        sch.tick(epoch(2,10));self.assertEqual(len(sch.active),0)
        self.assertEqual(len([j for j in self.c.jobs() if j['state']=='failed']),10)
        with patch('control.time.time',return_value=epoch(3,0)):sch.tick(epoch(3,0))
        self.assertEqual(len(sch.active),1)
        sch.close()

    def test_success_does_not_drift_off_the_hour(self):
        sch=self.make([site()])
        with patch('control.time.time',return_value=epoch(2,0)):sch.tick(epoch(2,0))
        next(iter(sch.active.values()))[0].code=0
        sch.tick(epoch(2,0,45))
        sch.tick(epoch(2,9));self.assertEqual(len(sch.active),0)
        sch.tick(epoch(2,20));self.assertEqual(len(sch.active),0)
        with patch('control.time.time',return_value=epoch(3,0)):sch.tick(epoch(3,0))
        self.assertEqual(len(sch.active),1)
        sch.close()

    def test_manual_connection_outside_hour_and_when_routine_disabled(self):
        s=site('BEH',enabled=False);sch=self.make([s])
        self.c.enqueue(s,'test',override=True)
        sch.tick(epoch(2,35))
        self.assertEqual(len(sch.active),1)
        self.assertEqual(next(iter(sch.active.values()))[2]['kind'],'test')
        sch.close()

    def test_four_connection_slots_and_expired_hourly_requests(self):
        sites=[site('SITE'+str(i),port=1000+i) for i in range(5)]
        sch=self.make(sites);conf=self.c.config();conf['max_connections']=4;self.c.save_config(conf,conf['revision'])
        now=epoch(2,9,40)
        with patch('control.time.time',return_value=now):sch.tick(now)
        self.assertEqual(len(sch.active),4)
        self.assertTrue(all(j['deadline']==epoch(2,10) for p,h,j in sch.active.values()))
        sch.tick(epoch(2,10))
        self.assertEqual(len(sch.active),0)
        self.assertTrue(any(j['state']=='skipped' for j in self.c.jobs()))
        sch.close()


class CollectionBoundsTests(unittest.TestCase):
    def test_history_slice_is_one_day_from_first_available_record(self):
        s=site();schema=fixture()
        rows=[{'Datetime':datetime(2024,1,day),'RecNbr':day,**{f['name']:1.0 for f in schema['fields']}} for day in [1,2,3]]
        events=[]
        with tempfile.TemporaryDirectory() as d:
            collector.collect(s,d,start=datetime(1990,1,1),end=datetime(2026,1,1),limits={'history_seek':True,'fresh_only':True,'max_records':1000},checkpoint=lambda **kw:events.append(kw),factory=lambda *a,**k:FakeDevice([rows]))
            store=collector.Store(d);self.assertEqual(store.db.execute('SELECT COUNT(*) FROM records').fetchone()[0],2);store.db.close()
        self.assertTrue(events[-1]['complete'])
        self.assertEqual(events[-1]['coverage'],'2024-01-02 00:00:00')

    def test_cap_preserves_separate_callback_checkpoint(self):
        s=site();schema=fixture();rows=[{'Datetime':datetime(2026,10,25,1,i),'RecNbr':i,**{f['name']:1.0 for f in schema['fields']}} for i in range(3)]
        events=[]
        with tempfile.TemporaryDirectory() as d:
            collector.collect(s,d,start=datetime(2026,10,25),limits={'max_records':2,'fresh_only':True},checkpoint=lambda **kw:events.append(kw),factory=lambda *a,**k:FakeDevice([rows]))
            store=collector.Store(d);self.assertEqual(store.db.execute('SELECT COUNT(*) FROM records').fetchone()[0],2);store.db.close()
        self.assertEqual(sum(e.get('count',0) for e in events),2)
        self.assertFalse(events[-1]['complete'])


class WebTests(Base):
    def setUp(self):
        super().setUp();self.config([site(security_code=1234)])
        self.web=create_app(self.temp.name);self.web.testing=True;self.client=self.web.test_client()
    def auth(self):
        self.c.set('password_hash',generate_password_hash('test password 123'))
        with self.client.session_transaction() as s:s.update(authenticated=True,csrf='test-token')
        return {'X-CSRF-Token':'test-token'}
    def test_setup_and_login_csrf(self):
        self.assertEqual(self.client.get('/api/status').status_code,401)
        self.client.get('/setup')
        with self.client.session_transaction() as s:csrf=s['csrf']
        token=(self.c.root/'setup-code.txt').read_text().strip()
        result=self.client.post('/setup',data={'csrf':csrf,'token':token,'password':'long secure password'})
        self.assertEqual(result.status_code,302)
        self.assertFalse((self.c.root/'setup-code.txt').exists())
        self.assertEqual(self.client.get('/api/status').status_code,200)
        self.assertEqual(self.client.post('/logout').status_code,403)
    def test_edit_secrets_conflicts_and_jobs(self):
        headers=self.auth();data=self.client.get('/api/status').get_json()['config']
        self.assertNotIn('security_code',data['sites'][0])
        self.assertNotIn('1234',self.client.get('/api/config/export').get_data(as_text=True))
        data['sites'][0]['label']='New label'
        self.assertEqual(self.client.put('/api/config',json=data,headers=headers).status_code,200)
        self.assertEqual(self.c.config()['sites'][0]['security_code'],1234)
        self.assertEqual(self.client.put('/api/config',json=data,headers=headers).status_code,400)
        self.assertEqual(self.client.post('/api/sites/ROS/jobs',json={'kind':'recent'},headers=headers).status_code,202)
        self.assertEqual(self.client.get('/').status_code,200)
        self.assertIn(b'Last record',self.client.get('/').data)
        self.assertNotIn(b'Workspace settings',self.client.get('/').data)
    def test_revision_restore_keeps_secrets_server_side(self):
        headers=self.auth();conf=self.c.config();conf['sites'][0]['security_code']=9999
        self.c.save_config(conf,conf['revision'])
        response=self.client.post('/api/revisions/1/restore',json={'revision':2},headers=headers)
        self.assertEqual(response.status_code,200)
        self.assertEqual(self.c.config()['sites'][0]['security_code'],1234)
        self.assertNotIn('security_code',response.get_json()['sites'][0])

    def test_remove_retains_data(self):
        headers=self.auth();marker=self.c.root/'collected';marker.mkdir();(marker/'data.dat').write_text('retain')
        conf=self.client.get('/api/status').get_json()['config'];conf['sites']=[]
        self.assertEqual(self.client.put('/api/config',json=conf,headers=headers).status_code,200)
        self.assertEqual((marker/'data.dat').read_text(),'retain')


if __name__=='__main__':unittest.main()
