import json
from pathlib import Path
import tempfile
import unittest
from datetime import datetime
from unittest.mock import patch
import app
from control import Control
from test_admin import Base,site
from test_collector import FakeDevice,fixture
from site_status import short_error


class CompactStatusTests(Base):
    def setUp(self):
        super().setUp();self.config([site(tables=['Public','Measurement'])])

    def job(self,table,state,finished):
        identifier=self.c.enqueue(site(tables=['Public','Measurement']),'recent',table)
        with self.c.connect() as db:
            db.execute('UPDATE jobs SET state=?,finished=?,message=? WHERE id=?',(state,finished,'No logger response' if state=='failed' else '',identifier))
        return identifier

    def test_actual_record_is_not_coverage_or_public_timestamp(self):
        schema=fixture();store=app.Store(self.c.config()['output_directory'])
        gen=store.generation('ROS','Measurement',schema)
        row={'Datetime':datetime(2026,10,24,12),'RecNbr':25,**{f['name']:1 for f in schema['fields']}}
        store.accept(gen,[row],schema);store.db.close()
        identifier=self.job('Measurement','success',100)
        job=next(j for j in self.c.jobs() if j['id']==identifier)
        self.c.checkpoint(job,complete=True,coverage='2026-10-25 23:00:00')
        self.job('Public','success',200)
        status=self.c.status()['sites']['ROS']
        self.assertEqual(status['last_record'],'2026-10-24 12:00:00')
        self.assertEqual(status['record_number'],25)
        self.assertEqual(status['last_success'],100)
        self.assertEqual(status['primary_table'],'Measurement')

    def test_primary_fallback_missing_records_and_partial_failure(self):
        self.job('Public','success',100);self.job('Measurement','failed',110)
        status=self.c.status()['sites']['ROS']
        self.assertEqual(status['status'],'Partial failure')
        self.assertTrue(status['needs_attention']);self.assertIsNone(status['last_record'])
        conf=self.c.config();conf['sites'][0]['tables']=['Public'];self.c.save_config(conf,conf['revision'])
        status=self.c.status()['sites']['ROS']
        self.assertEqual(status['primary_table'],'Public');self.assertEqual(status['last_success'],100)

    def test_stages_reported_by_worker_and_cannot_change_finished_jobs(self):
        identifier=self.c.enqueue(site(tables=['Public','Measurement']),'test')
        with self.c.connect() as db:db.execute("UPDATE jobs SET state='running' WHERE id=?",(identifier,))
        for phase in ['Connecting','Connected','Downloading']:
            self.c.progress(identifier,phase)
            self.assertEqual(self.c.status()['sites']['ROS']['status'],phase)
        with self.c.connect() as db:db.execute("UPDATE jobs SET state='success' WHERE id=?",(identifier,))
        self.c.progress(identifier,'Failed','late callback')
        self.assertEqual(next(j for j in self.c.jobs() if j['id']==identifier)['phase'],'Downloading')

    def test_summary_success_survives_limited_visible_activity(self):
        self.job('Measurement','success',123)
        with self.c.connect() as db:
            for i in range(105):db.execute("INSERT INTO jobs(site,kind,tab,state,created,finished,snapshot) VALUES('OTHER','recent','Measurement','success',1,2,'{}')")
        state=self.c.status()
        self.assertEqual(len(state['jobs']),100)
        self.assertEqual(state['sites']['ROS']['last_success'],123)
        self.assertEqual(state['sites']['ROS']['status'],'OK')

    def test_capacity_cannot_be_overridden(self):
        conf=self.c.config();conf['max_connections']=8
        with self.assertRaisesRegex(ValueError,'fixed at 4'):self.c.save_config(conf,conf['revision'])
        with self.c.connect() as db:
            altered=self.c.config();altered['max_connections']=1
            db.execute("UPDATE kv SET value=? WHERE key='config'",(json.dumps(altered),))
        self.assertEqual(self.c.config()['max_connections'],4)

    def test_short_errors(self):
        self.assertEqual(short_error(KeyError('RecData')),'No response from logger')
        self.assertEqual(short_error('Logger identity mismatch: expected serial 3'),'Logger serial does not match')


class WorkerPhaseTests(unittest.TestCase):
    def test_real_collection_hooks_are_ordered(self):
        phases=[]
        with tempfile.TemporaryDirectory() as root:
            app.collect(site(),root,factory=lambda *a,**k:FakeDevice([]),progress=phases.append)
        self.assertEqual(phases,['Connecting','Connected','Downloading'])
        phases=[]
        with tempfile.TemporaryDirectory() as root:
            with self.assertRaises(OSError):
                app.collect(site(),root,factory=lambda *a,**k:(_ for _ in ()).throw(OSError('offline')),progress=phases.append)
        self.assertEqual(phases,['Connecting'])


if __name__=='__main__':unittest.main()
