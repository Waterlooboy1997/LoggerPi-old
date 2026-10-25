import csv
import json
import math
from pathlib import Path
import tempfile
import unittest
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import patch
import app

REFERENCE=Path(__file__).parent/'reference_data/ROS/ROS_Measurement.dat'


def fixture():
    with REFERENCE.open(newline='') as f:
        r=csv.reader(f)
        headers=[next(r) for _ in range(4)]
    return {'header':headers[0],'fields':[{'name':n,'units':u,'processing':p,'type':'IEEE4B' if n not in ('CR310_BatteryVoltage_Min','CR310_PanelTemp_Max','Temp_C_Avg') else 'FP2'} for n,u,p in zip(headers[1][2:],headers[2][2:],headers[3][2:])],'table_signature':123}


class StoreTests(unittest.TestCase):
    def test_format_ros(self):
        s=fixture()
        with REFERENCE.open() as f:
            self.assertEqual(app.header_lines(s),[next(f).rstrip('\n') for _ in range(4)])
        with Path('tests/fixtures/ros_sample.csv').open(newline='') as f:
            r=next(csv.DictReader(f))
        row={app.text(k):datetime.fromisoformat(v) if k=='Datetime' else int(v) if k=='RecNbr' else float(v) for k,v in r.items()}
        self.assertEqual(app.record_line(row,s),'"2026-10-25 01:59:00",61869,13.06,26.27,0.5371194,0.791,190.34,"NAN","NAN",1,0.1464,0.2681,0.4363,-5050')
        self.assertEqual(app.number(-7999.0,'FP2'),'-7999')
        self.assertEqual(app.number(-8190.0,'FP2'),'"NAN"')
        self.assertEqual(app.number(8191.0,'FP2'),'"INF"')
        self.assertEqual(app.number(-8191.0,'FP2'),'"-INF"')
        self.assertEqual(app.metadata_text(86),'V')
        self.assertEqual(app.number(7,'Long'),'7')
        self.assertEqual(app.number(1.4453125,'IEEE4B'),'1.445313')
        self.assertEqual(app.number(-1.4453125,'IEEE4B'),'-1.445313')
        self.assertEqual(app.text(b'hello'),'hello')
        self.assertEqual(app.number('a,"b','ASCII'),'"a,""b"')

    def test_full_reference_text_roundtrip(self):
        schema=fixture()
        with REFERENCE.open() as handle:
            for _ in range(4):next(handle)
            for line in handle:
                values=next(csv.reader([line]))
                row={'Datetime':values[0],'RecNbr':int(values[1])}
                row.update({f['name']:float(v) for f,v in zip(schema['fields'],values[2:])})
                self.assertEqual(app.record_line(row,schema),line.rstrip('\n'))

    def test_resume_conflict_import_and_export(self):
        schema=fixture()
        row={'Datetime':datetime(2026,1,1),'RecNbr':1,**{f['name']:1.0 for f in schema['fields']}}
        with tempfile.TemporaryDirectory() as d:
            s=app.Store(d)
            gen=s.generation('ROS','Measurement',schema)
            s.accept(gen,[row,row],schema)
            changed=dict(row,waterLevel=2.0)
            s.accept(gen,[changed],schema)
            s.export('ROS','Measurement')
            self.assertEqual(s.db.execute('SELECT COUNT(*) FROM records').fetchone()[0],1)
            self.assertEqual(s.db.execute('SELECT COUNT(*) FROM conflicts').fetchone()[0],1)
            s.db.close()
            s=app.Store(d)
            self.assertEqual(s.cursor(gen),('2026-01-01 00:00:00',1))
            s.export('ROS','Measurement')
            new_schema=json.loads(json.dumps(schema));new_schema['header'][6]='999'
            gen2=s.generation('ROS','Measurement',new_schema)
            s.import_existing(Path(d)/'ROS_Measurement.dat',gen2,new_schema)
            s.export('ROS','Measurement')
            self.assertTrue((Path(d)/f'ROS_Measurement.generation-{gen}.dat').exists())
            s.db.close()

    def test_import_existing_preserves_lines(self):
        schema=fixture()
        with tempfile.TemporaryDirectory() as d:
            path=Path(d)/'ROS_Measurement.dat'
            original=REFERENCE.read_text().splitlines()[:10]
            path.write_text('\n'.join(original)+'\n')
            s=app.Store(d)
            gen=s.generation('ROS','Measurement',schema)
            s.import_existing(path,gen,schema)
            s.export('ROS','Measurement')
            self.assertEqual(path.read_text().splitlines(),original)
            s.db.close()

    def test_import_conflicting_duplicates(self):
        schema=fixture()
        row={'Datetime':datetime(2026,1,1),'RecNbr':1,**{f['name']:1.0 for f in schema['fields']}}
        with tempfile.TemporaryDirectory() as d:
            path=Path(d)/'ROS_Measurement.dat'
            path.write_text('\n'.join(app.header_lines(schema)+[app.record_line(row,schema),app.record_line(dict(row,waterLevel=2.0),schema)])+'\n')
            s=app.Store(d);gen=s.generation('ROS','Measurement',schema)
            s.import_existing(path,gen,schema)
            self.assertEqual(s.db.execute('SELECT COUNT(*) FROM records').fetchone()[0],1)
            self.assertEqual(s.db.execute('SELECT COUNT(*) FROM conflicts').fetchone()[0],1)
            s.db.close()

    def test_failed_batch_rolls_back_records_and_progress(self):
        with tempfile.TemporaryDirectory() as d:
            s=app.Store(d); schema=fixture()
            gen=s.generation('ROS','Measurement',schema)
            row={'Datetime':datetime(2026,1,1),'RecNbr':1,**{f['name']:1.0 for f in schema['fields']}}
            bad=dict(row,RecNbr=2);del bad['waterLevel']
            with self.assertRaises(KeyError):s.accept(gen,[row,bad],schema)
            self.assertIsNone(s.cursor(gen))
            self.assertEqual(s.db.execute('SELECT COUNT(*) FROM records').fetchone()[0],0)
            s.db.close()

    def test_atomic_export_failure_recovers(self):
        with tempfile.TemporaryDirectory() as d:
            s=app.Store(d); schema=fixture()
            gen=s.generation('ROS','Measurement',schema)
            s.export('ROS','Measurement')
            path=Path(d)/'ROS_Measurement.dat'; original=path.read_bytes()
            with patch('app.os.replace',side_effect=OSError('simulated crash')):
                with self.assertRaises(OSError):s.export('ROS','Measurement')
            self.assertEqual(path.read_bytes(),original)
            s.export('ROS','Measurement');s.db.close()


class FakeDevice:
    def __init__(self,batches):
        self.batches=batches
        schema=fixture()
        self.table_def=[{'Header':{'TableName':b'Measurement'},'Signature':123,'Fields':[{'FieldName':f['name'].encode(),'FieldType':f['type'],'Dimension':1,'Units':f['units'].encode(),'Processing':f['processing'].encode()} for f in schema['fields']]}]
        self.pakbus=SimpleNamespace(link=SimpleNamespace(close=lambda:None))
    def gettime(self):return datetime(2026,10,25,2)
    def getprogstat(self):return dict(OSVer=b'CR310.Std.10.07',SerialNbr=b'13437',ProgName=b'CPU:P25_OPUS_ROS_V5.cr300',ProgSig=3640)
    def get_data_generator(self,*a):yield from self.batches
    def bye(self):pass


class CollectionTests(unittest.TestCase):
    def test_overlap_reset_and_identity(self):
        site=app.load_config('settings.json')['sites'][0]
        schema=fixture()
        def row(stamp,rec):return {'Datetime':datetime.fromisoformat(stamp),'RecNbr':rec,**{f['name']:1.0 for f in schema['fields']}}
        a=row('2026-10-25 00:00:00',100)
        b=row('2026-10-25 01:00:00',0)
        with tempfile.TemporaryDirectory() as d:
            factory=lambda *args,**kwargs:FakeDevice([[a],[b]])
            app.collect(site,d,all_history=True,factory=factory)
            app.collect(site,d,all_history=True,factory=factory)
            s=app.Store(d)
            self.assertEqual(s.db.execute('SELECT COUNT(*) FROM generations').fetchone()[0],2)
            self.assertEqual(s.db.execute('SELECT COUNT(*) FROM records').fetchone()[0],2)
            s.db.close()
            older=row('2026-10-24 23:00:00',99)
            newer=row('2026-10-25 01:15:00',1)
            app.collect(site,d,all_history=True,factory=lambda *args,**kwargs:FakeDevice([[older,a,b,newer]]))
            s=app.Store(d)
            self.assertEqual(s.db.execute('SELECT COUNT(*) FROM generations').fetchone()[0],2)
            self.assertEqual(s.db.execute('SELECT COUNT(*) FROM records WHERE gen=1').fetchone()[0],2)
            self.assertEqual(s.db.execute('SELECT COUNT(*) FROM records WHERE gen=2').fetchone()[0],2)
            s.db.close()
            wrong=dict(site,expected_serial='wrong')
            with self.assertRaisesRegex(ValueError,'identity mismatch'):
                app.collect(wrong,d,factory=factory)

    def test_empty_and_interruption_retry(self):
        site=app.load_config('settings.json')['sites'][0]
        with tempfile.TemporaryDirectory() as d:
            with self.assertRaises(OSError):
                app.collect(site,d,factory=lambda *a,**k:(_ for _ in ()).throw(OSError('offline')))
            app.collect(site,d,factory=lambda *a,**k:FakeDevice([]))
            self.assertEqual(len((Path(d)/'ROS_Measurement.dat').read_text().splitlines()),4)

    def test_partial_network_failure_resumes_without_duplicates(self):
        site=app.load_config('settings.json')['sites'][0]
        schema=fixture()
        row={'Datetime':datetime(2026,10,25,1),'RecNbr':1,**{f['name']:1.0 for f in schema['fields']}}
        newer=dict(row,Datetime=datetime(2026,10,25,1,15),RecNbr=2)
        class Interrupted(FakeDevice):
            def get_data_generator(self,*args):
                yield [row]
                raise ConnectionError('simulated mid-download disconnect')
        with tempfile.TemporaryDirectory() as d:
            with self.assertRaises(ConnectionError):app.collect(site,d,factory=lambda *a,**k:Interrupted([]))
            s=app.Store(d)
            self.assertEqual(s.cursor(1),('2026-10-25 01:00:00',1));s.db.close()
            app.collect(site,d,factory=lambda *a,**k:FakeDevice([[row,newer]]))
            s=app.Store(d)
            self.assertEqual(s.db.execute('SELECT COUNT(*) FROM records').fetchone()[0],2)
            self.assertEqual(s.cursor(1),('2026-10-25 01:15:00',2));s.db.close()
            self.assertEqual(len((Path(d)/'ROS_Measurement.dat').read_text().splitlines()),6)

    def test_site_lock(self):
        with tempfile.TemporaryDirectory() as d:
            with app.site_lock(Path(d)/'site.lock'):
                with self.assertRaises(OSError):
                    with app.site_lock(Path(d)/'site.lock'):pass


class SchedulerTests(unittest.TestCase):
    def test_independent_workers_and_timeout(self):
        args=SimpleNamespace(site=None,command='once',config='settings.json',output=None,start=None,end=None,all=False)
        config={'output_directory':'validation','sites':[{'name':'offline','max_run_seconds':1},{'name':'online','max_run_seconds':1}]}
        class Proc:
            def __init__(self,code):self.code=code;self.killed=False
            def poll(self):return self.code
            def kill(self):self.killed=True;self.code=-9
            def wait(self):return self.code
            def terminate(self):self.code=-15
        hung,online=Proc(None),Proc(0)
        with patch('app.subprocess.Popen',side_effect=[hung,online]) as popen, patch('app.time.monotonic',side_effect=[0,0,0,2]),patch('app.time.sleep'):
            self.assertEqual(app.run_workers(args,config),1)
            self.assertEqual(popen.call_count,2)
            self.assertTrue(hung.killed)

    def test_service_retries(self):
        args=SimpleNamespace(site=None,command='serve',config='settings.json',output=None,start=None,end=None,all=False)
        config={'output_directory':'validation','sites':[{'name':'offline','retry_interval_seconds':1}]}
        class Proc:
            def poll(self):return 1
            def terminate(self):pass
            def wait(self):pass
        with patch('app.subprocess.Popen',side_effect=lambda *a,**k:Proc()) as popen,patch('app.time.monotonic',side_effect=[0,0,1,2,3,4,5]),patch('app.time.sleep',side_effect=[None,None,None,None,KeyboardInterrupt]):
            with self.assertRaises(KeyboardInterrupt):app.run_workers(args,config)
            self.assertGreaterEqual(popen.call_count,2)


if __name__=='__main__':unittest.main()


