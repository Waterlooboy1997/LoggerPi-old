import time
import unittest
from unittest.mock import Mock,patch
from connection import SingleAttemptCR1000,CONNECTION_TIMEOUT_EXIT
from test_admin import Base,site,FakeProc,epoch
from scheduler import Scheduler
import worker


class HandshakeTests(unittest.TestCase):
    def test_only_one_hello_and_no_reconnect_after_unrelated_frames(self):
        link=Mock();protocol=Mock()
        protocol.get_hello_cmd.return_value=(b'hello',9)
        protocol.wait_packet.side_effect=[None,({},{}),({'source':364},{'reply':True})]
        with patch('connection.PakBus',return_value=protocol),patch.object(SingleAttemptCR1000,'bye'):
            device=SingleAttemptCR1000(link,dest=364)
            self.assertTrue(device.connected)
            link.open.assert_called_once();protocol.write.assert_called_once_with(b'hello')
            self.assertEqual(protocol.wait_packet.call_count,3)
            device.connected=False

    def test_deadline_closes_without_reconnecting(self):
        link=Mock();protocol=Mock();protocol.get_hello_cmd.return_value=(b'hello',9)
        protocol.wait_packet.return_value=({}, {})
        with patch('connection.PakBus',return_value=protocol),patch('connection.time.monotonic',side_effect=[0,1,21]):
            with self.assertRaises(TimeoutError):SingleAttemptCR1000(link,dest=364)
        link.open.assert_called_once();link.close.assert_called_once();protocol.write.assert_called_once()


class WatchdogTests(Base):
    def test_cancel_connection_timer_after_connected(self):
        self.config([site()]);identifier=self.c.enqueue(site(),'test')
        with self.c.connect() as db:
            db.execute("UPDATE jobs SET state='running',deadline=? WHERE id=?",(time.time()+60,identifier))
        overall,connection=Mock(),Mock()
        def connected(*args,**kwargs):
            kwargs['progress']('Connecting');kwargs['progress']('Connected');kwargs['progress']('Downloading')
        with patch('worker.threading.Timer',side_effect=[overall,connection]) as timer,patch('worker.app.collect',side_effect=connected):
            worker.run(str(self.c.root),identifier)
        self.assertEqual(timer.call_args_list[1].args[0],20)
        connection.start.assert_called_once();connection.cancel.assert_called();overall.cancel.assert_called_once()

    def test_connection_timeout_is_failed_not_a_download_limit(self):
        self.config([site()]);scheduler=Scheduler(self.c,popen=FakeProc)
        identifier=self.c.enqueue(site(),'test',override=True)
        now=epoch(2,30)
        scheduler.tick(now)
        self.assertEqual(len(scheduler.active),1)
        next(iter(scheduler.active.values()))[0].code=CONNECTION_TIMEOUT_EXIT
        scheduler.tick(now+20)
        job=next(j for j in self.c.jobs() if j['id']==identifier)
        self.assertEqual(job['state'],'failed')
        self.assertEqual(job['message'],'Connection failed after 20 seconds')
        self.assertFalse(any(j['state'] in ('queued','running') for j in self.c.jobs()))
        self.assertEqual(self.c.status()['sites']['ROS']['status'],'Failed')
        scheduler.close()


if __name__=='__main__':unittest.main()
