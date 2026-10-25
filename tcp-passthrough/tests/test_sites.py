"""Site metadata persistence and dashboard API integration."""
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import re
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from shared_state import SharedState
from site_config import SiteConfig
from web_dashboard import create_app


class SiteTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / 'sites.json'
        self.sites = SiteConfig(self.path)

    def app(self):
        ports = [300, 305, 310, 361, 6785]
        return create_app(SimpleNamespace(clients=dict.fromkeys(ports)), SharedState(ports),
                          SimpleNamespace(snapshot=lambda: {}), ['127.0.0.0/8'], self.sites)

    def test_seed_assignments(self):
        expected = {305:'UTU',308:'BSL',310:'HUL',319:'BUOY319',350:'FDF',351:'JRD',
                    352:'MBR',353:'BRD',354:'FDU',355:'WCZ',356:'UBD',357:'DIX',358:'WFC',
                    359:'BSU',360:'URO',361:'Free',362:'BEH',363:'MRM',364:'ROS',365:'LDC1',
                    366:'LDC2',367:'LDC3',368:'Porphyry',369:'Grey',370:'Broken',623:'LDW',
                    721:'BUOY721',724:'BUOY724'}
        self.assertEqual(self.sites.names(), expected)
        entries = json.loads(self.path.read_text())
        for e in entries:
            if e['callback_port'] is not None:
                port = e['callback_port']
                self.assertEqual(e['pakbus'], 305 if port == 310 else 1 if port in (368,369,370) else port)
        umr = next(e for e in entries if e['site'] == 'UMR')
        self.assertIsNone(umr['callback_port'])
        self.assertIn('digiscapegbrmathewsroad.dyndns.org:6785', umr['note'])

    def test_rename_clear_and_restart(self):
        self.sites.set_site(310, '  New site  ')
        restored = SiteConfig(self.path)
        self.assertEqual(restored.names()[310], 'New site')
        self.assertEqual(next(e for e in restored.entries if e['callback_port'] == 310)['pakbus'], 305)
        restored.set_site(310, '')
        restored.set_site(300, 'New assignment')
        self.assertEqual(SiteConfig(self.path).names()[310], '')
        self.assertEqual(SiteConfig(self.path).names()[300], 'New assignment')
        self.assertEqual(next(e for e in restored.entries if e['site'] == 'UMR')['callback_port'], None)

    def test_concurrent_saves_preserve_both_assignments(self):
        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(lambda item: self.sites.set_site(*item),
                                        [(305, 'Changed UTU'), (310, 'Changed HUL')]))
        self.assertEqual(results, ['Changed UTU', 'Changed HUL'])
        restored = SiteConfig(self.path)
        self.assertEqual(restored.names()[305], 'Changed UTU')
        self.assertEqual(restored.names()[310], 'Changed HUL')

    def test_failed_write_keeps_previous_file_and_memory(self):
        previous = self.path.read_bytes()
        with patch('site_config.os.replace', side_effect=PermissionError('read-only')):
            with self.assertRaises(OSError):
                self.sites.set_site(310, 'Changed')
        self.assertEqual(self.path.read_bytes(), previous)
        self.assertEqual(self.sites.names()[310], 'HUL')
        self.assertEqual(list(self.path.parent.glob('.sites-*')), [])

    def test_invalid_files_not_overwritten(self):
        for content in ('{broken', '{}', '[{"site": "X", "callback_port": true}]',
                        '[{"site":"X","callback_port":310},{"site":"Y","callback_port":310}]'):
            self.path.write_text(content)
            with self.assertRaisesRegex(ValueError, 'Cannot load site configuration'):
                SiteConfig(self.path)
            self.assertEqual(self.path.read_text(), content)

    def test_api_persistence_validation_and_security(self):
        client = self.app().test_client()
        page = client.get('/').text
        token = re.search(r'name="control-token" content="([^"]+)"', page)[1]
        headers = {'X-Control-Token': token}
        endpoint = '/api/ports/310/site'
        self.assertEqual(client.post(endpoint, json={'site':'Other'}).status_code, 403)
        self.assertEqual(client.post(endpoint, headers={**headers, 'Origin':'https://other.example'}, json={'site':'Other'}).status_code, 403)
        self.assertEqual(client.post(endpoint, headers=headers, json={'site':'Other'}, environ_overrides={'REMOTE_ADDR':'192.0.2.1'}).status_code, 403)
        self.assertEqual(client.post('/api/ports/999/site', headers=headers, json={'site':'X'}).status_code, 404)
        for value in (None, 42, [], 'x'*101, 'bad\nname', '\x7fname'):
            self.assertEqual(client.post(endpoint, headers=headers, json={'site':value}).status_code, 400)
        for payload in ({}, [], None):
            self.assertEqual(client.post(endpoint, headers=headers, json=payload).status_code, 400)
        response = client.post(endpoint, headers=headers, json={'site':' <New & site> '})
        self.assertEqual(response.json['site'], '<New & site>')
        self.assertEqual(client.get('/api/ports/310').json['site'], '<New & site>')
        status = {p['port']:p for p in client.get('/api/status').json['ports']}
        self.assertEqual(status[310]['site'], '<New & site>')
        self.assertEqual(status[361]['site'], 'Free')
        self.assertEqual(status[6785]['site'], '')
        self.sites = SiteConfig(self.path)
        self.assertEqual(self.app().test_client().get('/api/ports/310').json['site'], '<New & site>')

    def test_api_save_failure(self):
        client = self.app().test_client()
        token = re.search(r'name="control-token" content="([^"]+)"', client.get('/').text)[1]
        with patch.object(self.sites, '_write', side_effect=OSError('disk full')):
            with self.assertLogs('dashboard', level='ERROR'):
                response = client.post('/api/ports/310/site', headers={'X-Control-Token':token}, json={'site':'Failed'})
        self.assertEqual(response.status_code, 500)
        self.assertEqual(client.get('/api/ports/310').json['site'], 'HUL')
