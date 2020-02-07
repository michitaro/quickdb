import time
import unittest

from quickdb.test_config import REPO_DIR

from . import jsonnpy
from .sqlserver import app


@unittest.skipUnless(REPO_DIR, 'REPO_DIR is not set')
class TestSqlServer(unittest.TestCase):
    def test_create_jobs(self):
        client = app.test_client()
        data = jsonnpy.dumps({
            'sql': 'SELECT COUNT(*) FROM pdr2_dud',
        })
        res = client.post('/jobs', data=data, headers={"Content-type": "application/hscssp-jsonnpy"})
        self.assertEqual(res.status_code, 200)
        res_data = jsonnpy.loads(res.data)
        self.assertEqual(res_data['status'], 'done')
        self.assertIsNotNone(res_data['result'])

    def test_sql_error(self):
        client = app.test_client()
        data = jsonnpy.dumps({
            'sql': 'SELECT no_such_function(*) FROM pdr2_dud LIMIT 10',
        })
        res = client.post('/jobs', data=data, headers={"Content-type": "application/hscssp-jsonnpy"})
        self.assertEqual(res.status_code, 200)
        res_data = jsonnpy.loads(res.data)
        self.assertEqual(res_data['status'], 'error')
        self.assertRegex(res_data['reason'], '.*Unknown function:')

    def test_create_deferred(self):
        client = app.test_client()
        data = jsonnpy.dumps({
            'sql': 'SELECT sleep(0.5) FROM pdr2_dud',
            'deferred': True,
        })
        res = client.post('/jobs', data=data, headers={"Content-type": "application/hscssp-jsonnpy"})
        self.assertEqual(res.status_code, 200)
        res_data = jsonnpy.loads(res.data)
        job_id = res_data['job_id']

        res = client.get(f'/jobs/{job_id}', data=data, headers={"Content-type": "application/hscssp-jsonnpy"})
        res_data = jsonnpy.loads(res.data)
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res_data['status'], 'running')

        time.sleep(1)
        res = client.get(f'/jobs/{job_id}', data=data, headers={"Content-type": "application/hscssp-jsonnpy"})
        res_data = jsonnpy.loads(res.data)
        self.assertEqual(res.status_code, 200)
        res_data = jsonnpy.loads(res.data)
        self.assertEqual(res_data['status'], 'done')
