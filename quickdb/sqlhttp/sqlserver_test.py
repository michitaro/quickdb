from io import BytesIO
import os
import time
import unittest

from quickdb.test_config import REPO_DIR

from . import jsonnpy
from .sqlserver import app, jsonnpy_response


class TestSqlServer(unittest.TestCase):
    def _sync_job(self, sql: str):
        client = app.test_client()
        data = jsonnpy.dumps({'sql': sql})
        res = client.post('/jobs', data=data, headers={"Content-type": "application/hscssp-jsonnpy"})
        self.assertEqual(res.status_code, 200)
        res_data = jsonnpy.loads(res.data)
        return res_data

    def test_create_jobs(self):
        sql = 'SELECT COUNT(*) FROM pdr2_dud'
        res_data = self._sync_job(sql)
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
        self.assertRegex(res_data['reason'], '.*No such function:')

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

    def test_stream_query(self):
        answer = self._sync_job('SELECT COUNT(*) FROM pdr2_dud')['result']['target_list'][1][0]
        client = app.test_client()
        data = jsonnpy.dumps({
            'sql': 'SELECT object_id FROM pdr2_dud',
            'streaming': True,
        })
        res = client.post('/jobs', data=data, headers={"Content-type": "application/hscssp-jsonnpy"})
        self.assertEqual(res.status_code, 200)
        stream = BytesIO(res.data)
        count = 0
        while True:
            chunk = jsonnpy.load(stream)
            if chunk['type'] == 'end':
                break
            assert chunk['type'] == 'progress'
            count += len(chunk['progress']['data'][0][0])
        self.assertEqual(count, answer)
