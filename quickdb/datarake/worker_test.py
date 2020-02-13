from contextlib import contextmanager
from functools import lru_cache, reduce
import os
import pickle
from quickdb.utils.evaluate import evaluate
from typing import Dict
from quickdb.datarake.interface import Progress, ProgressCB
import socket
import socketserver
import threading
import unittest

from quickdb.datarake.auth import AuthError, knock
from quickdb.sspcatalog.errors import UserError
from quickdb.test_config import REPO_DIR

from . import api
from . import config
from . import worker

SOCK_FILE = './test.sock'


@unittest.skipUnless(REPO_DIR, 'REPO_DIR is not set')
class ConfigSetting(unittest.TestCase):
    def setUp(self):
        super().setUp()
        config.tasks, self._tasks = get_tasks, config.tasks

    def tearDown(self):
        super().tearDown()
        config.tasks, self._tasks = get_tasks, config.tasks


class TestWorker(ConfigSetting):
    def test_process_request(self):
        history = []

        def progress(p: Progress):
            history.append(p.done)

        make_env = '''
        def mapper(patch):
            return patch.size

        def reducer(a, b):
            return a + b

        '''
        job = worker.Job(api.WorkerRequest(make_env, {}))

        self.assertEqual(
            job.run(progress),
            process_request_simple(make_env, {}, progress),
        )
        self.assertGreater(len(history), 0)
        self.assertTrue(
            all(history[i + 1] - history[i] > 0 for i in range(len(history) - 1))
        )

    def test_process_request_exception(self):
        make_env = '''
        def mapper(patch):
            return patch('no.such.column')

        def reducer(a, b):
            return a + b

        '''
        with self.assertRaises(UserError):
            worker.Job(api.WorkerRequest(make_env, {})).run()


class ServerTest(unittest.TestCase):
    def _cleanup(self):
        try:
            os.unlink(SOCK_FILE)
        except:
            pass

    def setUp(self):
        super().setUp()
        self._cleanup()

    def tearDown(self) -> None:
        super().tearDown()
        self._cleanup()

    @contextmanager
    def server(self):
        class ServerThread(threading.Thread):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.ready = threading.Event()

            def run(self):
                with socketserver.UnixStreamServer(SOCK_FILE, worker.Handler) as server:
                    self.server = server
                    self.ready.set()
                    server.serve_forever()

        th = ServerThread()
        th.start()
        th.ready.wait()

        with socket.socket(socket.AF_UNIX) as sock:
            sock.connect(SOCK_FILE)
            try:
                wfile = sock.makefile('wb', buffering=0)
                rfile = sock.makefile('rb')
                yield wfile, rfile
            finally:
                th.server.shutdown()
                th.join()


class TestServer(ServerTest, ConfigSetting):
    def test_normal_case(self):
        with self.server() as (wfile, rfile):
            knock(wfile, rfile)
            make_env = '''
            def mapper(patch):
                return patch.size

            def reducer(a, b):
                return a + b
            '''

            pickle.dump(api.WorkerRequest(make_env, {}), wfile)
            wfile.close()
            while True:
                res = pickle.load(rfile)
                if not isinstance(res, api.Progress):
                    break
            res: api.WorkerResult = res
            self.assertIsInstance(res, api.WorkerResult)
            self.assertEqual(
                res.value,
                reduce(lambda a, b: a + b, [p.size for p in patches('pdr2_dud')]),
            )

    def test_invalid_credentials(self):
        with self.server() as (wfile, rfile):
            with self.assertRaises(AuthError):
                knock(wfile, rfile, salt=b'invalid salt')

    def test_unexpected_exception(self):
        with self.server() as (wfile, rfile):
            knock(wfile, rfile)
            make_env = '''
            def mapper(patch):
                0 / 0

            def reducer(a, b):
                return a + b
            '''

            pickle.dump(api.WorkerRequest(make_env, {}), wfile)
            wfile.close()
            while True:
                res = pickle.load(rfile)
                if not isinstance(res, api.Progress):
                    break
            self.assertIsInstance(res, ZeroDivisionError)

    def test_interrupt(self):
        with self.server() as (wfile, rfile):
            import time

            knock(wfile, rfile)
            make_env = '''
            import time
            
            def mapper(patch):
                time.sleep(1)
                return patch.size
            
            def reducer(a, b):
                return a + b

            chunksize = 1
            '''

            pickle.dump(api.WorkerRequest(make_env, {}), wfile)
            time.sleep(0.1)
            pickle.dump(api.Interrupt(), wfile)
            wfile.close()
            while True:
                res = pickle.load(rfile)
                if not isinstance(res, api.Progress):
                    break
            self.assertIsInstance(res, api.UserError)


def get_tasks(env):
    return patches('pdr2_dud')


@lru_cache()
def patches(rerun_name: str):
    from quickdb.sspcatalog.patch import Rerun
    return Rerun(f'{REPO_DIR}/{rerun_name}').patches[:100]


def process_request_simple(make_env: str, shared: Dict,
                           progress: ProgressCB = None):
    from functools import reduce
    env = evaluate(make_env, shared)
    return reduce(env['reducer'], map(env['mapper'], config.tasks(env)))
