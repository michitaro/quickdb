import argparse
import contextlib
import logging
import multiprocessing
import os
import pickle
import socketserver
import subprocess
import threading
from typing import Callable, Dict, Tuple

from quickdb.datarake.api import WorkerRequest
from quickdb.datarake.auth import AuthError, authenticate
from quickdb.datarake.interface import Progress, ProgressCB
from quickdb.sql2mapreduce.sqlast.sqlast import SqlError
from quickdb.sspcatalog.errors import UserError
from quickdb.utils.evaluate import evaluate
from quickdb.utils.interruptableselect import (InterruptableSelect,
                                               SelectInterrupted)

from . import api, config


class WorkerServer(socketserver.TCPServer):
    pass


WorkerServer.allow_reuse_address = True


def main():
    assert config.this_worker is not None, f'hostname={config.hostname}'

    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, default=2394)
    parser.add_argument('--host', default='0.0.0.0')
    parser.add_argument('--pid-file', default='pid')
    parser.add_argument('--parallel', '-j', type=int)
    args = parser.parse_args()

    get_pool(args.parallel)

    logging.basicConfig(level=logging.INFO)

    with critical(), pid_file(args.pid_file):
        with WorkerServer((args.host, args.port), Handler) as server:
            logging.info('worker successfully started')
            server.serve_forever()


class Handler(socketserver.StreamRequestHandler):
    def handle(self):
        try:
            authenticate(self)
        except AuthError:
            return

        def progress(p: Progress):
            pickle.dump(p,  self.wfile)
            self.wfile.flush()

        request: api.WorkerRequest = pickle.load(self.rfile)
        job = Job(request)
        with InterruptableSelect([self.rfile], [], []) as select:
            def stop():
                try:
                    select.wait()
                except SelectInterrupted:
                    pass
                else:  # got stop message
                    job.interrupt()
            stop_th = threading.Thread(target=stop)
            stop_th.start()
            try:
                result = job.run(progress)
            except (UserError, SqlError) as e:
                pickle.dump(api.UserError(str(e)), self.wfile)
            except Exception as e:
                pickle.dump(e, self.wfile)
            else:
                select.interrupt()
                pickle.dump(api.WorkerResult(result), self.wfile)
            finally:
                select.interrupt()
                stop_th.join()


class GetPool:
    def __init__(self):
        self._pool = None

    def __call__(self, n_procs=None):
        if self._pool is None:
            self._pool = multiprocessing.Pool(n_procs)
        return self._pool


get_pool = GetPool()


class Job:
    def __init__(self, request: api.WorkerRequest):
        self._request = request
        self._interrupted = False

    def run(self, progress: ProgressCB = None):
        make_env = self._request.make_env
        shared = self._request.shared
        env = evaluate(make_env, dict(shared))  # pass a copy of `shared` because `evaluate` affects passed `shared` object.
        reducer = env['reducer']
        tasks = config.tasks(env)
        pool = get_pool()
        chunksize = env.get('chunksize') or min(len(tasks) // multiprocessing.cpu_count() + 1, 1024)
        items = [(self, make_env, shared, slice(start, start + chunksize)) for start in range(0, len(tasks), chunksize)]
        result = None
        for i, value in enumerate(pool.imap_unordered(Job._process_partial_tasks, items)):
            if self._interrupted:
                raise UserError('Cancelled')
            result = value if i == 0 else reducer(result, value)
            if progress:
                progress(Progress(done=i + 1, total=len(items)))
        return result

    def interrupt(self):
        self._interrupted = True

    @staticmethod
    def _process_partial_tasks(args: Tuple['Job', str, Dict, slice]):
        job, make_env, shared, part = args
        from functools import reduce
        env = cached_evaluate(job, make_env, shared)
        tasks = config.tasks(env)
        mapper = config.mapper_wrapper(env['mapper'])
        reducer = env['reducer']
        return reduce(reducer, map(mapper, tasks[part]))


class CachedEvaluate:
    def __init__(self):
        self._job = None
        self._cache = None

    def __call__(self, job: Job, make_env: str, shared: Dict):
        if self._job is not job:
            self._job = job
            self._cache = evaluate(make_env, shared)
        return self._cache


cached_evaluate = CachedEvaluate()


@contextlib.contextmanager
def critical():
    try:
        os.makedirs('.lock')
    except:
        raise RuntimeError('another process may be running')
    try:
        yield
    finally:
        subprocess.check_call(['rm', '-rf', '.lock'])


@contextlib.contextmanager
def pid_file(pid_file: str):
    with open(pid_file, 'w') as f:
        f.write(f'{os.getpid()}')
    try:
        yield
    finally:
        os.unlink(pid_file)


if __name__ == '__main__':
    main()
