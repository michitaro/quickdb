import argparse
import contextlib
from itertools import islice
import logging
import multiprocessing
import os
import pickle
from quickdb.utils.evaluate import evaluate
import socketserver
import subprocess
from typing import Dict, Generic, Iterable, List, Tuple

from quickdb.datarake.auth import AuthError, authenticate
from quickdb.datarake.interface import Progress, ProgressCB
from quickdb.sql2mapreduce.sqlast.sqlast import SqlError
from quickdb.sspcatalog.errors import UserError
from quickdb.sspcatalog.patch import Patch

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
        def progress(p: Progress):
            pickle.dump(p,  self.wfile)
            self.wfile.flush()

        try:
            authenticate(self)
        except AuthError:
            return

        request: api.WorkerRequest = pickle.load(self.rfile)

        try:
            result = process_request(request. make_env, request.shared, progress)
        except (UserError, SqlError) as e:
            pickle.dump(api.UserError(str(e)), self.wfile)
        except Exception as e:
            pickle.dump(e, self.wfile)
        else:
            pickle.dump(api.WorkerResult(result), self.wfile)


def get_pool(n_procs=None):
    if get_pool.pool is None:
        get_pool.pool = multiprocessing.Pool(n_procs)
    return get_pool.pool


get_pool.pool = None


def process_request(make_env: str, shared: Dict,
                    progress: ProgressCB = None):
    process_request.request_id += 1
    env = evaluate(make_env, dict(shared))  # pass a copy of `shared` because `evaluate` affects passed `shared` object.
    reducer = env['reducer']
    tasks = config.tasks(env)
    pool = get_pool()
    chunksize = min(len(tasks) // multiprocessing.cpu_count(), 1023)
    chunksize += 1
    items = [(process_request.request_id, make_env, shared, slice(start, start + chunksize)) for start in range(0, len(tasks), chunksize)]
    result = None
    for i, value in enumerate(pool.imap_unordered(_process_partial_tasks, items)):
        result = value if i == 0 else reducer(result, value)
        if progress:
            progress(Progress(done=i + 1, total=len(items)))
    return result


process_request.request_id = 0


def _process_partial_tasks(args: Tuple[int, str, Dict, slice]):
    request_id, make_env, shared, part = args
    from functools import reduce
    env = cached_evaluate(request_id, make_env, shared)
    tasks = config.tasks(env)
    mapper = config.mapper_wrapper(env['mapper'])
    reducer = env['reducer']
    return reduce(reducer, map(mapper, tasks[part]))


class CachedEvaluate:
    def __init__(self):
        self._request_id = -1
        self._cache = None

    def __call__(self, request_id: int, make_env: str, shared: Dict):
        if self._request_id != request_id:
            self._request_id = request_id
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
