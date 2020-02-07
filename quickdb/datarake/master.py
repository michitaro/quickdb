import concurrent.futures
import pickle
from quickdb.utils.evaluate import evaluate
import socket
from functools import reduce
from itertools import repeat
from typing import Callable, Dict, Iterable

from quickdb.datarake.auth import knock
from quickdb.datarake.interface import Progress, ProgressCB
from quickdb.sspcatalog.errors import UserError

from . import config, api


def run_make_env(make_env: str, shared: Dict = None, progress: ProgressCB = None):
    shared = {} if shared is None else shared
    mapped_values = scatter(make_env, shared, progress)
    env = evaluate(make_env, dict(shared))  # we need to pass a copy of `shared` because `evaluate` makes some changes on `shared`
    finalizer = env.get('finalizer')
    result = reduce(env['reducer'], (mv.value for mv in mapped_values))
    if finalizer:
        result = finalizer(result)
    return result


def scatter(make_env: str, shared: Dict, progress: ProgressCB = None) -> Iterable[api.WorkerResult]:
    progresses: Dict[config.Worker, Progress] = {}

    def progress1(worker: config.Worker, p: Progress):
        progresses[worker] = p
        if progress:
            progress(reduce(lambda a, b: Progress(done=a.done + b.done, total=a.total + b.total), progresses.values()))

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(config.workers)) as pool:
        return pool.map(unpack_args(post_request), zip(
            config.workers,
            repeat(make_env),
            repeat(shared),
            repeat(progress1),
        ))


def unpack_args(f):
    def g(args):
        return f(*args)
    return g


def post_request(worker, make_env, shared, progress1: Callable[[config.Worker, float], float]):
    with socket.socket() as sock:
        sock.connect((worker.host, worker.port))
        rfile = sock.makefile('rb')
        wfile = sock.makefile('wb', buffering=0)
        knock(wfile, rfile)
        request = api.WorkerRequest(make_env, shared)
        pickle.dump(request, wfile)
        while True:
            res = pickle.load(rfile)
            if isinstance(res, api.Progress):
                progress1(worker, res)
            else:
                break
        if isinstance(res, api.WorkerResult):
            return res
        elif isinstance(res, api.UserError):
            raise UserError(res.reason)
        else:
            raise RuntimeError(res)

        # raise res
        # error = response.get('error')
        # if error is not None:
        #     raise RuntimeError(f'@{worker.host}: {error}')
        # return response


def test():
    make_env = '''
        rerun = 'pdr2_dud'

        def mapper(patch):
            return patch.size
        
        def reducer(a, b):
            return a + b
    '''
    result = run_make_env(make_env)
    print(result)


if __name__ == '__main__':
    test()
