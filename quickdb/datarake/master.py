import concurrent.futures
from contextlib import contextmanager
import pickle
from quickdb.datarake.safeevent import SafeEvent, wait_for_safe_event
import socket
import threading
from functools import reduce
from itertools import repeat
from typing import Callable, Dict, Iterable, Optional

from quickdb.datarake.auth import knock
from quickdb.datarake.interface import Progress, ProgressCB
from quickdb.sspcatalog.errors import UserError
from quickdb.utils.evaluate import evaluate

from . import api, config


def run_make_env_with_interrupt(make_env: str, *, interrupt_notifiyer: SafeEvent, shared: Optional[Dict], progress: Optional[ProgressCB]):
    shared = {} if shared is None else shared
    mapped_values = scatter(make_env, shared, progress, interrupt_notifiyer)
    env = evaluate(make_env, dict(shared))  # we need to pass a copy of `shared` because `evaluate` makes some changes on `shared`
    finalizer = env.get('finalizer')
    result = reduce(env['reducer'], (mv.value for mv in mapped_values))
    if finalizer:
        result = finalizer(result)
    return result


def run_make_env(make_env: str, shared: Dict = None, progress: ProgressCB = None, interrupt_notifiyer: SafeEvent = None):
    with SafeEvent() as noop:
        return run_make_env_with_interrupt(make_env, interrupt_notifiyer=interrupt_notifiyer or noop, shared=shared, progress=progress)


def scatter(make_env: str, shared: Dict, progress: Optional[ProgressCB], interrupt_notifiyer: SafeEvent) -> Iterable[api.WorkerResult]:
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
            repeat(interrupt_notifiyer),
        ))


def unpack_args(f):
    def g(args):
        return f(*args)
    return g


def post_request(worker, make_env, shared, progress1: Callable[[config.Worker, float], float], interrupt_notifiyer: SafeEvent):
    with socket.socket() as sock:
        sock.connect((worker.host, worker.port))
        rfile = sock.makefile('rb')
        wfile = sock.makefile('wb', buffering=0)
        knock(wfile, rfile)
        request = api.WorkerRequest(make_env, shared)
        pickle.dump(request, wfile)
        with wait_for_safe_event(interrupt_notifiyer, lambda: pickle.dump(api.Interrupt(), wfile)):
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
            raise RuntimeError(f'{res}@{worker.host}')


def test():
    make_env = '''
        rerun = 'pdr2_dud'

        def mapper(patch):
            return patch.size
        
        def reducer(a, b):
            return a + b
    '''
    with SafeEvent() as interrupt_notifyer:
        try:
            result = run_make_env(make_env, interrupt_notifiyer=interrupt_notifyer)
            print(result)
        except KeyboardInterrupt:
            interrupt_notifyer.set()


if __name__ == '__main__':
    test()
