import concurrent.futures
import glob
import logging
import os
import pickle
import re
import socket
from typing import List

from . import config, utils
from .timer import Timer


class Worker:
    def __init__(self, socketfile: str):
        self.socketfile = socketfile

    @property
    def host(self):
        bname = os.path.basename(self.socketfile)
        m = re.match(r'worker-(.*?)\.sock$', bname)
        return m.group(1)


workers: List[Worker] = [Worker(s) for s in glob.glob('/sockets/worker-*.sock')]


def run(make_env, context=None):
    if context is None:
        context = {}
    mapped_values = list(scatter(make_env, context))
    env = utils.evaluate(make_env, context)
    return {
        'result': utils.reduce(env['reducer'], (mv['result'] for mv in mapped_values), env.get('initial')),
        'time': {w.host: mv['time'] for w, mv in zip(workers, mapped_values)},
    }


def scatter(make_env, context):
    from itertools import repeat
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(workers)) as pool:
        return pool.map(pack_args(post_request), zip(
            workers,
            repeat(make_env),
            repeat(context),
        ))


def pack_args(f):
    def g(args):
        return f(*args)
    return g


def post_request(worker: Worker, make_env, context):
    try:
        with socket.socket(socket.AF_UNIX) as sock:
            sock.connect(worker.socketfile)
            rfile = sock.makefile('rb')
            wfile = sock.makefile('wb', buffering=0)
            try:
                request = dict(make_env=make_env, context=context)
                pickle.dump(request, wfile)
                response = pickle.load(rfile)
                error = response.get('error')
                if error is not None:
                    raise RuntimeError(f'@{worker.host}: {error}')
                return response
            finally:
                wfile.close()
                rfile.close()
    except ConnectionError:
        logging.error(f'Connection Error on {worker.host}')
        raise


def test():
    make_env = '''
        def mapper(t):
            return len(t)

        def reducer(acc, val):
            return acc + val
    '''
    print(run(make_env))


if __name__ == '__main__':
    test()
