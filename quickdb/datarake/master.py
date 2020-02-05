import concurrent.futures
import logging
import pickle
import socket
from typing import Dict

from quickdb.sql2mapreduce import ProgressCB

from . import config, utils


def run_make_env(make_env: str, shared: Dict = None, progress: ProgressCB = None):
    shared = {} if shared is None else shared
    mapped_values = scatter(make_env, shared)
    env = utils.evaluate(make_env, dict(shared))  # we need to copy context because of lazy evaluation of scatter
    result = utils.reduce(env['reducer'], pick_result(mapped_values, None), env.get('initial'))
    return result


def pick_result(a, time):
    for worker, v in zip(config.workers, a):
        if time is not None:
            time[worker.host] = v['time']
        yield v['result']


def scatter(make_env, context):
    from itertools import repeat
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(config.workers)) as pool:
        return pool.map(pack_args(post_request), zip(
            config.workers,
            repeat(make_env),
            repeat(context),
        ))


def pack_args(f):
    def g(args):
        return f(*args)
    return g


def post_request(worker, make_env, context):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((worker.host, worker.port))
            rfile = sock.makefile('rb')
            wfile = sock.makefile('wb', buffering=0)
            nonce = rfile.readline().strip()
            wfile.write(utils.hash(nonce) + '\n'.encode('utf-8'))
            auth_line = rfile.readline().decode('utf-8')
            if auth_line.startswith('ng:'):
                reason = auth_line.split(':', 1)[1]
                raise RuntimeError(f'{worker.host}: {reason}')
            else:
                request = dict(make_env=make_env, context=context)
                pickle.dump(request, wfile)
                response = pickle.load(rfile)
                error = response.get('error')
                if error is not None:
                    raise RuntimeError(f'@{worker.host}: {error}')
            return response
    except ConnectionError:
        logging.error(f'Connection Error on {worker.host}')
        raise


if __name__ == '__main__':
    make_env = '''
        rerun = 'pdr2_dud'
    
        def mapper(patch):
            return patch.size

        def reducer(acc, val):
            return acc + val
    '''

    print(run_make_env(make_env))
