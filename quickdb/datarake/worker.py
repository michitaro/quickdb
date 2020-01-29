import argparse
import contextlib
import logging
import multiprocessing
import os
import pickle as serialize
import secrets
import signal
import socketserver
import subprocess
import threading
import traceback

from . import config, utils
from .timer import Timer

logging.basicConfig(level=logging.INFO)


class WorkerServer(socketserver.TCPServer):
    allow_reuse_address = True


g_worker_process = None
g_debug = False


def main():
    global g_worker_process, g_debug
    assert config.this_worker is not None, f'hostname={config.hostname}'
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, default=2394)
    parser.add_argument('--host', default='0.0.0.0')
    parser.add_argument('--pid-file', default='pid')
    parser.add_argument('--debug', action='store_true')
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    if args.debug:
        g_debug = True

    with exclusive():
        with open(args.pid_file, 'w') as f:
            f.write(f'{os.getpid()}')
        try:
            with WorkerServer((args.host, args.port), Handler) as server:
                th = None
                def shutdown_server(): server.shutdown()

                def on_sigterm(*args):
                    th = threading.Thread(target=shutdown_server)
                    th.start()
                signal.signal(signal.SIGTERM, on_sigterm)
                logging.info('worker successfully started')
                with multiprocessing.Pool() as g_worker_process:
                    server.serve_forever()
                if th:
                    th.join()
        finally:
            os.unlink(args.pid_file)

    logging.info('worker exited normally')


class Handler(socketserver.StreamRequestHandler):
    def handle(self):
        if self._verify_credentials():
            timer = Timer()
            response = {}
            try:
                with timer('load'):
                    request = serialize.load(self.rfile)
                response['result'] = process_request(request, timer)
            except:
                response['error'] = traceback.format_exc()
            response['time'] = timer.asdict()
            serialize.dump(response, self.wfile)

    def _verify_credentials(self):
        error = []
        while True:
            if config.master_addr != self.client_address[0]:
                error.append(f'connection from {self.client_address} is not allowed')
                break
            nonce = bytes(f'{secrets.randbits(512):0128x}', 'utf-8')
            self.wfile.write(nonce + '\n'.encode('utf-8'))
            self.wfile.flush()
            hashed = self.rfile.readline(1024).strip()
            if hashed != utils.hash(nonce):
                error.append(f'invalid credentials')
                break
            break
        if len(error) > 0:
            self.wfile.write(f'ng: {", ".join(error)}\n'.encode('utf-8'))
        else:
            self.wfile.write('ok\n'.encode('utf-8'))
        self.wfile.flush()
        return len(error) == 0


# def process_request_safe(request):
#     from . import parallel
#     env = utils.evaluate(request['make_env'], request.get('context'))
#     mapped_values = parallel.map(env['mapper'], config.tasks(env))
#     return utils.reduce(env['reducer'], mapped_values, env.get('initial'))


def process_request(request, timer):
    # equivalent to process_request_safe above but fast
    global g_request_id
    import itertools
    if g_debug:
        starmap = itertools.starmap
    else:
        starmap = g_worker_process.starmap
    process_request.request_id += 1
    with timer('make_env'):
        env = utils.evaluate(request['make_env'], dict(request.get('context', {})))
        tasks = config.tasks(env)
    with timer('map'):
        mapped_values = starmap(run_mapper, zip(
            itertools.repeat(process_request.request_id),
            itertools.repeat(request),
            range(len(tasks))
        ))
    with timer('reduce'):
        return utils.reduce(env['reducer'], mapped_values, env.get('initial'))


process_request.request_id = 0


def run_mapper(request_id, request, i):
    memo = run_mapper.memo
    if memo.get('request_id') != request_id:
        env = utils.evaluate(request['make_env'], request.get('context'))
        tasks = config.tasks(env)
        memo['request_id'] = request_id
        memo['env'] = env
        memo['tasks'] = tasks
    return memo['env']['mapper'](memo['tasks'][i])


run_mapper.memo = {}


@contextlib.contextmanager
def exclusive():
    try:
        os.makedirs('.lock')
    except:
        raise RuntimeError('another process may be running')
    try:
        yield
    finally:
        subprocess.check_call(['rm', '-rf', '.lock'])


if __name__ == '__main__':
    if os.environ.get('DATARAKE_TRACE'):
        import trace
        tracer = trace.Trace()
        tracer.run('main()')
    else:
        main()
