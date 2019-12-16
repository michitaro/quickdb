from .timer import Timer
from . import config
from . import utils
import multiprocessing
import socketserver
import argparse
import pickle
import os
import traceback


g_worker_process = None
g_debug = False


def main():
    print('start main')
    global g_worker_process, g_debug
    parser = argparse.ArgumentParser()
    parser.add_argument('--debug', action='store_true')
    args = parser.parse_args()

    if args.debug:
        g_debug = True

    socketpath = '/sockets/master.sock'
    with socketserver.UnixStreamServer(socketpath, Handler) as server:
        os.chmod(socketpath, 0o777)
        with multiprocessing.Pool() as g_worker_process:
            server.serve_forever()


class Handler(socketserver.StreamRequestHandler):
    def handle(self):
        timer = Timer()
        response = {}
        print('start handle')
        try:
            with timer('load'):
                request = pickle.load(self.rfile)
                # import ipdb ; ipdb.set_trace()
            response['result'] = process_request(request, timer)
        except Exception:
            traceback.print_exc()
            response['error'] = traceback.format_exc()
        response['time'] = timer.asdict()
        pickle.dump(response, self.wfile)


# def process_request_safe(request):
#     from . import parallel
#     env = utils.evaluate(request['make_env'], request.get('context'))
#     mapped_values = parallel.map(env['mapper'], config.tasks(request['worker'], env))
#     return utils.reduce(env['reducer'], mapped_values, env.get('initial'))


def process_request(request, timer):
    # equivalent to process_request_safe above but fast
    import itertools
    if g_debug:
        starmap = itertools.starmap
    else:
        starmap = g_worker_process.starmap
    process_request.request_id += 1

    env = utils.evaluate(request['make_env'], request['context'])

    tasks = config.tasks(env)
    with timer('map'):
        mapped_values = starmap(run_mapper, zip(
            itertools.repeat(process_request.request_id),
            itertools.repeat(request),
            range(len(tasks)),
        ))
    with timer('reduce'):
        return utils.reduce(env['reducer'], mapped_values, env.get('initial'))


process_request.request_id = 0


def run_mapper(request_id, request, i):
    # This function is needed to cache evaluated make_env
    memo = run_mapper.memo
    if memo.get('request_id') != request_id:
        env = utils.evaluate(request['make_env'], request['context'])
        tasks = config.tasks(env)
        memo['request_id'] = request_id
        memo['env'] = env
        memo['tasks'] = tasks
    return memo['env']['mapper'](memo['tasks'][i])


run_mapper.memo = {}


if __name__ == '__main__':
    main()
