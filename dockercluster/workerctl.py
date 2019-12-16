import argparse
import contextlib
import os
import shlex
import subprocess
from concurrent.futures import ThreadPoolExecutor
from pprint import pprint
from typing import List

from . import buildimage, config
from .logger import logger
from .utils.chdir import chdir


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--parallel', '-j', type=int, default=len(config.worker_nodes))
    parser.set_defaults(func=lambda _: parser.error('No subcommand specified.'))
    subparsers = parser.add_subparsers()

    with tap(subparsers.add_parser('update-code')) as p:
        p.add_argument('--only-code', action='store_true')
        p.set_defaults(func=update_code)

    with tap(subparsers.add_parser('shell')) as p:
        p.add_argument('cmds', nargs='+')
        p.set_defaults(func=shell)

    subparsers.add_parser('start').set_defaults(func=start_workermanagerd)
    subparsers.add_parser('status').set_defaults(func=show_workermanagerd_status)
    subparsers.add_parser('stop').set_defaults(func=stop_workermanagerd)
    subparsers.add_parser('ps').set_defaults(func=show_dockerclusters)

    args = parser.parse_args()
    args.func(args)


def update_code(args):
    def update_code1(wn: config.WorkerNodeConfig):
        subprocess.check_call(['ssh', f'{wn.ssh_user}@{wn.host}', '--', 'mkdir', '-p', wn.codedir])
        subprocess.check_call(['rsync', '-a', '--delete',
                               '--exclude', '.venv',
                               '--exclude', 'run',
                               './', f'{wn.ssh_user}@{wn.host}:{wn.codedir}'])
        if not args.only_code:
            subprocess.check_call(['ssh', f'{wn.ssh_user}@{wn.host}', '--', 'sh', '-c', shlex.quote(f'''
                cd {wn.codedir} && \
                ( PIPENV_VENV_IN_PROJECT=1 {wn.pipenv_path} install || \
                PIPENV_VENV_IN_PROJECT=1 {wn.pipenv_path} --python {wn.python_path} install )
            ''')])
            subprocess.check_call(['ssh', f'{wn.ssh_user}@{wn.host}', '--', 'sh', '-c', shlex.quote(f'''
                cd {wn.codedir} && PIPENV_VENV_IN_PROJECT=1 {wn.pipenv_path} run python -m dockercluster.buildimage
            ''')])
    run_in_parallel(update_code1, args.parallel)


def shell(args):
    def cmds(wn: config.WorkerNodeConfig):
        return args.cmds
    ssh(cmds, args.parallel, pretty=True)


def start_workermanagerd(args):
    logger.info('Stopping running workermanagerds...')
    stop_workermanagerd(args)

    logger.info('Starting workermanagerds...')

    def cmds(wn: config.WorkerNodeConfig):
        return ['sh', '-c', shlex.quote(f'''
            cd {wn.codedir} &&
            mkdir -p run/pid &&
            {wn.pipenv_path} run sh -c 'nohup python -m dockercluster.workermanagerd >> run/log 2>&1 & echo $! > run/pid/workermanagerd.pid'
            cat run/pid/workermanagerd.pid
        ''')]
    ssh(cmds, args.parallel)


def show_workermanagerd_status(args):
    def cmds(wn: config.WorkerNodeConfig):
        return ['sh', '-c', shlex.quote(f'''
            [ -f {wn.codedir}/run/pid/workermanagerd.pid ] && cat {wn.codedir}/run/pid/workermanagerd.pid
        ''')]
    ssh(cmds, args.parallel)


def stop_workermanagerd(args):
    def cmds(wn: config.WorkerNodeConfig):
        return ['sh', '-c', shlex.quote(f'''
            if [ -f {wn.codedir}/run/pid/workermanagerd.pid ] ; then
                kill $(cat {wn.codedir}/run/pid/workermanagerd.pid)
                rm {wn.codedir}/run/pid/workermanagerd.pid
            fi
        ''')]
    ssh(cmds, args.parallel)


def show_dockerclusters(args):
    def cmds(wn: config.WorkerNodeConfig):
        return ['sh', '-c', shlex.quote(f''' ls {wn.workdir}/dockerclusters ''')]
    ssh(cmds, args.parallel, pretty=True)


def ssh(cmds, parallel, pretty=False):
    def ssh1(wn: config.WorkerNodeConfig):
        try:
            return subprocess.check_output(['ssh', f'{wn.ssh_user}@{wn.host}', '--'] + cmds(wn)).decode().strip()
        except:
            pass
    results = run_in_parallel(ssh1, parallel)
    if pretty:
        for wn, result in results:
            print(f'== {wn.host} ==')
            print(result)
    else:
        pprint({wn.host: result for wn, result in results})


def run_in_parallel(f, parallel):
    with ThreadPoolExecutor(max_workers=parallel) as exclutor:
        return zip(config.worker_nodes, exclutor.map(f, config.worker_nodes))


@contextlib.contextmanager
def tap(a):
    yield a


if __name__ == '__main__':
    with chdir(os.path.join(os.path.dirname(__file__), '..')):
        main()
