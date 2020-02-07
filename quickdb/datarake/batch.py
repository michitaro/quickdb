import functools
from . import parallel
import argparse
import subprocess
import os
from . import config
from . import workerctrl


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--check', action='store_true')
    parser.add_argument('--update-code', action='store_true')
    parser.add_argument('--restart', '-r', action='store_true')
    parser.add_argument('cmd', nargs='*')
    args = parser.parse_args()

    if args.check:
        check_connection()

    if len(args.cmd) > 0:
        show_result(batch(*args.cmd))

    if args.update_code:
        if args.restart:
            workerctrl.stop()
        parallel.map(update_code1, config.workers)
        if args.restart:
            workerctrl.start()
            workerctrl.show_status()


def show_result(batch_result):
    for worker, returncode, out, err in batch_result:
        print(f'{worker.host}({returncode})')
        if len(out) > 0:
            print(f'{out.decode("utf-8", "ignore")}')
        if len(err) > 0:
            print(f'err: {err.decode("utf-8", "ignore")}')


def batch(*cmd):
    def f(worker):
        return cmd
    return ssh(f)


def ssh(cmd_function):
    def ssh1(worker, cmd1):
        p = subprocess.Popen(
            ['ssh', worker.host, '--', 'cd', worker.work_dir, ';', *cmd1],
            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        p.wait()
        return worker, p.returncode, out, err
    cmds = map(cmd_function, config.workers)
    cmds = list(cmds)
    return parallel.starmap(ssh1, list(zip(config.workers, cmds)), len(config.workers))


def update_code1(worker):
    # exclude = 'repobuilder htmlcov log'.split()
    exclude = 'htmlcov log *.fits'.split()
    subprocess.check_call([
        'rsync', '-av', '--delete',
        *functools.reduce(lambda a, b : a + b, (['--exclude', f] for f in exclude), []),
        f'{os.path.dirname(os.path.realpath(__file__))}/../../',
        '-m', f'{worker.host}:{worker.work_dir}/python_path',
    ])


def check_connection():
    for worker in config.workers:
        subprocess.check_call(['ssh', worker.host, 'id'])


if __name__ == '__main__':
    main()
