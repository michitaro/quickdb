from . import batch
import argparse
import pprint
import time
import re
import subprocess
import logging
logging.basicConfig(level=logging.INFO)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--trace', '-t', action='store_true')
    parser.add_argument('action', nargs='+', choices='start stop restart status cleanup kill-by-port'.split())
    args = parser.parse_args()

    for action in args.action:
        exec_action(action, args)


def exec_action(action, args):
    if action == 'status':
        show_status()
    elif action == 'start':
        start(args.trace)
    elif action == 'stop':
        stop()
    elif action == 'restart':
        restart()
    elif action == 'kill-by-port':
        kill_by_port()


def start(trace=False):
    def cmd(worker):
        env = 'DATARAKE_TRACE=1' if trace else ''
        return ['bash', '-c', f"""'cd {worker.work_dir}/python_path
                                   {env} nohup {worker.python_path} -u -m quickdb.datarake.worker --port {worker.port} >> log 2>&1 &'"""]
    batch.show_result(batch.ssh(cmd))


def stop():
    def cmd(worker):
        return ['bash', '-c', f"""'cd {worker.work_dir}/python_path
                                   [ -e pid ] && kill $(cat pid)'"""]
    batch.show_result(batch.ssh(cmd))


def cleanup():
    def cmd(worker):
        return ['bash', '-c', f"""'cd {worker.work_dir}/python_path
                                   rm -rf log'"""]
    batch.show_result(batch.ssh(cmd))


def show_status():
    def cmd(worker):
        return ['bash', '-c', f"""'cd {worker.work_dir}/python_path
                                   [ -e pid ] && cat pid'"""]
    pids = dict((worker.host, out.decode('utf-8', 'ignore')) for worker, returncode, out, err in batch.ssh(cmd))
    pprint.pprint(pids)


def restart():
    stop()
    time.sleep(1)
    start()


def kill_by_port():
    def cmd(worker):
        return ['bash', '-c', f"""'netstat -nlp | grep 0.0.0.0:{worker.port}'"""]
    pid_re = re.compile('\s(\d+)/python\s*')
    for worker, returncode, out, err in batch.ssh(cmd):
        m = pid_re.search(out.decode('utf-8'))
        if m:
            pid = m.group(1)
            logging.info(f'kill {pid} on {worker.host}')
            pid = m.group(1)
            subprocess.check_call(['ssh', worker.host, 'kill', '-9', pid])
            subprocess.check_call(['ssh', worker.host, 'rm', '-rf', f'{worker.work_dir}/.lock', f'{worker.work_dir}/pid'])

    def cmd(worker):
        return ['bash', '-c', f"""'cd {worker.work_dir}/python_path
                                   rm -rf pid .lock'"""]
    batch.show_result(batch.ssh(cmd))


if __name__ == '__main__':
    main()
