import contextlib
import os
import subprocess


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
