'''
with-style chdir
'''
import os
import contextlib


@contextlib.contextmanager
def chdir(dirname):
    cwd = os.getcwd()
    try:
      os.chdir(dirname)
      yield os.getcwd()
    finally:
      os.chdir(cwd)
