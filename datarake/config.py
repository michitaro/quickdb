import socket


###############################################################################
# master_addr
# 
# Workers allow access from only `master_addr`
###############################################################################
master_addr = '192.168.0.100' # socket.gethostbyname('quickdb-master.example.com')


###############################################################################
# workers
#
# Master scatters jobs over `workers`
###############################################################################
class Worker(object):
    def __init__(self, host, work_dir, python_path, port=2935):
        self.host = host
        self.port = port
        self.work_dir = work_dir
        self.python_path = python_path

user = 'quickdb'
work_dir = f'/home/{user}/quickdb'
python_path = f'/home/{user}/anaconda3/bin/python'

workers = [
    Worker('quickdb-worker-1.example.com', work_dir, python_path),
    Worker('quickdb-worker-2.example.com', work_dir, python_path),
    Worker('quickdb-worker-3.example.com', work_dir, python_path),
    Worker('quickdb-worker-3.example.com', work_dir, python_path),
    ]


###############################################################################
# tasks
#
# Each worker processes `tasks`.
# `tasks` defines jobs a worker should process.
###############################################################################
from sspcatalog.table import Table
import glob

DEFAULT_RERUN = 'pdr1_wide'

class TaskCache():
    def __init__(self):
        self._cache = {}

    def __call__(self, worker, env):
        rerun = env.get('rerun', DEFAULT_RERUN)
        if rerun not in self._cache:
            self._cache[rerun] = [Table(worker, patch_dir) for patch_dir in glob.glob(f'{worker.work_dir}/repo/{rerun}/patches/*')]
        return self._cache[rerun]


tasks = TaskCache()


###############################################################################
# preload
#
# lines below are executed in both master and workers,
# so we can put global settings here.
###############################################################################

import numpy
import sql2mapreduce.nonaggr

numpy.seterr(all='ignore')
