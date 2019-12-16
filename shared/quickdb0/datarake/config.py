import glob
import socket

import numpy

import sql2mapreduce.nonaggr
###############################################################################
# tasks
#
# Each worker processes `tasks`.
# `tasks` defines jobs a worker should process.
###############################################################################
from sspcatalog.table import Table

DEFAULT_RERUN = 'pdr1_wide'


class TaskCache():
    def __init__(self):
        self._cache = {}

    def __call__(self, env):
        rerun = env.get('rerun', DEFAULT_RERUN)
        if rerun not in self._cache:
            self._cache[rerun] = [Table(patch_dir) for patch_dir in glob.glob(f'/data/{rerun}/patches/*')]
        return self._cache[rerun]


tasks = TaskCache()


###############################################################################
# preload
#
# lines below are executed in both master and workers,
# so we can put global settings here.
###############################################################################


numpy.seterr(all='ignore')
