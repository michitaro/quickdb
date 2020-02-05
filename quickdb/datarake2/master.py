from typing import Dict
from quickdb.sql2mapreduce import ProgressCB


def run_make_env(make_env: str, shared: Dict = None, progress: ProgressCB = None):
    shared = {} if shared is None else shared
    # mapped_values = scatter(make_env, shared)
    # env = utils.evaluate(make_env, dict(shared))  # we need to copy context because of lazy evaluation of scatter
    # result = utils.reduce(env['reducer'], pick_result(mapped_values, None), env.get('initial'))
    # return result
