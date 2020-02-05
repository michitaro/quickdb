from functools import lru_cache
from typing import Dict
import unittest

import numpy

from quickdb.sql2mapreduce.agg import run_agg_query
from quickdb.sql2mapreduce.sqlast.sqlast import Select
from quickdb.test_config import REPO_DIR

numpy.seterr(all='ignore')


@unittest.skipUnless(REPO_DIR, 'REPO_DIR is not set')
class TestRunAggQuery(unittest.TestCase):
    def test_count(self):
        sql = '''
            SELECT COUNT(*) as count FROM pdr2_dud
        '''
        result = run_test_agg_sql(sql)
        count = 0
        for patch in patches('pdr2_dud'):
            count += patch.size
        self.assertEqual(count, result.group_by[None][0])
        self.assertEqual('count', result.target_names[0])

    def test_count_group(self):
        sql = '''
            SELECT COUNT(*) FROM pdr2_dud
            GROUP BY forced.i.extendedness_value < 0.5
        '''
        result = run_test_agg_sql(sql)
        count_true = 0
        count_false = 0
        for patch in patches('pdr2_dud'):
            count_true += (patch('forced.i.extendedness_value') < 0.5).sum()
            count_false += numpy.logical_not(patch('forced.i.extendedness_value') < 0.5).sum()
        self.assertEqual(count_true, result.group_by[(True,)][0])
        self.assertEqual(count_false, result.group_by[(False,)][0])

    def test_count_nested_group(self):
        sql = '''
            SELECT COUNT(*) FROM pdr2_dud
            GROUP BY
                object_id % 2,
                forced.i.extendedness_value < 0.5
        '''
        result = run_test_agg_sql(sql)
        count = 0
        for patch in patches('pdr2_dud'):
            count += patch.size
        for l in result.group_by.values():
            count -= l[0]
        self.assertEqual(count, 0)

    def test_count_where_nested_group(self):
        sql = '''
            SELECT
                COUNT(*)
            FROM
                pdr2_dud
            WHERE
                object_id % 3 = 0
            GROUP BY
                object_id % 2,
                forced.i.extendedness_value < 0.5
        '''
        result = run_test_agg_sql(sql)
        count = 0
        for patch in patches('pdr2_dud'):
            count += (patch('object_id') % 3 == 0).sum()
        for l in result.group_by.values():
            count -= l[0]
        self.assertEqual(count, 0)

    def test_evaluation_of_agg_results(self):
        sql = '''
            SELECT 2 * COUNT(*) FROM pdr2_dud
        '''
        result = run_test_agg_sql(sql)
        count = 0
        for patch in patches('pdr2_dud'):
            count += patch.size
        self.assertEqual(2 * count, result.group_by[None][0])

    def test_context_dependent(self):
        sql = '''
            SELECT
                (object_id % 2 + 1) * 2
            FROM pdr2_dud
            GROUP BY
                object_id % 2
        '''
        result = run_test_agg_sql(sql)
        self.assertEqual(result.group_by[(0,)], [2])
        self.assertEqual(result.group_by[(1,)], [4])

    def test_shared(self):
        sql = '''
            SELECT
                COUNT(*), shared.message
            FROM
                pdr2_dud
        '''
        result = run_test_agg_sql(sql, shared={'message': 'Hello world'})
        self.assertEqual(result.group_by[None][1], 'Hello world')


def run_test_agg_sql(sql: str, shared: Dict = None):
    select = Select(sql)
    return run_agg_query(select, run_make_env, shared=shared)


@lru_cache()
def patches(rerun_name: str):
    from quickdb.sspcatalog.patch import Rerun
    return Rerun(f'{REPO_DIR}/{rerun_name}').patches[:10]


def run_make_env(make_env: str, shared: Dict, progress=None):
    from quickdb.datarake.utils import evaluate
    from functools import reduce
    shared = through_serialization(shared)
    env = evaluate(make_env, shared)
    return env['finalizer'](reduce(env['reducer'], map(env['mapper'], patches(env['rerun']))))


def through_serialization(a):
    import pickle
    return pickle.loads(pickle.dumps(a))
