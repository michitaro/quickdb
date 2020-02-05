from functools import lru_cache
from quickdb.test_config import REPO_DIR
from quickdb.sql2mapreduce.sqlast.sqlast import Select
from quickdb.sql2mapreduce.nonagg import run_nonagg_query
from typing import Dict
import unittest


@unittest.skipUnless(REPO_DIR, 'REPO_DIR is not set')
class TestNonagg(unittest.TestCase):
    def test_sql_basic(self):
        sql = '''
            SELECT
                object_id
            FROM
                pdr2_dud
            LIMIT 100
        '''
        result = run_test_nonagg_sql(sql)
        self.assertEqual(len(result.target_list[0]), 100)

    def test_sql_where_clause(self):
        sql = '''
            SELECT
                object_id
            FROM
                pdr2_dud
            WHERE
                NOT object_id % 2 = 0
            LIMIT 100
        '''
        result = run_test_nonagg_sql(sql)
        self.assertTrue((result.target_list[0] % 2 == 1).all())

    def test_sql_order_clause(self):
        sql = '''
            SELECT
                forced.i.psfflux_flux
            FROM
                pdr2_dud
            WHERE
                NOT isnan(forced.i.psfflux_flux)
            ORDER BY
                forced.i.psfflux_flux
            LIMIT 100
        '''
        result = run_test_nonagg_sql(sql)
        a = result.target_list[0]
        self.assertTrue(((a[1:] - a[:-1]) >= 0).all())

    def test_sql_order_clause_reversed(self):
        sql = '''
            SELECT
                forced.i.psfflux_flux
            FROM
                pdr2_dud
            WHERE
                NOT isnan(forced.i.psfflux_flux)
            ORDER BY
                forced.i.psfflux_flux DESC
            LIMIT 100
        '''
        result = run_test_nonagg_sql(sql)
        a = result.target_list[0]
        self.assertTrue(((a[1:] - a[:-1]) <= 0).all())

    def test_shared_value(self):
        sql = '''
            SELECT
                0 * object_id + shared.the_answer
            FROM
                pdr2_dud
            LIMIT 1
        '''
        result = run_test_nonagg_sql(sql, shared={'the_answer': 42})
        self.assertEqual(result.target_list[0][0], 42)


@lru_cache()
def cached_rerun(rerun_name: str):
    from quickdb.sspcatalog.patch import Rerun
    return Rerun(f'{REPO_DIR}/{rerun_name}')


def run_test_nonagg_sql(sql: str, shared: Dict = None):
    select = Select(sql)
    return run_nonagg_query(select, run_make_env, shared=shared)


def run_make_env(make_env: str, shared: Dict, progress=None):
    from quickdb.datarake.utils import evaluate
    from functools import reduce
    shared = through_serialization(shared)
    env = evaluate(make_env, shared)
    rerun = cached_rerun(env['rerun'])
    patches = rerun.patches[:10]
    return env['finalizer'](reduce(env['reducer'], map(env['mapper'], patches)))


def through_serialization(a):
    import pickle
    return pickle.loads(pickle.dumps(a))
