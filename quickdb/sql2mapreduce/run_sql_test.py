from quickdb.test_config import REPO_DIR
from quickdb.sql2mapreduce.nonagg_test import run_make_env
import unittest
from .run_sql import QueryResult, run_sql


@unittest.skipUnless(REPO_DIR, 'REPO_DIR is not set')
class TestRunSql(unittest.TestCase):
    def test_run_sql_nonagg(self):
        sql = '''
            SELECT object_id, object_id % 2 FROM pdr2_dud LIMIT 10
        '''
        result = run_sql(sql, run_make_env)
        self.assertIsInstance(result, QueryResult)
        self.assertEqual(len(result.target_names), 2)
        self.assertEqual(len(result.target_list), 2)
        self.assertEqual(len(result.target_list[0]), 10)
        self.assertEqual(len(result.target_list[1]), 10)

    def test_run_sql_agg(self):
        sql = '''
            SELECT COUNT(*), object_id % 2 FROM pdr2_dud GROUP BY object_id % 2
        '''
        result = run_sql(sql, run_make_env)
        self.assertIsInstance(result, QueryResult)
        self.assertEqual(len(result.target_names), 3)
        self.assertEqual(len(result.target_list), 3)
        self.assertEqual(len(result.target_list[0]), 2)
