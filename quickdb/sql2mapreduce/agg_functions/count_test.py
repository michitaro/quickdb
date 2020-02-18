from quickdb.test_config import REPO_DIR
import math
import numpy
from quickdb.sql2mapreduce.agg_test import run_test_agg_sql, patches
import unittest


@unittest.skipUnless(REPO_DIR, 'REPO_DIR is not set')
class TestCountAggCall(unittest.TestCase):
    def test_minmax_integer(self):
        sql = '''
        SELECT count(*) FROM pdr2_dud WHERE object_id % 5 = 2
        '''
        result = run_test_agg_sql(sql)
        ps = patches('pdr2_dud')
        count = 0
        for p in ps:
            count += p[p('object_id') % 5 == 2].size
        self.assertEqual(result.group_by[None][0], count)
