from quickdb.test_config import REPO_DIR
import math
import numpy
from quickdb.sql2mapreduce.agg_test import run_test_agg_sql, patches
import unittest


@unittest.skipUnless(REPO_DIR, 'REPO_DIR is not set')
class TestCountAggCall(unittest.TestCase):
    def test_minmax_integer(self):
        sql = '''
        SELECT sum(object_id) FROM pdr2_dud WHERE object_id % 100 = 0
        '''
        result = run_test_agg_sql(sql)
        ps = patches('pdr2_dud')
        sum = 0
        for p in ps:
            sum += p[p('object_id') % 100 == 0]('object_id').sum()
        self.assertEqual(result.group_by[None][0], sum)
