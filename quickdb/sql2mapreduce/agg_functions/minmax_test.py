from quickdb.test_config import REPO_DIR
import math
import numpy
from quickdb.sql2mapreduce.agg_test import run_test_agg_sql, patches
import unittest


@unittest.skipUnless(REPO_DIR, 'REPO_DIR is not set')
class TestMinMaxAggCall(unittest.TestCase):
    @unittest.skip('')
    def test_minmax_integer(self):
        sql = '''
        SELECT min(object_id), max(object_id) FROM pdr2_dud
        '''
        result = run_test_agg_sql(sql)
        ps = patches('pdr2_dud')
        m = min((numpy.nanmin(p('object_id')) for p in ps))
        M = max((numpy.nanmax(p('object_id')) for p in ps))
        self.assertEqual(result.group_by[None][0], m)
        self.assertEqual(result.group_by[None][1], M)

    def test_minmax_float(self):
        sql = '''
        SELECT minmax(forced.i.psfflux_flux) FROM pdr2_dud
        '''
        result = run_test_agg_sql(sql)
        print(result)
        self.assertFalse(math.isnan(result.group_by[None][0].min))
        self.assertFalse(math.isnan(result.group_by[None][0].max))
