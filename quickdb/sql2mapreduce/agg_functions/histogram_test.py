from quickdb.test_config import REPO_DIR
import numpy
from quickdb.sql2mapreduce.agg_test import run_test_agg_sql, patches
import unittest


@unittest.skipUnless(REPO_DIR, 'REPO_DIR is not set')
class TestHistogramAggCall(unittest.TestCase):
    def test_minmax(self):
        sql = '''
        SELECT histogram(flux2mag(forced.i.psfflux_flux)) FROM pdr2_dud
        '''
        result = run_test_agg_sql(sql)
        hist, bins = result.group_by[None][0]
        self.assertEqual(len(hist), len(bins) - 1)

    def test_minmax_with_range(self):
        sql = '''
        SELECT histogram(flux2mag(forced.i.psfflux_flux), range => (0, 30)) FROM pdr2_dud
        '''
        result = run_test_agg_sql(sql)
        hist, bins = result.group_by[None][0]
        self.assertEqual(len(hist), len(bins) - 1)
