from quickdb.test_config import REPO_DIR
import numpy
from quickdb.sql2mapreduce.agg_test import run_test_agg_sql, patches
import unittest


@unittest.skipUnless(REPO_DIR, 'REPO_DIR is not set')
class CrossmatchAggCall(unittest.TestCase):
    def test_crossmatch(self):
        sql = '''
        SELECT
            crossmatch(
                forced.coord,
                shared.my_cat,
                5 / arcsec,
                object_id
            )
        FROM
            pdr2_dud
        WHERE
            forced.isprimary
        '''
        result = run_test_agg_sql(sql, shared={'my_cat': gen_coord(1000)})


def gen_coord(n):
    a0 = 331
    a1 = 340
    d0 = -0.58
    d1 = 2
    r = numpy.random.uniform(a0, a1, n)  # type: ignore
    d = numpy.random.uniform(d0, d1, n)  # type: ignore
    return numpy.array([r, d])
