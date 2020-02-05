import contextlib
import os
import time
import unittest

from quickdb.datarake import master
from quickdb.sql2mapreduce import Select, run_agg_query


@unittest.skipUnless(os.environ.get('CLUSTER_TEST'), '$CLUSTER_SET is not set')
class TestCluster(unittest.TestCase):
    def test_direct_python_code(self):
        make_env = '''
            def mapper(patch):
                object_id = patch('object_id')
                mod = object_id % 2
                return {
                    0: (mod == 0).sum(),
                    1: (mod == 1).sum(),
                }
            
            def reducer(a, b):
                return {
                    0: a[0] + b[0],
                    1: a[1] + b[1],
                }

            rerun = 'pdr2_wide'
        '''

        with timeit('direct'):
            result = master.run_make_env(make_env)
        print(result)

    def test_agg_query(self):
        select = Select('''
            SELECT COUNT(*) FROM pdr2_wide
            GROUP BY object_id % 2
        ''')

        with timeit('SQL'):
            result = run_agg_query(select, master.run_make_env)

        print(result)


@contextlib.contextmanager
def timeit(label: str):
    start = time.time()
    yield
    end = time.time()
    print(f'{label}: {end - start}s')
