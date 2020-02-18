from quickdb.sqlhttp.sqlclient import post_sql_streaming
import os
import unittest


@unittest.skipUnless(os.environ.get('CLUSTER_TEST'), '$CLUSTER_SET is not set')
class TestSqlClient(unittest.TestCase):
    def test_streaming(self):
        sql = 'SELECT object_id from pdr2_dud'
        for msg in post_sql_streaming(sql):
            print(msg)
