import unittest
from . import deploy


class TestBatch(unittest.TestCase):
    def test_divided(self):
        a = []
        for b in deploy.batch(range(4), 2):
            a.append(b)
        self.assertListEqual(a, [[0, 1], [2, 3]])

    def test_individed(self):
        a = []
        for b in deploy.batch(range(4), 3):
            a.append(b)
        self.assertListEqual(a, [[0, 1, 2], [3]])
