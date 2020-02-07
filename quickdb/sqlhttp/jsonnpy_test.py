import unittest
import numpy
from . import jsonnpy


samples = [
    {
        'sql': 'SELECT ...',
        'shared': {
            'coords': numpy.arange(1000, dtype=float),
        },
    },
    {
        'dict': {'array': [1,2,3], 'str': 'str'},
    },
]


class TestJsonNpy(unittest.TestCase):
    def test_dumps_loads(self):
        for s in samples:
            self.assertEqual(
                repr(jsonnpy.loads(jsonnpy.dumps(s))),
                repr(s),
            )
