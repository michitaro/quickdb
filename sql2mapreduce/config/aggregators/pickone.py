import numpy
from ...aggregator import Aggregator
from ... import sqlast


class PickOneAggregator(Aggregator):
    n_args = 1

    @classmethod
    def call_map_args_pycode(cls, fc, pcb):
        return pcb(fc.nameless_args[0])

    def map(self, vals):
        sqlast.check(len(numpy.unique(vals)) == 1, f'{ self.fc } has non-unique value')
        self.val = vals[0]

    def reduce(self, other):
        assert self.val != other.val

    def result(self):
        return self.val
