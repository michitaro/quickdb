import numpy
from ...aggregator import Aggregator
from ... import sqlast


class MinMaxAggregator(Aggregator):
    n_args = 1

    @classmethod
    def call_map_args_pycode(cls, fc, pcb):
        return pcb(fc.nameless_args[0])

    def map(self, vals):
        assert len(vals.shape) == 1
        fs = vals[numpy.isfinite(vals)]
        self.min = fs.min() if len(fs) > 0 else numpy.nan
        self.max = fs.max() if len(fs) > 0 else numpy.nan

    def reduce(self, other):
        self.min = numpy.nanmin([self.min, other.min])
        self.max = numpy.nanmax([self.max, other.max])

    def result(self):
        return self.min, self.max
