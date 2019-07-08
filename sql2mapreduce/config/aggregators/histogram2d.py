import numpy
from ...aggregator import Aggregator
from ... import sqlast
from .minmax import MinMaxAggregator


class Histogram2dAggregator(Aggregator):
    n_args = 2
    required_args = ['range']

    @classmethod
    def call_map_args_pycode(cls, fc, pcb):
        return pcb.args_pycode(
            fc.nameless_args[0],
            fc.nameless_args[1],
            fc.named_args['range'],
            bins=fc.named_args.get('bins'),
            weights=fc.named_args.get('weights'),
            )

    def map(self, xvals, yvals, histrange, bins=(50, 50), weights=None):
        self.hist, self.xedges, self.yedges = numpy.histogram2d(xvals, yvals, bins=bins, range=histrange, weights=weights)

    def reduce(self, other):
        self.hist += other.hist

    def result(self):
        return self.hist, self.xedges, self.yedges

    @classmethod
    def _add_missing_ast_args(cls, ast):
        if 'range' not in ast.named_args:
            ast.args.append(
                sqlast.NamedArgExpr.build(
                    name='range',
                    arg=sqlast.RowExpr.build(
                        args=[
                            MinMaxAggregator.build_call_ast(ast.args[0]),
                            MinMaxAggregator.build_call_ast(ast.args[1]),
                            ])))


