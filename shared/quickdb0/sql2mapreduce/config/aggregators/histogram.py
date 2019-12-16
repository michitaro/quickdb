import numpy
from ...aggregator import Aggregator
from ... import sqlast
from .minmax import MinMaxAggregator


class HistogramAggregator(Aggregator):
    n_args = 1
    required_args = ['range']

    @classmethod
    def call_map_args_pycode(cls, fc, pcb):
        return pcb.args_pycode(
            fc.nameless_args[0],
            fc.named_args['range'],
            bins=fc.named_args.get('bins'),
            weights=fc.named_args.get('weights'),
            )

    def map(self, vals, histrange, bins=50, weights=None):
        self.hist, self.edges = numpy.histogram(vals, bins=bins, range=histrange, weights=weights)

    def reduce(self, other):
        self.hist += other.hist

    def result(self):
        return self.hist, self.edges

    @classmethod
    def _add_missing_ast_args(cls, ast):
        if 'range' not in ast.named_args:
            ast.args.append(
                sqlast.NamedArgExpr.build(
                    name='range',
                    arg=MinMaxAggregator.build_call_ast(ast.args[0])))

