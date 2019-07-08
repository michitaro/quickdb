from ...aggregator import Aggregator
from ... import sqlast


class CountAggregator(Aggregator):
    @classmethod
    def call_map_args_pycode(cls, fc, pcb):
        return pcb.t

    def map(self, t):
        self.n = len(t)

    def reduce(self, other):
        self.n += other.n

    def result(self):
        return self.n

    @classmethod
    def check_ast_args(cls, ast):
        sqlast.check(ast.agg_star, 'count can take only "*"')
