from typing import Dict, List

from quickdb.sql2mapreduce.agg import AggCall, AggContext
from quickdb.sql2mapreduce.sqlast.sqlast import Expression, SqlError


class SumAggCall(AggCall):
    def __init__(self, args: List[Expression], named_args: Dict[str, Expression], agg_star: bool):
        if len(args) != 1 or len(named_args) != 0 or agg_star:
            raise SqlError(f'`sum` accept only 1 argument')
        self.arg = args[0]

    def mapper(self, context: AggContext):
        return self.arg(context).sum()

    def reducer(self, a, b):
        return a + b

    def finalizer(self, a):
        return a
