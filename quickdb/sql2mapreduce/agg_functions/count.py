from typing import Dict, List

from quickdb.sql2mapreduce.agg import AggCall, AggContext
from quickdb.sql2mapreduce.sqlast.sqlast import Expression, SqlError


class CountAggCall(AggCall):
    def __init__(self, args: List[Expression], named_args: Dict[str, Expression], agg_star: bool):
        if not agg_star:  # pragma: no cover
            raise SqlError(f'COUNT accepts only * for its parameter')

    def mapper(self, context: AggContext):
        return context._patch.size

    def reducer(self, a, b):
        return a + b

    def finalizer(self, a):
        return a
