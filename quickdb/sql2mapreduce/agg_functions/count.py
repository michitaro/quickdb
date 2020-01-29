from typing import Dict, List

from quickdb.sql2mapreduce.agg import AggCall, AggContext
from quickdb.sql2mapreduce.sqlast.sqlast import Expression, SqlError


class CoutnAggCall(AggCall):
    def __init__(self, args: List[Expression], named_args: Dict[str, Expression]):
        pass

    def mapper(self, context: AggContext):
        return context._patch.size

    def reducer(self, a, b):
        return a + b

    def finalizer(self, a):
        return a
