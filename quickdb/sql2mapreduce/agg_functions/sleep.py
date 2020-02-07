from typing import Dict, List
from quickdb.sql2mapreduce.agg import AggCall, AggContext
from quickdb.sql2mapreduce.sqlast.sqlast import Expression, SqlError


class SleepAggCall(AggCall):
    def __init__(self, args: List[Expression], named_args: Dict[str, Expression], star_agg: bool):
        if len(args) != 1:
            raise SqlError(f'function `sleep` accepts only 1 positional paramter')
        self._duration = args[0]

    def mapper(self, context: AggContext):
        return self._duration(context)

    def reducer(self, a, b):
        return a

    def finalizer(self, a):
        import time
        time.sleep(a)
        return 0
