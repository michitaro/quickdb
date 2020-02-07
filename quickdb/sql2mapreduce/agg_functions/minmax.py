from typing import Any, Dict, List, NamedTuple, Optional

import numpy

from quickdb.sql2mapreduce.agg import AggCall, AggContext
from quickdb.sql2mapreduce.sqlast.sqlast import Expression, SqlError


class MinMax(NamedTuple):
    min: Any
    max: Any


class MinMaxAggCall(AggCall):
    def __init__(self, args: List[Expression], named_args: Dict[str, Expression], agg_star: bool):
        if agg_star or len(args) != 1 or len(named_args) != 0:  # pragma: no cover
            raise SqlError(f'minmax accepts only 1 argument')
        self._array = args[0]

    def mapper(self, context: AggContext):
        ea = self._array(context)
        fea = ea[numpy.isfinite(ea)]
        if len(fea) > 0:
            return MinMax(min=numpy.min(fea), max=numpy.max(fea))

    def reducer(self, a: Optional[MinMax], b: Optional[MinMax]):
        if a and b:
            return MinMax(min=min(a.min, b.min), max=max(a.max, b.max))
        return a or b

    def finalizer(self, a):
        return a or MinMax(min=numpy.nan, max=numpy.nan)


class MinAggCall(MinMaxAggCall):
    def finalizer(self, a):
        return a.min


class MaxAggCall(MinMaxAggCall):
    def finalizer(self, a):
        return a.max
