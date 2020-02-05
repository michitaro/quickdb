from typing import Any, Dict, List, NamedTuple

import numpy

from quickdb.sql2mapreduce.agg import AggCall, AggContext
from quickdb.sql2mapreduce.sqlast.sqlast import Expression, SqlError


class MinMax(NamedTuple):
    min: Any
    max: Any


MinMaxMapperResult = List[MinMax]


class MinMaxAggCall(AggCall):
    def __init__(self, args: List[Expression], named_args: Dict[str, Expression], agg_star: bool):
        if agg_star or len(named_args) > 0:  # pragma: no cover
            raise SqlError(f'minmax does not accept named args: {named_args}')
        self._args = args

    def mapper(self, context: AggContext) -> MinMaxMapperResult:
        return [MinMax(numpy.nanmin(ea), numpy.nanmax(ea)) for ea in (a(context) for a in self._args)]

    def reducer(self, a: MinMaxMapperResult, b: MinMaxMapperResult) -> MinMaxMapperResult:
        return [MinMax(min(aa.min, bb.min), max(aa.max, bb.max)) for aa, bb in zip(a, b)]

    def finalizer(self, a: MinMaxMapperResult):
        return a


class MinAggCall(MinMaxAggCall):
    def finalizer(self, a: MinMaxMapperResult):
        return a[0].min


class MaxAggCall(MinMaxAggCall):
    def finalizer(self, a: MinMaxMapperResult):
        return a[0].max
