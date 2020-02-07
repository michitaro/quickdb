from quickdb.utils.cached_property import cached_property
from quickdb.sql2mapreduce.agg_functions.minmax import MinMaxAggCall
from typing import Dict, List, Optional

import numpy

from quickdb.sql2mapreduce.agg import AggCall, AggContext
from quickdb.sql2mapreduce.sqlast.sqlast import Expression, SqlError


class HistogramAgg2DCall(AggCall):
    def __init__(self, args: List[Expression], named_args: Dict[str, Expression], agg_star: bool):
        if len(args) != 2:
            raise SqlError(f'function `histogram` accepts only 1 positional paramter')
        self._x = args[0]
        self._y = args[1]
        self._bins: Optional[Expression] = named_args.pop('bins', None)
        self._range = named_args.pop('range', None)
        # self._weights: Optional[Expression] = named_args.pop('weights')
        if len(named_args) != 0:
            raise SqlError(f'Unknown named argument: {named_args.keys()} for function `histogram`')

    @cached_property
    def subaggrs(self, ) -> List[AggCall]:
        if self._range is None:
            self._x_minmax = MinMaxAggCall([self._x], {}, False)
            self._y_minmax = MinMaxAggCall([self._y], {}, False)
            return [self._x_minmax, self._y_minmax]
        return []

    def mapper(self, context: AggContext):
        bins: int = 50 if self._bins is None else self._bins.evaluate(context)
        if self._range is not None:
            row = self._range(context)
            if not isinstance(row, list):
                raise SqlError(f'range must be a list: {row}')
            range = row
        else:
            range = (self._x_minmax.result(context), self._y_minmax.result(context))
        return numpy.histogram2d(self._x(context), self._y(context), bins=bins, range=range)

    def reducer(self, a, b):
        return a[0] + b[0], a[1], a[2]

    def finalizer(self, a):
        return a
