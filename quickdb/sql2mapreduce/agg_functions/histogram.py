from quickdb.sql2mapreduce.agg_functions.minmax import MinMaxAggCall
from typing import Dict, List, Optional

import numpy

from quickdb.sql2mapreduce.agg import AggCall, AggContext
from quickdb.sql2mapreduce.sqlast.sqlast import Expression, SqlError


class HistogramAggCall(AggCall):
    def __init__(self, args: List[Expression], named_args: Dict[str, Expression]):
        if len(args) != 1:
            raise SqlError(f'function `histogram` accepts only 1 positional paramter')
        self._array = args[0]
        self._bins: Optional[Expression] = named_args.pop('bins')
        self._range = named_args.pop('range')
        # self._weights: Optional[Expression] = named_args.pop('weights')
        if len(named_args) != 0:
            raise SqlError(f'Unknown named argument: {named_args.keys()} for function `histogram`')

    def subaggrs(self, ) -> List[AggCall]:
        if self._range is None:
            self._minmax = MinMaxAggCall([self._array], {}, False)
        return [self._minmax]

    def mapper(self, context: AggContext):
        bins: int = 50 if self._bins is None else self._bins.evaluate(context)
        if self._range is not None:
            row = self._range.evaluate(context)
            if not isinstance(row, list):
                raise SqlError(f'range must be a list: {row}')
            range = row.args
        else:
            range = self._minmax.result(context)
        return numpy.histogram(self._array.evaluate(context), bins=bins, range=range)

    def reduce1(self, a):
        pass
