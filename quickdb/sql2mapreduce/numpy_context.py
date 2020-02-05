from quickdb.sql2mapreduce.sqlast.sqlast import BetweenExpression, BinaryOperationExpression, BoolExpression, ConstExpression, Context, ColumnRefExpression, FuncCallExpression, IndirectionExpression, RowExpression, SharedValueRefExpression, SqlError, UnaryOperationExpression
from typing import Dict, List, Tuple, Union

import numpy

from quickdb.sql2mapreduce.nonagg_functions import nonagg_functions
from quickdb.sspcatalog.patch import Patch

BINARY_OPERATIONS = {
    '=': lambda a, b: a == b,
    '<>': lambda a, b: a != b,
    '<': lambda a, b: a < b,
    '>': lambda a, b: a > b,
    '<=': lambda a, b: a <= b,
    '>=': lambda a, b: a >= b,
    '+': lambda a, b: a + b,
    '-': lambda a, b: a - b,
    '*': lambda a, b: a * b,
    '/': lambda a, b: a / b,
    '%': lambda a, b: a % b,
    '//': lambda a, b: a // b,
}


@Context.implementation
class NumpyContext(Context):
    def __init__(self, patch: Patch, shared: Dict = None):
        super().__init__()
        self._patch = patch
        self._shared = shared or {}

    def sliced_context(self, slice: Union[numpy.ndarray, slice]):
        return self.__class__(self._patch[slice])

    def evaluate_ConstExpression(self, e: ConstExpression):
        return e.value

    def evaluate_ColumnRefExpression(self, e: ColumnRefExpression):
        return self._patch.column(e.fields)

    def evaluate_SharedValueRefExpression(self, e: SharedValueRefExpression):
        if e.name in self._shared:
            return self._shared[e.name]
        raise SqlError(f'No such shared value: {e.name}')  # pragma: no cover

    def evaluate_UnaryOperationExpression(self, e: UnaryOperationExpression):
        a = e.a(self)
        if e.name == '-':
            return - a
        if e.name == '+':
            return a
        raise SqlError(f'Unknwon unary operator: {e.name}')  # pragma: no cover

    def evaluate_BinaryOperationExpression(self, e: BinaryOperationExpression):
        if e.name in BINARY_OPERATIONS:
            return BINARY_OPERATIONS[e.name](e.a(self), e.b(self))
        raise SqlError(f'Unknwon binary operator: {e.name}')  # pragma: no cover

    def evaluate_BetweenExpression(self, e: BetweenExpression):
        a = e.a(self)
        b = e.b(self)
        c = e.c(self)
        if e.negate:
            return numpy.logical_or(a < b, a > c)
        else:
            return numpy.logical_and(b <= a, a <= c)

    def evaluate_BoolExpression(self, e: BoolExpression):
        if e.name == 'AND':
            return numpy.logical_and.reduce([a(self) for a in e.args])
        if e.name == 'OR':
            return numpy.logical_or.reduce([a(self) for a in e.args])
        else:
            assert e.name == 'NOT' and len(e.args) == 1
            return numpy.logical_not(e.args[0](self))

    def evaluate_FuncCallExpression(self, e: FuncCallExpression):
        if e.name in nonagg_functions:
            return nonagg_functions[e.name](*[a(self) for a in e.args], **{k: a(self) for k, a in e.named_args.items()})
        raise SqlError(f'Unknown function: {e.name}')  # pragma: no cover

    def evaluate_RowExpression(self, e: RowExpression):
        return [a(self) for a in e.args]

    def evaluate_IndirectionExpression(self, e: IndirectionExpression):
        return e.arg(self)[e.index]
