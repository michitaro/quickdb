from typing import Dict, List, Tuple, Union

import numpy

from quickdb.sql2mapreduce.nonagg_functions import nonagg_functions
from quickdb.sql2mapreduce.sqlast import Context
from quickdb.sql2mapreduce.sqlast.sqlast import Expression, SqlError
from quickdb.sspcatalog.patch import Patch


class NumpyContext(Context):
    def __init__(self, patch: Patch):
        super().__init__()
        self._patch = patch

    def columnref(self, ref: Tuple[str, ...]):
        return self._patch.column(ref)

    def binary_operation(self, name: str, left, right):
        ops = {
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
        if name not in ops:
            raise SqlError(f'Unknown binary operator {name}')  # pragma: no cover
        return ops[name](left, right)

    def unary_operation(self, name: str, right):
        ops = {
            '+': lambda a: a,
            '-': lambda a: -a,
        }
        if name not in ops:
            raise SqlError(f'Unknown unary operator {name}')  # pragma: no cover
        return ops[name](right)

    def between(self, a, b, c):
        return numpy.logical_and(b <= a, a <= c)

    def boolean_operation(self, name, args: List):
        ops = {
            'AND': lambda args: numpy.logical_and(args[0], args[1]),
            'OR': lambda args: numpy.logical_or(args[0], args[1]),
            'NOT': lambda args: numpy.logical_not(args[0]),
        }
        if name not in ops:
            raise SqlError(f'Unknown boolean operator {name}')  # pragma: no cover
        return ops[name](args)

    def funccall(self, name: Tuple[str, ...], args: List, named_args: Dict, agg_star: bool, expression: Expression):
        if name not in nonagg_functions:  # pragma: no cover
            raise SqlError(f'No such function: {name}')
        if agg_star:
            raise SqlError(f'"*" is not allowed here: {".".join(name)}')
        return nonagg_functions[name](*args, **named_args)

    def indirection(self, arg, index: int):
        if len(arg.shape) != 2:  # pragma: no cover
            raise SqlError(f'Invali use of [{index}]')
        return arg[index]

    def sliced_context(self, slice: Union[numpy.ndarray, slice]):
        return self.__class__(self._patch[slice])
