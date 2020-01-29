# See https://github.com/postgres/postgres/blob/master/src/include/nodes/parsenodes.h
from itertools import chain
from pprint import pprint
from typing import (Any, Callable, Dict, Iterable, Iterator, List, Optional,
                    Tuple, TypeVar, Union, cast)

import numpy

from ...utils.cached_property import cached_property


class SqlError(RuntimeError):
    pass


class Context:
    '''
    A Context object gives bindings to Expression ASTs of Select object when they are evaluated.
    e.g. which file should be bound to columnref ('forced', 'i', 'psfflux_flux').
    '''

    def columnref(self, ref: Tuple[str, ...]):
        raise NotImplementedError(f'{self.__class__}.columnref must be implemented')  # pragma: no cover

    def binary_operation(self, name: str, left, right):
        raise NotImplementedError(f'{self.__class__}.binary_operation must be implemented')  # pragma: no cover

    def unary_operation(self, name: str, right):
        raise NotImplementedError(f'{self.__class__}.unary_operation must be implemented')  # pragma: no cover

    def between(self, a, b, c):
        raise NotImplementedError(f'{self.__class__}.between must be implemented')  # pragma: no cover

    def boolean_operation(self, op: str, args: List):
        raise NotImplementedError(f'{self.__class__}.boolop must be implemented')  # pragma: no cover

    def funccall(self, name: Tuple[str, ...], args: List, named_args: Dict, agg_star: bool, expression: 'Expression'):
        raise NotImplementedError(f'{self.__class__}.funccall must be implemented')  # pragma: no cover

    def indirection(self, arg, index: int):
        raise NotImplementedError(f'{self.__class__}.indirection must be implemented')  # pragma: no cover

    def sliced_context(self, slice: Union[slice, numpy.ndarray]) -> 'Context':
        raise NotImplementedError(f'{self.__class__}.sliced_context must be implemented')  # pragma: no cover


class Select:
    def __init__(self, sql: str, print=False):
        self._sql = sql
        statements = parse_sql(sql)
        if len(statements) != 1:
            raise SqlError(f'Multiple statements are not allowed: {sql}')  # pragma: no cover
        stmt = statements[0]['RawStmt']['stmt']
        if print:
            pprint(stmt)  # pragma: no cover
        meta = extract_meta(stmt)
        self._meta = meta
        if meta.typename != 'SelectStmt':
            raise SqlError('Only SELECT statements are supported')  # pragma: no cover
        self._meta = meta
        # populate members
        self.target_list
        self.from_clause
        self.where_clause
        self.sort_clause
        self.limit_count
        self.limit_offset
        self.group_clause
        # check if all clauses have been walked
        assert meta.a.pop('op') == 0
        if len(meta.a) > 0:
            raise SqlError(f'Unknown syntax: {meta.a}')  # pragma: no cover

    @cached_property
    def target_list(self):
        if 'targetList' not in self._meta.a:
            raise SqlError(f'target list must be specified')  # pragma: no cover
        targetList = self._meta.a.pop('targetList')
        return [ResTarget(t['ResTarget']) for t in targetList]

    @cached_property
    def from_clause(self):
        if 'fromClause' not in self._meta.a:
            raise SqlError(f'from clause must be specified')  # pragma: no cover
        fromClause = self._meta.a.pop('fromClause')
        if len(fromClause) != 1:
            raise SqlError(f'Multiple from clauses are not allowed')  # pragma: no cover
        meta = extract_meta(fromClause[0])
        if meta.typename != 'RangeVar':
            raise SqlError(f'Not supported syntax in from clause')  # pragma: no cover
        return RangeVar(meta.a)

    @cached_property
    def where_clause(self):
        if 'whereClause' in self._meta.a:
            whereClause = self._meta.a.pop('whereClause')
            return Expression.from_rawast(whereClause)

    @cached_property
    def sort_clause(self):
        if 'sortClause' in self._meta.a:
            sortClause = self._meta.a.pop('sortClause')
            return [SortBy(s['SortBy']) for s in sortClause]

    @cached_property
    def limit_count(self) -> Optional[int]:
        if 'limitCount' in self._meta.a:
            limitCount = self._meta.a.pop('limitCount')
            if 'A_Const' not in limitCount:
                raise SqlError('Limit count must be an integer')  # pragma: no cover
            val = A_Const(limitCount.pop('A_Const')).val
            if not isinstance(val, int):
                raise SqlError('Limit count must be an integer')  # pragma: no cover
            return val

    @cached_property
    def limit_offset(self) -> Optional[int]:
        if 'limitOffset' in self._meta.a:
            limitOffset = self._meta.a.pop('limitOffset')
            if 'A_Const' not in limitOffset:
                raise SqlError('Limit count must be an integer')  # pragma: no cover
            val = A_Const(limitOffset.pop('A_Const')).val
            if not isinstance(val, int):
                raise SqlError('Limit offset must be an integer')  # pragma: no cover
            return val

    @cached_property
    def group_clause(self):
        if 'groupClause' in self._meta.a:
            groupClause = self._meta.a.pop('groupClause')
            return [Expression.from_rawast(gc) for gc in groupClause]


class SortBy:
    def __init__(self, a: Dict):
        self.node = Expression.from_rawast(a.pop('node'))
        self.sortby_dir = a.pop('sortby_dir')
        if a.pop('sortby_nulls'):
            raise SqlError('Syntax `NULLS {FIRST} | {LAST}` is not supported')  # pragma: no cover
        assert a.pop('location') == -1 and len(a) == 0, a

    @cached_property
    def reverse(self):
        return self.sortby_dir == 2


class extract_meta:
    def __init__(self, rawast):
        assert len(rawast) == 1
        typename = next(iter(rawast))
        self.typename: str = typename
        self.a: Dict = rawast[typename]  # `a` stands for attributes


class ResTarget:
    def __init__(self, a: Dict):
        self.val: Expression = Expression.from_rawast(a.pop('val'))
        self.name: Optional[str] = a.pop('name', None)
        assert a.pop('location') >= 0
        assert len(a) == 0, a


class RangeVar:
    def __init__(self, a):
        self._a = a
        if 'schemaname' in a:
            raise SqlError(f'Schema cannot be specified: {a["schemaname"]}')  # pragma: no cover
        self.relname: str = a.pop('relname')
        # populate self.alias
        self.alias
        assert a.pop('inh') is True and a.pop('relpersistence') == 'p' and a.pop('location') >= 0
        assert len(a) == 0, a

    @cached_property
    def alias(self):
        if 'alias' in self._a:
            alias = self._a.pop('alias')
            return alias['Alias']['aliasname']


class Expression:
    '''
    Represents an expression ast
    '''

    def __init__(self, a):
        raise RuntimeError(f'{self.__class__.__name__}.__init__ must be implemented: a={a}')  # pragma: no cover

    def evaluate(self, context: Context) -> ...:
        raise RuntimeError(f'{self.__class__.__name__}.evaluate must be implemented')  # pragma: no cover

    def walk(self, cb: Callable[['Expression'], None]):
        '''
        Walk ast tree leaf first
        '''
        for child in self._children():
            child.walk(cb)
        cb(self)

    def _children(self) -> Iterable['Expression']:
        raise NotImplementedError(f'{self.__class__}._children must be implemented')  # pragma: no cover

    @staticmethod
    def from_rawast(rawast) -> 'Expression':
        meta = extract_meta(rawast)
        cls = expression_classes[meta.typename]
        return cls(meta.a)

    @property
    def location(self) -> int:
        if hasattr(self, '_location'):
            return cast(Any, self)._location  # pylint: disable=no-member
        return -1


expression_classes: Dict = {}


T = TypeVar('T')


def expression_class(cls: T) -> T:
    expression_classes[cls.__name__] = cls
    return cls


@expression_class
class A_Const(Expression):
    def __init__(self, a):
        meta = extract_meta(a['val'])
        self.typename = meta.typename
        if meta.typename == 'String':
            self.val = meta.a['str']
        elif meta.typename == 'Float':
            self.val = float(meta.a['str'])
        elif meta.typename == 'Integer':
            self.val = meta.a['ival']
        else:
            raise SqlError(f'Unknwon A_Const type: {meta.typename}, a={a}')  # pragma: no cover

    def evaluate(self, context: Context):
        return self.val

    def _children(self) -> Iterable['Expression']:
        return []


@expression_class
class ColumnRef(Expression):
    def __init__(self, a):
        fields = a.pop('fields')
        if any('String' not in f for f in fields):
            raise SqlError(f'"*" is not allowed for selecting columns')  # pragma: no cover
        self.fields: Tuple[str, ...] = tuple(f['String']['str'] for f in fields)

    def evaluate(self, context: Context):
        return context.columnref(self.fields)

    def _children(self) -> Iterable['Expression']:
        return []


class BinaryOperationExpression(Expression):
    def __init__(self, name: str, left: Expression, right: Expression):
        self._name = name
        self._left = left
        self._right = right

    def evaluate(self, context: Context):
        return context.binary_operation(
            self._name,
            self._left.evaluate(context),
            self._right.evaluate(context),
        )

    def _children(self) -> Iterable['Expression']:
        return [self._left, self._right]


class UnaryOperationExpression(Expression):
    def __init__(self, name: str, right: Expression):
        self._name = name
        self._right = right

    def evaluate(self, context: Context):
        return context.unary_operation(self._name, self._right.evaluate(context))

    def _children(self) -> Iterable['Expression']:
        return [self._right]


class BetweenExpression(Expression):
    def __init__(self, a: Expression, b: Expression, c: Expression):
        self._a = a
        self._b = b
        self._c = c

    def evaluate(self, context: Context):
        return context.between(
            self._a.evaluate(context),
            self._b.evaluate(context),
            self._c.evaluate(context),
        )

    def _children(self) -> Iterable['Expression']:
        return [self._a, self._b, self._c]


@expression_class
class A_Expr(Expression):

    def __init__(self, a: Dict):
        self._location = a.pop('location')
        kind = a.pop('kind')
        if kind == 0:
            name_tuple = tuple(s['String']['str'] for s in a.pop('name'))
            assert len(name_tuple) == 1, f'Unknown operator: '
            name = name_tuple[0]
            if 'lexpr' in a:  # binary operator
                left = Expression.from_rawast(a.pop('lexpr'))
                right = Expression.from_rawast(a.pop('rexpr'))
                self._expr = BinaryOperationExpression(name, left, right)
            else:  # unary operator
                right = Expression.from_rawast(a.pop('rexpr'))
                self._expr = UnaryOperationExpression(name, right)
        elif kind == 11:  # BETWEEN
            assert a.pop('name')[0]['String']['str'] == 'BETWEEN'
            rexpr = a.pop('rexpr')
            self._expr = BetweenExpression(
                Expression.from_rawast(a.pop('lexpr')),
                Expression.from_rawast(rexpr[0]),
                Expression.from_rawast(rexpr[1]),
            )
        elif kind == 12:  # pragma: no cover
            assert a.pop('name')[0]['String']['str'] == 'NOT BETWEEN'
            raise SqlError(f'NOT BETWEEN Syntax is not supported. Use "NOT (a BETWEEN b AND c)" instead of "a NOT BETWEEN b AND c"')
        assert len(a) == 0, (a, kind)

    def evaluate(self, context: Context):
        return self._expr.evaluate(context)

    def _children(self) -> Iterable['Expression']:
        return [self._expr]


@expression_class
class BoolExpr(Expression):
    def __init__(self, a: Dict):
        self._location = a.pop('location')
        self._op = {
            0: 'AND',
            1: 'OR',
            2: 'NOT',
        }[a.pop('boolop')]
        self._args = [Expression.from_rawast(arg) for arg in a.pop('args')]
        assert len(a) == 0, a

    def evaluate(self, context: Context):
        return context.boolean_operation(self._op, [a.evaluate(context) for a in self._args])

    def _children(self) -> Iterable['Expression']:
        return self._args


@expression_class
class FuncCall(Expression):
    def __init__(self, a: Dict):
        self._location = a.pop('location')
        self.funcname = tuple(s['String']['str'] for s in a.pop('funcname'))
        agg_star = a.pop('agg_star', False)
        self._agg_star = agg_star
        args = [] if agg_star else [Expression.from_rawast(arg) for arg in a.pop('args')]
        self.args = [cast(Expression, e) for e in args if not isinstance(e, NamedArgExpr)]
        named_args: List[NamedArgExpr] = [e for e in args if isinstance(e, NamedArgExpr)]
        self.named_args = {e.name: e.arg for e in named_args}
        if len(named_args) != len(self.named_args):
            raise SqlError(f'Argument names must be unique for function `{self._funcname}`')
        assert len(a) == 0, a

    def evaluate(self, context: Context):
        args = [arg.evaluate(context) for arg in self.args]
        named_args = {k: v.evaluate(context) for k, v in self.named_args.items()}
        return context.funccall(self.funcname, args, named_args, self._agg_star, self)

    def _children(self) -> Iterable['Expression']:
        return chain(self.args, self.named_args.values())


@expression_class
class NamedArgExpr(Expression):
    def __init__(self, a: Dict):
        self._location = a.pop('location')
        self.arg = Expression.from_rawast(a.pop('arg'))
        self.name = a.pop('name')
        assert a.pop('argnumber') == -1, a
        assert len(a) == 0, a


class Row:
    def __init__(self, *args):
        self.args = tuple(args)

    def __eq__(self, other: 'Row'):
        return isinstance(other, Row) and self.args == other.args


@expression_class
class RowExpr(Expression):
    def __init__(self, a: Dict):
        self._location = a.pop('location')
        assert a.pop('row_format') == 2
        self._args = [Expression.from_rawast(arg) for arg in a.pop('args')]
        assert len(a) == 0

    def evaluate(self, context: Context):
        return Row(*(a.evaluate(context) for a in self._args))

    def _children(self) -> Iterable['Expression']:
        return self._args


@expression_class
class A_Indirection(Expression):
    def __init__(self, a: Dict):
        self._arg = Expression.from_rawast(a.pop('arg'))
        indices = a.pop('indirection')
        if len(indices) > 1:
            raise SqlError(f'Nested indices are not supported')  # pragma: no cover
        index = indices[0]['A_Indices']
        if 'is_slice' in index:
            raise SqlError(f'Slicing is not supported')  # pragma: no cover
        uidx = A_Const(index.pop('uidx')['A_Const'])
        assert len(index) == 0, index
        if uidx.typename != 'Integer':
            raise SqlError(f'Index value must be an integer. given: {uidx.val}')  # pragma: no cover
        self._index = cast(int, uidx.val)

    def evaluate(self, context: Context):
        return context.indirection(self._arg.evaluate(context), self._index)

    def _children(self) -> Iterable['Expression']:
        return [self._arg]


def parse_sql(sql: str):
    from pglast import parse_sql
    from pglast.parser import ParseError  # pylint: disable=no-name-in-module
    try:
        return parse_sql(sql)
    except ParseError as error:  # pragma: no cover
        raise SqlError(str(error))
