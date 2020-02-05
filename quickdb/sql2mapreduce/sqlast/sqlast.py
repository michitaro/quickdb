# See https://github.com/postgres/postgres/blob/master/src/include/nodes/parsenodes.h
from abc import ABCMeta, abstractmethod
from itertools import chain
from pprint import pprint

from typing import Callable, Dict, Iterable, List, NamedTuple, Optional, Tuple, Type, TypeVar, Union, cast

from ...utils.cached_property import cached_property
from .constants import constants


def parse_sql(sql: str):
    from pglast import parse_sql
    from pglast.parser import ParseError  # pylint: disable=no-name-in-module
    try:
        return parse_sql(sql)
    except ParseError as error:  # pragma: no cover
        raise SqlError(str(error))


class SqlError(RuntimeError):
    pass


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
            val = A_Const(limitCount.pop('A_Const')).value
            if not isinstance(val, int):
                raise SqlError('Limit count must be an integer')  # pragma: no cover
            return val

    @cached_property
    def limit_offset(self) -> Optional[int]:
        if 'limitOffset' in self._meta.a:
            limitOffset = self._meta.a.pop('limitOffset')
            if 'A_Const' not in limitOffset:
                raise SqlError('Limit count must be an integer')  # pragma: no cover
            val = A_Const(limitOffset.pop('A_Const')).value
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


T = TypeVar('T')


class Context:
    @staticmethod
    def implementation(ContextFactory: T) -> T:
        for cls in expresison_classes.values():
            assert cls.evaluator_name in dir(ContextFactory), f'{ContextFactory.__name__}.{cls.evaluator_name} must be implemented.'  # type: ignore
        return ContextFactory


class Expression(metaclass=ABCMeta):
    def evaluate(self, context: Context):
        # pylint: disable=no-member
        return getattr(context, self.__class__.evaluator_name)(self)  # type: ignore

    def __call__(self, context: Context):
        return self.evaluate(context)

    @classmethod
    def from_rawast(cls, rawast) -> 'Expression':
        meta = extract_meta(rawast)
        if meta.typename not in pg_nodes:  # pragma: no cover
            raise RuntimeError(f'Unknwon pg_node: {meta.typename}. {rawast}')
        cls = pg_nodes[meta.typename]
        return cls(meta.a)

    @abstractmethod
    def _children(self) -> Iterable['Expression']:  # pragma: no cover
        ...

    def walk(self, cb: Callable[['Expression'], None], break_if: Callable[['Expression'], bool] = None):
        '''
        Walk ast tree leaf first
        '''
        if not (break_if and break_if(self)):
            for child in self._children():
                child.walk(cb, break_if)
        cb(self)

    @abstractmethod
    def __repr__(self) -> str:  # pragma: no cover
        ...


pg_nodes: Dict[str, Callable] = {}
expresison_classes: Dict[str, Type[Expression]] = {}


def pg_node(factory: T) -> T:
    pg_nodes[factory.__name__] = factory
    return factory


def expression_class(cls: T) -> T:
    expresison_classes[cls.__name__] = cls
    cls.evaluator_name = f'evaluate_{cls.__name__}'
    return cls


@expression_class
class ConstExpression(Expression):
    def __init__(self, value: Union[str, int, float]):
        self.value = value

    def _children(self) -> Iterable['Expression']:
        return []

    def __repr__(self) -> str:
        return repr(self.value)


@pg_node
def A_Const(a: Dict):
    meta = extract_meta(a['val'])
    if meta.typename == 'String':
        return ConstExpression(meta.a['str'])
    elif meta.typename == 'Float':
        return ConstExpression(float(meta.a['str']))
    elif meta.typename == 'Integer':
        return ConstExpression(meta.a['ival'])
    else:  # pragma: no cover
        raise SqlError(f'Unknwon A_Const type: {meta.typename}, a={a}')


@expression_class
class ColumnRefExpression(Expression):
    def __init__(self, fields: Tuple[str, ...]):
        self.fields = fields

    def _children(self) -> Iterable['Expression']:
        return []

    def __repr__(self) -> str:
        return f'column[{".".join(self.fields)}]'


@expression_class
class SharedValueRefExpression(Expression):
    def __init__(self, name: str):
        self.name = name

    def _children(self) -> Iterable['Expression']:
        return []

    def __repr__(self) -> str:
        return f'shared value: {self.name}'


@pg_node
def ColumnRef(a: Dict):
    fs = a.pop('fields')
    if any('String' not in f for f in fs):
        raise SqlError(f'"*" is not allowed for selecting columns')  # pragma: no cover
    fields = tuple(f['String']['str'] for f in fs)
    if fields in constants:
        return ConstExpression(constants[fields])
    if len(fields) == 2 and fields[0] == 'shared':
        return SharedValueRefExpression(fields[1])
    return ColumnRefExpression(fields)


@expression_class
class UnaryOperationExpression(Expression):
    def __init__(self, name: str, a: Expression):
        self.name = name
        self.a = a

    def _children(self) -> Iterable[Expression]:
        return [self.a]

    def __repr__(self) -> str:
        return f'{self.name} ({self.a})'


@expression_class
class BinaryOperationExpression(Expression):
    def __init__(self, name: str, a: Expression, b: Expression):
        self.name = name
        self.a = a
        self.b = b

    def _children(self) -> Iterable[Expression]:
        return [self.a, self.b]

    def __repr__(self) -> str:
        return f'({self.a}) {self.name} ({self.b})'


@expression_class
class BetweenExpression(Expression):
    def __init__(self, a: Expression, b: Expression, c: Expression, negate: bool):
        self.a = a
        self.b = b
        self.c = c
        self.negate = negate

    def _children(self) -> Iterable[Expression]:
        return [self.a, self.b, self.c]

    def __repr__(self) -> str:
        return f'({self.a}) BETWEEN {"NOT " if self.negate else ""}({self.b}) AND ({self.c})'


@pg_node
def A_Expr(a: Dict):
    kind = a.pop('kind')
    if kind == 0:
        name_tuple = tuple(s['String']['str'] for s in a.pop('name'))
        assert len(name_tuple) == 1, f'Unknown operator: '
        name = name_tuple[0]
        if 'lexpr' in a:  # binary operator
            left = Expression.from_rawast(a.pop('lexpr'))
            right = Expression.from_rawast(a.pop('rexpr'))
            e = BinaryOperationExpression(name, left, right)
        else:  # unary operator
            right = Expression.from_rawast(a.pop('rexpr'))
            e = UnaryOperationExpression(name, right)
    elif kind == 11:  # BETWEEN
        assert a.pop('name')[0]['String']['str'] == 'BETWEEN'
        rexpr = a.pop('rexpr')
        e = BetweenExpression(
            Expression.from_rawast(a.pop('lexpr')),
            Expression.from_rawast(rexpr[0]),
            Expression.from_rawast(rexpr[1]),
            False,
        )
    elif kind == 12:
        assert a.pop('name')[0]['String']['str'] == 'NOT BETWEEN'
        rexpr = a.pop('rexpr')
        e = BetweenExpression(
            Expression.from_rawast(a.pop('lexpr')),
            Expression.from_rawast(rexpr[0]),
            Expression.from_rawast(rexpr[1]),
            True,
        )
    else:  # pragma: no cover
        raise SqlError(f'Unknwon expr name: {a}')
    a.pop('location')
    assert len(a) == 0, (a, kind)
    return e


@expression_class
class BoolExpression(Expression):
    def __init__(self, name: str, args: List[Expression]):
        self.name = name
        self.args = args

    def _children(self):
        return self.args

    def __repr__(self) -> str:
        if self.name == 'NOT':
            return f'NOT ({self.args[0]})'
        else:
            return f' {self.name} '.join(f'({a})' for a in self.args)


@pg_node
def BoolExpr(a: Dict):
    op = {
        0: 'AND',
        1: 'OR',
        2: 'NOT',
    }[a.pop('boolop')]
    args = [Expression.from_rawast(arg) for arg in a.pop('args')]
    a.pop('location')
    assert len(a) == 0, a
    return BoolExpression(op, args)


@expression_class
class FuncCallExpression(Expression):
    def __init__(self, name: Tuple[str, ...], args: List[Expression], named_args: Dict[str, Expression], agg_star: bool):
        self.name = name
        self.args = args
        self.named_args = named_args
        self.agg_star = agg_star

    def _children(self) -> Iterable['Expression']:
        return chain(self.args, self.named_args.values())

    def __repr__(self) -> str:
        funcname = '.'.join(self.name)
        if self.agg_star:
            return f'{funcname}(*)'
        else:
            return f'{funcname}(*{self.args}, **{self.named_args})'


@pg_node
def FuncCall(a: Dict):
    a.pop('location')
    funcname = tuple(s['String']['str'] for s in a.pop('funcname'))
    agg_star = a.pop('agg_star', False)
    raw_args = [] if agg_star else [Expression.from_rawast(arg) for arg in a.pop('args')]
    args = [cast(Expression, e) for e in raw_args if not isinstance(e, NamedArg)]
    named_args0: List[NamedArg] = [e for e in raw_args if isinstance(e, NamedArg)]
    named_args = {e.name: e.arg for e in named_args0}
    if len(named_args) != len(named_args0):  # pragma: no cover
        raise SqlError(f'Argument names must be unique for function `{funcname}`')
    assert len(a) == 0, a
    return FuncCallExpression(funcname, args, named_args, agg_star)


class NamedArg(NamedTuple):
    name: str
    arg: Expression


@pg_node
def NamedArgExpr(a: Dict):
    a.pop('location')
    arg = Expression.from_rawast(a.pop('arg'))
    name = a.pop('name')
    assert a.pop('argnumber') == -1, a
    assert len(a) == 0, a
    return NamedArg(name, arg)


@expression_class
class RowExpression(Expression):
    def __init__(self, args: List[Expression]):
        self.args = args

    def _children(self) -> Iterable['Expression']:
        return self.args

    def __repr__(self) -> str:
        return str(self.args)


@pg_node
def RowExpr(a: Dict):
    a.pop('location')
    assert a.pop('row_format') == 2
    args = [Expression.from_rawast(arg) for arg in a.pop('args')]
    return RowExpression(args)


@expression_class
class IndirectionExpression(Expression):
    def __init__(self, arg: Expression, index: int):
        self.arg = arg
        self.index = index

    def _children(self) -> Iterable['Expression']:
        return [self.arg]

    def __repr__(self) -> str:
        return f'({self.arg})[{self.index}]'


@pg_node
def A_Indirection(a: Dict):
    arg = Expression.from_rawast(a.pop('arg'))
    indices = a.pop('indirection')
    if len(indices) > 1:
        raise SqlError(f'Nested indices are not supported')  # pragma: no cover
    index = indices[0]['A_Indices']
    if 'is_slice' in index:
        raise SqlError(f'Slicing is not supported')  # pragma: no cover
    uidx = A_Const(index.pop('uidx')['A_Const'])
    assert len(index) == 0, index
    if not isinstance(uidx.value, int):
        raise SqlError(f'Index value must be an integer. given: {uidx.value}')  # pragma: no cover
    return IndirectionExpression(arg, uidx.value)
