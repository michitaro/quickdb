import textwrap
import json


class Ast(object):
    SKIP_ATTRS = 'location'.split()

    @classmethod
    def build_from_raw(cls, raw_attrs):
        self = cls()
        self.raw = raw_attrs
        raw_attrs = dict(raw_attrs) # we use a copy because raw_attr will be changed (poped) in this method.
        for a in self.attrs:
            raw_attr = raw_attrs.pop(a.name, None)
            if raw_attr is None:
                check(a.optional, f'missing attribute: {self.__class__.__name__}.{a.name}')
                setattr(self, a.name, None)
            else:
                if isinstance(raw_attr, list):
                    setattr(self, a.name, rawarray2ast(raw_attr))
                else:
                    if a.primitive:
                        setattr(self, a.name, raw_attr)
                    else:
                        setattr(self, a.name, raw2ast(raw_attr))
        if len([k for k in raw_attrs.keys() if k not in self.SKIP_ATTRS]) > 0:
            print(self, raw_attrs)
        self._after_build()
        return self

    @classmethod
    def build(cls, **kwargs):
        self = cls()
        for k, v in kwargs.items():
            setattr(self, k, v)
        for a in self.attrs:
            if not hasattr(self, a.name):
                setattr(self, a.name, None)
        self._after_build()
        return self

    def walk(self, f, break_if=None, leaf_first=False):
        if break_if is not None and break_if(self):
            return
        if not leaf_first:
            f(self)
        for attr in self.attrs:
            a = getattr(self, attr.name)
            if a is not None:
                if isinstance(a, Ast):
                    a.walk(f, break_if, leaf_first)
                else:
                    if isinstance(a, list):
                        for aa in a:
                            if isinstance(aa, Ast):
                                aa.walk(f, break_if, leaf_first)
        if leaf_first:
            f(self)

    def _after_build(self):
        pass

    def __repr__(self):
        return f'<{ self.__class__.__name__ } { json.dumps(self.raw) }>'


class Attr(object): 
    def __init__(self, name, optional=False, primitive=False):
        self.name = name
        self.optional = optional
        self.primitive = primitive


class SqlError(RuntimeError): pass


class RawStmt(Ast):
    attrs = [
        Attr('stmt'),
        Attr('stmt_len', primitive=True, optional=True),
        ]


class SelectStmt(Ast):
    attrs = [
        Attr('targetList'),
        Attr('op', primitive=True),
        Attr('fromClause', optional=True),
        Attr('whereClause', optional=True),
        Attr('groupClause', optional=True),
        Attr('havingClause', optional=True),
        Attr('sortClause', optional=True),
        Attr('limitCount', optional=True),
        Attr('limitOffset', optional=True),
        ]


class VariableSetStmt(Ast):
    attrs = [
        Attr('kind', primitive=True),
        Attr('name', primitive=True),
        Attr('args'),
        ]


class ResTarget(Ast):
    attrs = [
        Attr('val'),
        Attr('name', optional=True, primitive=True),
        ]


class SortBy(Ast):
    attrs = [
        Attr('node'),
        Attr('sortby_dir', primitive=True),
        Attr('sortby_nulls', primitive=True),
    ]


class RangeVar(Ast):
    attrs = [
        Attr('relname', primitive=True),
        Attr('schemaname', optional=True, primitive=True),
        Attr('inh', primitive=True),
        Attr('relpersistence', primitive=True),
        Attr('alias', optional=True),
    ]


class Alias(Ast):
    attrs =[Attr('aliasname', primitive=True)]


class ColumnRef(Ast):
    attrs = [Attr('fields')]


class RowExpr(Ast):
    attrs = [
        Attr('args'),
        Attr('row_format', primitive=True),
        ]


class A_Expr(Ast):
    attrs = [
        Attr('kind', primitive=True),
        Attr('name'),
        Attr('lexpr'),
        Attr('rexpr', optional=True),
        ]


class BoolExpr(Ast):
    BOOLOP = ['and', 'or', 'not']

    attrs = [
        Attr('boolop', primitive=True),
        Attr('args'),
        ]

    def _after_build(self):
        self.op_type = self.BOOLOP[self.boolop]


class FuncCall(Ast):
    attrs = [
        Attr('funcname'),
        Attr('args', optional=True),
        Attr('agg_star', optional=True, primitive=True)]

    def _after_build(self):
        assert all(isinstance(f, String) for f in self.funcname)
        self.name_tuple = tuple(s.str for s in self.funcname)

    @property
    def named_args(self):
        return {a.name: a.arg for a in self.args if isinstance(a, NamedArgExpr)}

    @property
    def nameless_args(self):
        return [a for a in self.args if not isinstance(a, NamedArgExpr)]


class NamedArgExpr(Ast):
    attrs = [
        Attr('arg'),
        Attr('name', primitive=True),
        Attr('argnumber', primitive=True),
        ]


class A_Indirection(Ast):
    attrs = [
        Attr('arg'),
        Attr('indirection'),
        ]


class A_Indices(Ast):
    attrs = [
        Attr('uidx', optional=True),
        Attr('is_slice', optional=True, primitive=True),
        Attr('lidx', optional=True),
        ]


class A_Const(Ast):
    attrs = [Attr('val')]


class A_Star(Ast):
    attrs = []


class TypeCast(Ast):
    attrs = [
        Attr('arg'),
        Attr('typeName'),
    ]


class TypeName(Ast):
    attrs = [
        Attr('names'),
        Attr('typemod', primitive=True),
    ]


class Integer(Ast):
    attrs = [Attr('ival', primitive=True)]


class Float(Ast):
    attrs = [Attr('str', primitive=True)]


class String(Ast):
    attrs = [Attr('str', primitive=True)]


class Null(Ast):
    attrs = []


class NullTest(Ast):
    attrs= [
        Attr('arg'),
        Attr('nulltesttype', primitive=True),
    ]

    def _after_build(self):
        self.yes = self.nulltesttype == 0


# dict of subclasses of Ast
ast_classes = {k: v  for k, v in locals().items() if type(v) == type and Ast in v.mro()}


def pretty_print(ast, show_omitted=False):
    print(ast.__class__.__name__)
    print('-' * len(ast.__class__.__name__))
    return _pretty_print(ast, 0, show_omitted)


def _pretty_print(ast, indent, show_omitted):
    I = '    '
    for a in ast.attrs:
        v = getattr(ast, a.name, None)
        if v is None:
            if show_omitted:
                print(I * indent + f'{a.name}: {v.__class__.__name__} => None')
        else:
            if isinstance(v, list):
                for i, vv in enumerate(v):
                    print(I * indent + f'{a.name}[{i}]: {vv.__class__.__name__} => ')
                    _pretty_print(vv, indent + 1, show_omitted)
            else:
                if a.primitive:
                    print(I * indent + f'{a.name}: => {repr(v)}')
                else:
                    print(I * indent + f'{a.name}: {v.__class__.__name__} => ')
                    _pretty_print(v, indent + 1, show_omitted)
pp = pretty_print


def raw2ast(raw):
    if not isinstance(raw, dict):
        import ipdb ; ipdb.set_trace()
    assert len(raw) == 1
    type_name, raw_attrs = next(iter(raw.items()))
    check(type_name in ast_classes, f'Unknown ast type: {type_name}')
    return ast_classes[type_name].build_from_raw(raw_attrs)


def rawarray2ast(rawarray):
    return [raw2ast(raw) for raw in rawarray]


def parse_sql(sql):
    import pg_query
    return rawarray2ast(pg_query.parse_sql(sql))


def check(condition, message):
    if not condition:
        raise SqlError(message)


if __name__ == '__main__':
    sql = '''
    SELECT
        ref.id, ref.coord
    FROM
        pdr1_wide
    WHERE
        flux_to_mag(forced.i.flux_sinc) <= 25
    ORDER BY
        forced.i.flux_sinc
    LIMIT
        100
    '''
    print(sql)
    pp(parse_sql(sql)[0])
