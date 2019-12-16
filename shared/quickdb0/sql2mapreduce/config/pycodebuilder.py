import re
from ..sqlast import *
from . import sqlfuncs
from .. import pycodebuilder


class PycodeBuilder(pycodebuilder.PycodeBuilder):
    def __init__(self, stmt, t, a='a'):
        super().__init__(stmt, t, a)
        fromClause = self.stmt.fromClause[0]
        self.column_base = [fromClause.relname] if fromClause.schemaname else []

    def pycode(self, ast):
        # if not hasattr(self, f'{ast.__class__.__name__}_to_pycode'):
        #     import ipdb ; ipdb.set_trace()
        if ast is None:
            return 'None'
        return getattr(self, f'{ast.__class__.__name__}_to_pycode')(ast)

    def __call__(self, ast):
        return self.pycode(ast)

    def A_Const_to_pycode(self, ast):
        return self(ast.val)

    def ColumnRef_to_pycode(self, ast):
        assert all(isinstance(f, String) for f in ast.fields)
        key = tuple(f.str for f in ast.fields)
        if len(key) == 2 and key[0] == 'shared':
            return f'shared[{ repr(key[1]) }]'
        if key in consts:
            return repr(consts[key])
        return f'{self.t}.column(' + repr(self.resolve_colmun_ref(ast)) + ')'

    def resolve_colmun_ref(self, ast):
        ref = tuple(self.column_base + [f.str for f in ast.fields])
        # check(len(ref) >= 2, f'no such column: {repr(".".join(ref))}')
        return ref

    def FuncCall_to_pycode(self, ast):
        key = ast.name_tuple
        check(key in sqlfuncs.mapping, f'no such function: {".".join(key)}')
        real_name = sqlfuncs.mapping[key]
        args = []
        named_args = []
        for a in ast.args:
            if isinstance(a, NamedArgExpr):
                name = re.sub('\W', '_', a.name)
                named_args.append(f'{name}={self(a.arg)}')
            else:
                args.append(self(a))
        return f'sqlfuncs.{ real_name }({ l2s(args + named_args) })'

    def A_Indirection_to_pycode(self, ast):
        check(len(ast.indirection) == 1, 'not supported syntax: x[y][z]')
        check(not ast.indirection[0].is_slice, 'not supported syntax: x[y:z]')
        return f'({ self(ast.arg) })[:, { self(ast.indirection[0].uidx) }]'

    def BoolExpr_to_pycode(self, ast):
        if ast.op_type in ['and', 'or']:
            exprs = [self(a) for a in ast.args]
            return f'funcs.logical_{ ast.op_type }({ l2s(exprs) })'
        elif ast.op_type == 'not':
            return f'numpy.logical_not({ self(ast.args[0]) })'

    # def BoolExpr_to_pycode(self, ast):
    #     if ast.op_type in ['and', 'or']:
    #         flags = []
    #         exprs = []
    #         for a in ast.args:
    #             flag = self.is_flag(a)
    #             if flag: # OPTIMZE
    #                 flags.append(flag)
    #             else:
    #                 exprs.append(a)
    #         exprs = [self(e) for e in exprs]
    #         if len(flags) > 0:
    #             exprs.append(f'{ self.t }.flags_{ ast.op_type }(*{ repr(flags) })')
    #         return f'funcs.logical_{ ast.op_type }({ l2s(exprs) })'
    #     elif ast.op_type == 'not':
    #         return f'numpy.logical_not({ self(ast.args[0]) })'

    # def is_flag(self, ast):
    #     if isinstance(ast, ColumnRef):
    #         return (self.resolve_colmun_ref(ast), True)
    #     if isinstance(ast, BoolExpr) and ast.op_type == 'not' and isinstance(ast.args[0], ColumnRef):
    #         return (self.resolve_colmun_ref(ast.args[0]), False)

    def A_Expr_to_pycode(self, ast):
        if ast.kind == 0:
            if ast.name[0].str == '!':
                return f'funcs.factorial({ self(ast.lexpr) })'
            pyoperator = {
                '=': '==', '<>': '!=',
                '<': '<',
                '<=': '<=',
                '>': '>',
                '>=': '>=',
                '+': '+',
                '-': '-',
                '*': '*',
                '/': '/',
                '//': '//',
            }[ast.name[0].str]
            return f'({ self(ast.lexpr) }) { pyoperator } ({ self(ast.rexpr) })'
        elif ast.kind == 11: # between
            return f'funcs.between({ self(ast.lexpr) }, { self(ast.rexpr[0]) }, { self(ast.rexpr[1]) })'
        elif ast.kind == 7: # in
            raise SqlError('not implemented')
        else:
            raise SqlError(f'unknown kind: {ast.kind}')

    def Integer_to_pycode(self, ast):
        return repr(ast.ival)

    def Float_to_pycode(self, ast):
        # pg_query classifies big integer such as 37484559004074072 as a float
        # return repr(float(ast.str))
        return ast.str

    def String_to_pycode(self, ast):
        return repr(ast.str)

    def RowExpr_to_pycode(self, ast):
        pyargs = [self(a) for a in ast.args]
        return f'({ l2s(pyargs) },)'

    def NullTest_to_pycode(self, ast):
        check(isinstance(ast.arg, ColumnRef), f'null test can be performed on only a ColumnRef')
        return f'{ self.t }.null_check({ repr(self.resolve_colmun_ref(ast.arg)) }, { ast.yes })'

    def AggregateResultAst_to_pycode(self, ast):
        return f'AggregateResult[{ast.name}][{self.a}.group_value].result()'

    def args_pycode(self, *args, **kwargs):
        s = [self(a) for a in args]
        for k, v in kwargs.items():
            if v is not None:
                k = re.sub('\W', '_', k)
                s.append(f'{k}={self(v)}')
        return l2s(s)


def is_series_ast(ast):
    return any(isinstance(ast, A) for A in [ColumnRef, NullTest])


def l2s(l):
    return ', '.join(l)


import numpy


consts = {
    ('pi',): numpy.pi,
    ('e',): numpy.e,
    ('degree',): 180 / numpy.pi,
    ('arcmin',): 60 * 180 / numpy.pi,
    ('arcsec',): 3600 * 180 / numpy.pi,
}


def ast2pycode(ast, stmt, t):
    b = PycodeBuilder(stmt, t)
    return b.pycode(ast)
