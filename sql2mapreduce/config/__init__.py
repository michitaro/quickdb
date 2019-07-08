from .. import sqlast
from . import funcs
from . import sqlfuncs
from . import aggregators
from .pycodebuilder import PycodeBuilder, is_series_ast


def preamble(stmt):
    sqlast.check(stmt.fromClause, 'missing "FROM" clause')
    sqlast.check(len(stmt.fromClause) == 1, 'table must be pdr1_wide, pdr1_deep or pdr1_udeep')
    fc = stmt.fromClause[0]
    rerun = fc.schemaname if fc.schemaname else fc.relname
    return f'rerun = {repr(rerun)}'
