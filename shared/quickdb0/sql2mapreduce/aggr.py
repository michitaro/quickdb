import textwrap
import numpy
from . import sqlast
from . import config


from .nonaggr import target_names


def sql2make_envs(sql, print_ast=False):
    stmt = safe_stmt(sql)
    if print_ast:
        sqlast.pp(stmt)
    check_ast_args(stmt)
    wrap_series_ast(stmt)
    aggr_calls = pick_aggr_calls(stmt)
    make_envs = [aggr_call_to_make_env(ac, stmt) for ac in aggr_calls]
    return make_envs, stmt, target_names(stmt)


def safe_stmt(sql):
    stmts = sqlast.parse_sql(sql)
    sqlast.check(len(stmts) == 1, 'Too many statements')
    stmt = stmts[0].stmt
    sqlast.check(isinstance(stmt, sqlast.SelectStmt), f'Not supported statement: {stmt.__class__.__name__}')
    safe_attrs = set('targetList fromClause whereClause groupClause op'.split())
    for a in set(aa.name for aa in stmt.attrs) - safe_attrs:
        sqlast.check(getattr(stmt, a) is None, f'{a} is not supported in aggregation query')
    return stmt


def check_ast_args(ast):
    mapping = config.aggregators.mapping
    def f(ast):
        if isinstance(ast, sqlast.FuncCall) and ast.name_tuple in mapping:
            Aggregator = mapping[ast.name_tuple]
            Aggregator.check_ast_args(ast)
    ast.walk(f)


def is_aggr_call(ast):
    return isinstance(ast, sqlast.FuncCall) and ast.name_tuple in config.aggregators.mapping


def wrap_series_ast(stmt):
    series_ast = set()
    def break_if(ast):
        return is_aggr_call(ast)
    def f(ast):
        if config.is_series_ast(ast):
            series_ast.add(ast)
    stmt.walk(f, break_if)
    def g(node):
        if isinstance(node, sqlast.Ast) and node in series_ast:
            return config.aggregators.PickOneAggregator.build_call_ast(node)
    walk_and_replace_node(g, stmt.targetList)


def pick_aggr_calls(stmt):
    aggr_calls = []
    def f(ast):
        if is_aggr_call(ast):
            # ast doesn't contain child aggregate funccalls because we walks leaf first.
            Aggregator = config.aggregators.mapping[ast.name_tuple]
            ref = AggregateResultAst(len(aggr_calls), Aggregator, ast)
            aggr_calls.append(ref)
            return ref
    walk_and_replace_node(f, stmt.targetList)
    return aggr_calls


def walk_and_replace_node(f, node):
    if isinstance(node, list):
        for i, a in enumerate(node):
            walk_and_replace_node(f, a) # list elements are always asts
            newval = f(a)
            if newval is not None:
                node[i] = newval
    else:
        assert isinstance(node, sqlast.Ast)
        for attr in node.attrs:
            a = getattr(node, attr.name)
            if a is not None:
                if not attr.primitive:
                    walk_and_replace_node(f, a)
                    newval = f(a)
                    if newval is not None:
                        setattr(node, attr.name, newval)


class AggregateResultAst(sqlast.Ast):
    attrs = []

    def __init__(self, name, Aggregator, ast):
        self.name = name
        self.Aggregator = Aggregator
        self.ast = ast


from .nonaggr import where_clause_pycode


def aggr_call_to_make_env(ac, stmt):
    pcb = config.PycodeBuilder(stmt, 't', 'a')

    pycode = textwrap.dedent(f'''
        {config.preamble(stmt)}
        from sql2mapreduce.aggr import make_mapper, make_reducer
        from sql2mapreduce.config import funcs
        from sql2mapreduce.config import sqlfuncs
        from sql2mapreduce.config import aggregators
        import numpy

        def WHERE(t): return { where_clause_pycode(pcb) }
        def GROUP_BY(t): return { group_clause_pycode(pcb) }
        def CALL_MAP(a, t): return a.map({ ac.Aggregator.call_map_args_pycode(ac.ast, pcb) })

        mapper = make_mapper(aggregators.{ ac.Aggregator.__name__ }, CALL_MAP, WHERE, GROUP_BY)
        reducer = make_reducer()
    ''')
    return pycode


def group_clause_pycode(pcb):
    if pcb.stmt.groupClause is not None:
        return '[' + ", ".join(pcb(g) for g in pcb.stmt.groupClause) + "]"


def make_mapper(Aggregator, CALL_MAP, WHERE, GROUP_BY):
    # mapper returns {[group_value]: aggregator}
    def mapper(t):
        where = WHERE(t)
        if where is not None:
            t = t[where]

        group_values = GROUP_BY(t)
        mapped_values = {}

        def run_mapper(gv, t2):
            a = Aggregator(gv)
            CALL_MAP(a, t2)
            mapped_values[gv] = a

        if group_values is not None:
            gvs, gi = multi_column_unique(group_values)
            # gvs: list of group values
            # gi: group index
            for i, gv in enumerate(gvs):
                run_mapper(gv, t[gi == i])
        else:
            run_mapper(None, t)

        return mapped_values
    return mapper


def make_reducer():
    # acc, val: {[group_value]: aggregator}
    def reducer(acc, val):
        for k, v in val.items():
            if k in acc:
                acc[k].reduce(v)
            else:
                acc[k] = v
        return acc
    return reducer


def multi_column_unique(arr):
    if len(arr) == 1: # just for performance
        V, I = numpy.unique(arr[0], return_inverse=True)
        V = [(v,) for v in V]
    else:
        u = [numpy.unique(a, return_inverse=True) for a in arr]
        ii = numpy.array([i for v, i in u]).T
        vv = [v for v, i in u]
        II, I = numpy.unique(ii, axis=0, return_inverse=True)
        V = [tuple(vv[k][l] for k, l in enumerate(j)) for j in II]
    return V, I


if __name__ == '__main__':
    sql = '''
    SELECT
        -- crossmatch(ref.coord, shared.my_coord, 5 / arcsec, ref.id)
        shared.a + count(*)
    FROM
        pdr1_wide
    '''

    make_envs, stmt, names = sql2make_envs(sql)
    for make_env in make_envs:
        print(make_env)
    sqlast.pp(stmt)
