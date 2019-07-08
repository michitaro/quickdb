import textwrap
import numpy
from . import sqlast
from . import config


def sql2make_env(sql, print_ast=False):
    stmt = safe_stmt(sql)

    if print_ast:
        sqlast.pp(stmt)

    pcb = config.PycodeBuilder(stmt, 't')

    pycode = textwrap.dedent(f'''
        {config.preamble(stmt)}
        from sql2mapreduce.nonaggr import make_mapper, make_reducer
        from sql2mapreduce.config import funcs
        from sql2mapreduce.config import sqlfuncs
        import numpy

        def SELECT(t): return {target_list_pycode(pcb)}
        def WHERE(t): return {where_clause_pycode(pcb)}
        def ORDER_BY(t): return {sort_clause_pycode(pcb)}
        LIMIT = {limit_count(pcb)}

        mapper = make_mapper(SELECT, WHERE, ORDER_BY, LIMIT)
        reducer = make_reducer(LIMIT)
    ''')

    return pycode, target_names(stmt)


def safe_stmt(sql):
    stmts = sqlast.parse_sql(sql)
    sqlast.check(len(stmts) == 1, 'Too many statements')
    stmt = stmts[0].stmt
    sqlast.check(isinstance(stmt, sqlast.SelectStmt), f'Not supported statement: {stmt.__class__.__name__}')
    sqlast.check(stmt.limitOffset is None, f'OFFSET Clause is not supported')
    sqlast.check(stmt.groupClause is None, f'GROUP BY in non-aggregate function')
    return stmt


def target_list_pycode(pcb):
    return '[' + ", ".join(pcb(target.val) for target in pcb.stmt.targetList) + "]"

def target_names(stmt):
    names = []
    for i, t in enumerate(stmt.targetList):
        names.append(t.name or f'c{i}')
    return names

def where_clause_pycode(pcb):
    if pcb.stmt.whereClause:
        return pcb(pcb.stmt.whereClause)

def sort_clause_pycode(pcb):
    if pcb.stmt.sortClause:
        sqlast.check(all(s.sortby_nulls == 0 for s in pcb.stmt.sortClause), 'cannot set sortby_nulls')
        order = ['', '', '-']
        return '[' + ','.join(reversed([
            f"{order[s.sortby_dir]}(" + pcb(s.node) + ")"
            for s in pcb.stmt.sortClause])) + ']'

def limit_count(pcb):
    if pcb.stmt.limitCount:
        return pcb(pcb.stmt.limitCount)


def make_mapper(SELECT, WHERE, ORDER_BY, LIMIT):
    def mapper(t):
        where = WHERE(t)
        if where is not None:
            t = t[where]
        sort_values = ORDER_BY(t)
        if sort_values is not None:
            # assert len(t) == len(sort_values[0])
            sort_indices = numpy.lexsort(sort_values)
            t = t[sort_indices]
            sort_values = [v[sort_indices][:LIMIT] for v in sort_values]
        t = t[:LIMIT]
        select = SELECT(t)
        return dict(
            select=select,
            sort_values=sort_values,
        )
    return mapper


def make_reducer(LIMIT):
    def reducer(acc, val):
        if acc['sort_values'] is not None:
            sort_values = [numpy.hstack((a, v)) for a, v in zip(acc['sort_values'], val['sort_values'])]
            sort_indices = numpy.lexsort(sort_values)
            sort_values = [v[sort_indices][:LIMIT] for v in sort_values]
            select = [numpy.hstack((a.T, v.T)).T[sort_indices][:LIMIT] for a, v in zip(acc['select'], val['select'])] 
        else:
            sort_values = None
            select = [numpy.hstack((a.T, v.T)).T[:LIMIT] for a, v in zip(acc['select'], val['select'])]
        return dict(
            select=select,
            sort_values=sort_values,
        )
    return reducer


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

    print(sql2make_env(sql, print_ast=True)[0])
