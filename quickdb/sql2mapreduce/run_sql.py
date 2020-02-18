from quickdb.sspcatalog.errors import UserError
from quickdb.sql2mapreduce.nonagg_functions import nonagg_functions
from quickdb.datarake.safeevent import SafeEvent
from quickdb.sql2mapreduce.agg_functions import agg_functions
from quickdb.sql2mapreduce.nonagg import run_nonagg_query
from quickdb.sql2mapreduce.agg import run_agg_query
from quickdb.sql2mapreduce.sqlast.sqlast import Expression, FuncCallExpression, Select, SqlError
from quickdb.datarake.interface import ProgressCB, RunMakeEnv
from typing import Dict, List, NamedTuple


class QueryResult(NamedTuple):
    target_names: List[str]
    target_list: List


def run_sql(sql: str, run_make_env: RunMakeEnv, shared: Dict = None,
            progress: ProgressCB = None, interrupt_notifiyer: SafeEvent = None, streaming=False):
    select = Select(sql)
    if is_agg_query(select):
        if streaming:
            raise UserError(f'Aggregation query cannot be run streamly')
        result = run_agg_query(select, run_make_env, shared, progress=progress, interrupt_notifiyer=interrupt_notifiyer)
        group_values = []
        target_list = [[] for i in result.target_names]
        for gv, t in result.group_by.items():
            group_values.append(gv)
            for i, c in enumerate(t):
                target_list[i].append(c)
        return QueryResult(
            ['$group_by'] + result.target_names,
            [group_values] + target_list,
        )
    else:
        if streaming:
            if select.sort_clause:
                raise UserError(f'ORDER BY clause cannot be given in streaming query')
        result = run_nonagg_query(select, run_make_env, shared,
                                  progress=progress, interrupt_notifiyer=interrupt_notifiyer, streaming=streaming)
        return QueryResult(
            result.target_names,
            result.target_list,
        )


def is_agg_query(select: Select):
    aggs: List[Expression] = []

    def probe(e: Expression):
        if isinstance(e, FuncCallExpression):
            if e.name in agg_functions:
                aggs.append(e)
            elif e.name not in nonagg_functions:
                raise SqlError(f'No such function: {e.name}')

    for target in select.target_list:
        target.val.walk(probe)

    return len(aggs) > 0
