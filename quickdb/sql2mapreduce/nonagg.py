from quickdb.datarake.safeevent import SafeEvent
from typing import Any, Callable, Dict, List, NamedTuple, Optional, Union, cast

import numpy

from quickdb.datarake.interface import ProgressCB, RunMakeEnv
from quickdb.sql2mapreduce.sqlast.sqlast import Select, SqlError

from ..sspcatalog.patch import Patch
from .numpy_context import NumpyContext


class NonAggQueryResult(NamedTuple):
    target_list: List[numpy.ndarray]
    target_names: List[str]


def run_nonagg_query(select: Select, run_make_env: RunMakeEnv, shared: Dict = None, progress: ProgressCB = None, interrupt_notifiyer: SafeEvent = None) -> NonAggQueryResult:
    '''
    Args:
        select (Select): Select object to be run.
        run_make_env (callable): function used to execute python code.
                                 Usually this will be `quickdb.datarake.master.run`
    Returns:
        FinalizerResult
    '''
    check_select(select)
    make_env = '''
        from quickdb.sql2mapreduce.nonagg import nonagg_env
        rerun, mapper, reducer, finalizer = nonagg_env(select, shared)
    '''
    env_context = {'select': select, 'shared': shared}
    target_list = run_make_env(make_env, env_context, progress, interrupt_notifiyer)

    return NonAggQueryResult(
        target_list,
        [t.name or f'col{i}' for i, t in enumerate(select.target_list)],
    )


def nonagg_env(select: Select, shared: Dict):
    rerun = select.from_clause.relname

    def mapper(patch: Patch):
        context = NumpyContext(patch, shared=shared)
        sort_values: Optional[List[numpy.ndarray]]
        if select.where_clause:
            context = context.sliced_context(select.where_clause(context)[:select.limit_count])
        if select.sort_clause:
            # TODO: use numpy.partition when len(sort_clause) == 1
            sort_values = [(-1 if sc.reverse else +1) * sc.node(context) for sc in select.sort_clause]
            sort_indices = numpy.lexsort(sort_values[::-1])[:select.limit_count]
            sort_values = [sv[sort_indices] for sv in sort_values]
            context = context.sliced_context(sort_indices)
        else:
            sort_values = None
        target_list: List[numpy.ndarray] = [t.val(context) for t in select.target_list]
        return MapperResult(
            target_list,
            sort_values,
        )

    def reducer(a: MapperResult, b: MapperResult) -> MapperResult:
        if a.sort_values is None:
            target_list = [numpy.concatenate((a, v))[:select.limit_count] for a, v in zip(a.target_list, b.target_list)]
            sort_values = None
        else:
            a_sort_values = cast(List[numpy .ndarray], a.sort_values)
            b_sort_values = cast(List[numpy .ndarray], b.sort_values)
            sort_values = [numpy.concatenate((a, v)) for a, v in zip(a_sort_values, b_sort_values)]
            sort_indices = numpy.lexsort(sort_values[::-1])[:select.limit_count]
            target_list = [numpy.concatenate((a, v))[sort_indices] for a, v in zip(a.target_list, b.target_list)]
            sort_values = [sv[sort_indices] for sv in sort_values]
        return MapperResult(
            target_list,
            sort_values,
        )

    def finalizer(a: MapperResult):
        return a.target_list

    return rerun, mapper, reducer, finalizer


class MapperResult(NamedTuple):
    target_list: List[numpy.ndarray]
    sort_values: Optional[List[numpy.ndarray]]


def check_select(select: Select):
    if select.limit_count is None:
        raise SqlError('LIMIT must be specified')  # pragma: no cover
    if select.limit_offset is not None:
        raise SqlError('OFFSET is not supported')  # pragma: no cover
    if select.group_clause is not None:
        raise SqlError('GROUP clause are not allowed for non-agg query')  # pragma: no cover
