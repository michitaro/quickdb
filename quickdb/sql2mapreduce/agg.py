from typing import Any, Callable, Dict, List, NamedTuple, Optional, Tuple, Type, Union, cast

import numpy

from quickdb.sql2mapreduce.numpy_context import NumpyContext
from quickdb.sql2mapreduce.sqlast.sqlast import (Context, Expression, FuncCall,
                                                 Select, SqlError)
from quickdb.sspcatalog.patch import Patch


class AggCall: # pragma: no cover
    def __init__(self, args: List[Expression], named_args: Dict[str, Expression]):
        raise NotImplementedError(f'{self.__class__}.__init__ must be implemneted')

    def mapper(self, context: Context):
        raise NotImplementedError(f'{self.__class__}.mapper must be implemneted')

    def reducer(self, a, b):
        raise NotImplementedError(f'{self.__class__}.reduer must be implemneted')

    def finalizer(self, a):
        raise NotImplementedError(f'{self.__class__}.finalizer must be implemneted')

    @property
    def subaggrs(self) -> List['AggCall']:
        return []

    @property
    def result(self):
        raise NotImplementedError(f'{self.__class__}.result must be implemneted')


class AggContext(NumpyContext):
    def __init__(self, patch: Patch, agg_results: Dict, group_value):
        super().__init__(patch)
        self._group_value = group_value
        self._agg_results = agg_results

    def funccall(self, name: Tuple[str, ...], args: List, named_args: Dict, agg_star: bool, expression: Expression):
        if expression.location in self._agg_results:
            return self._agg_results[expression.location][self._group_value]
        return super().funccall(name, args, named_args, agg_star, expression)

    def sliced_context(self, slice: Union[slice, numpy.ndarray], group_value):
        return AggContext(self._patch[slice], self._agg_results, group_value)


class FinalizeContext(AggContext):
    def __init__(self, agg_results: Dict, group_value):
        super().__init__(None, agg_results, group_value)  # type: ignore

    def columnref(self, ref: Tuple[str, ...]):
        raise RuntimeError(f'Accessing columns in finalize phase: {ref}') # pragma: no cover


RunMakeEnv = Callable[[str, Dict, Optional[Callable[[float], None]]], Any]
ProgressCB = Callable[[float], None]


class AggQueryResult(NamedTuple):
    target_list: Dict[Any, List]
    target_names: List[str]


def run_agg_query(select: Select, run_make_env: RunMakeEnv, shared: Dict = None, progress: ProgressCB = None):
    from .agg_functions import agg_functions
    make_env = '''
    from quickdb.sql2mapreduce.agg import agg1_env
    rerun, mapper, reducer, finalizer = agg1_env(agg, select, agg_results)
    '''

    check_select(select)

    aggs: List[Tuple[Optional[Expression], AggCall]] = []

    def pick_aggs(e: Expression):
        if isinstance(e, FuncCall) and e.funcname in agg_functions:
            cls = cast(Type[AggCall], agg_functions[e.funcname]) # due to pyright's bug, we need cast
            a = cls(e.args, e.named_args)
            aggs.append((e, a))
            walk_subaggrs(a, lambda sa: aggs.append((None, sa)))

    for target in select.target_list:
        target.val.walk(pick_aggs)

    agg_results: Dict[int, Any] = {}
    for i, (e, agg) in enumerate(aggs):
        def progress1(v1: float):
            progress((i + v1) / len(aggs))
        shared = {'agg': agg, 'select': select, 'agg_results': agg_results}
        result = run_make_env(make_env, shared, progress1)
        if e:
            agg_results[e.location] = result

    group_values = next(iter(agg_results.values())).keys()

    target_list = {}
    for gv in group_values:
        context = FinalizeContext(agg_results, gv)
        target_list[gv] = [t.val.evaluate(context) for t in select.target_list]

    return AggQueryResult(
        target_list,
        [t.name or f'col{i}' for i, t in enumerate(select.target_list)],
    )


def check_select(select: Select):  # pragma: no cover
    if select.sort_clause:
        raise SqlError(f'ORDER clause is not allowed in aggregation query')
    if select.limit_count:
        raise SqlError('LIMIT clause is not allowed in aggregation query')
    if select.limit_offset is not None:
        raise SqlError('OFFSET is not supported')


def walk_subaggrs(a: AggCall, f: Callable[[AggCall], None]):
    q = a.subaggrs
    while len(q) > 0:
        a = q.pop(0)
        f(a)
        q += a.subaggrs


MapperResult = Dict


def agg1_env(agg: AggCall, select: Select, agg_results: Dict):
    rerun = select.from_clause.relname

    def mapper(patch: Patch) -> MapperResult:
        context = AggContext(patch, agg_results, None)
        if select.where_clause:
            context = context.sliced_context(select.where_clause.evaluate(context), None)
        if select.group_clause:
            mapped_values = {}
            group_values = [gc.evaluate(context) for gc in select.group_clause]
            gvs, gi = multi_column_unique(group_values)
            for i, gv in enumerate(gvs):
                mapped_values[gv] = agg.mapper(context.sliced_context(gi == i, gv))
            return mapped_values
        else:
            return {None: agg.mapper(context)}

    def reducer(a: MapperResult, b: MapperResult):
        for k, v in b.items():
            if k in a:
                a[k] = agg.reducer(a[k], v)
            else:
                a[k] = v
        return a

    def finalizer(a):
        return {k: agg.finalizer(v) for k, v in a.items()}

    return rerun, mapper, reducer, finalizer


def multi_column_unique(arr: List[numpy.ndarray]) -> Tuple[List[Tuple], numpy.ndarray]:
    '''
    Returns V, I
        V: list of group values
        I: group index
    '''
    if len(arr) == 1:  # just for performance
        V, I = numpy.unique(arr[0], return_inverse=True)
        V = [(v,) for v in V]
    else:
        u = [numpy.unique(a, return_inverse=True) for a in arr]
        ii = numpy.array([i for v, i in u]).T
        vv = [v for v, i in u]
        II, I = numpy.unique(ii, axis=0, return_inverse=True)
        V = [tuple(vv[k][l] for k, l in enumerate(j)) for j in II]
    return V, I  # type: ignore
