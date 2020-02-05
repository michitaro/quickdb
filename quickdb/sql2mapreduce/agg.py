import abc
from typing import (
    Any, Callable, Dict, List, NamedTuple, Optional, Tuple, Type, Union, cast)

import numpy

from quickdb.sql2mapreduce.interface import ProgressCB, RunMakeEnv
from quickdb.sql2mapreduce.numpy_context import NumpyContext
from quickdb.sql2mapreduce.sqlast.sqlast import (
    ColumnRefExpression, Context, Expression, FuncCallExpression, Select,
    SqlError)
from quickdb.sspcatalog.patch import Patch


class AggCall(metaclass=abc.ABCMeta):  # pragma: no cover
    @abc.abstractmethod
    def __init__(self, args: List[Expression], named_args: Dict[str, Expression], agg_star: bool):
        ...

    @abc.abstractmethod
    def mapper(self, context: Context):
        ...

    @abc.abstractmethod
    def reducer(self, a, b):
        ...

    @abc.abstractmethod
    def finalizer(self, a):
        ...

    @property
    def subaggrs(self) -> List['AggCall']:
        return []

    def result(self, context: 'AggContext'):
        return context._agg_results[self][context._group_value]


class AggContext(NumpyContext):
    def __init__(self, patch: Patch, agg_results: Dict, group_value, shared: Dict = None):
        super().__init__(patch, shared=shared)
        self._group_value = group_value
        self._agg_results = agg_results

    def evaluate_FuncCallExpression(self, e: FuncCallExpression):
        if e in self._agg_results:
            return self._agg_results[e][self._group_value]
        return super().evaluate_FuncCallExpression(e)

    def sliced_context(self, slice: Union[slice, numpy.ndarray], group_value):
        return AggContext(self._patch[slice], self._agg_results, group_value, self._shared)

    @property
    def size(self):
        return self._patch.size


class FinalizeContext(AggContext):
    def __init__(self, agg_results: Dict, group_value, shared: Dict = None):
        super().__init__(None, agg_results, group_value, shared=shared)  # type: ignore


class AggQueryResult(NamedTuple):
    group_by: Dict[Any, List]
    target_names: List[str]


class PickOneAggCall(AggCall):
    def __init__(self, args: List[Expression], named_args: Dict[str, Expression]):
        self.a = args[0]

    def mapper(self, context: Context):
        a = self.a(context)
        if len(numpy.unique(a)) >= 2:  # pragma: no cover
            raise SqlError(f'Non unique values in {self.a}')
        return a[0]

    def reducer(self, a, b):
        if a != b:  # pragma: no cover
            raise SqlError(f'Non unique values in {self.a}')
        return a

    def finalizer(self, a):
        return a


def run_agg_query(select: Select, run_make_env: RunMakeEnv, shared: Dict = None, progress: ProgressCB = None):
    from .agg_functions import agg_functions

    make_env = '''
    from quickdb.sql2mapreduce.agg import agg1_env
    rerun, mapper, reducer, finalizer = agg1_env(agg, select, agg_results, shared)
    '''

    check_select(select)

    aggs: List[Tuple[Optional[Expression], AggCall]] = []

    def pick_aggs(e: Expression):
        if isinstance(e, FuncCallExpression) and e.name in agg_functions:
            cls = cast(Type[AggCall], agg_functions[e.name])  # We need `cast` due to pyright's bug
            a = cls(e.args, e.named_args, e.agg_star)
            aggs.append((e, a))
            walk_subaggrs(a, lambda sa: aggs.append((None, sa)))

    for target in select.target_list:
        target.val.walk(pick_aggs)
        if is_context_dependent(target.val):
            aggs.append((target.val, PickOneAggCall([target.val], {})))

    if len(aggs) == 0:
        raise SqlError(f'No aggregation operation')

    # run aggregation queries
    agg_results: Dict[Union[Expression, AggCall], Any] = {}
    for i, (e, agg) in enumerate(aggs):
        def progress1(v1: float):
            progress((i + v1) / len(aggs))
        env_context = {'agg': agg, 'select': select, 'agg_results': agg_results, 'shared': shared}
        result = run_make_env(make_env, env_context, progress1)
        agg_results[agg] = result
        if e:
            agg_results[e] = result

    group_values = next(iter(agg_results.values())).keys()

    target_list = {}
    for gv in group_values:
        context = FinalizeContext(agg_results, gv, shared=shared)
        target_list[gv] = [
            agg_results[t.val][gv] if t.val in agg_results else t.val(context)
            for t in select.target_list
        ]

    return AggQueryResult(
        target_list,
        [t.name or f'col{i}' for i, t in enumerate(select.target_list)],
    )


def is_context_dependent(root: Expression):
    from .agg_functions import agg_functions

    context_dependent_expressions: List[Expression] = []

    def probe(e: Expression):
        if isinstance(e, ColumnRefExpression):
            context_dependent_expressions.append(e)

    def is_agg(e: Expression):
        return isinstance(e, FuncCallExpression) and e.name in agg_functions

    root.walk(probe, is_agg)
    return len(context_dependent_expressions) > 0


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


def agg1_env(agg: AggCall, select: Select, agg_results: Dict, shared: Dict):
    rerun = select.from_clause.relname

    def mapper(patch: Patch) -> MapperResult:
        context = AggContext(patch, agg_results, group_value=None, shared=shared)
        if select.where_clause:
            context = context.sliced_context(select.where_clause(context), None)
        if select.group_clause:
            mapped_values = {}
            group_values = [gc(context) for gc in select.group_clause]
            gvs, gi = multi_column_unique(group_values)
            for i, gv in enumerate(gvs):
                mapped_values[gv] = agg.mapper(context.sliced_context(gi == i, gv))
            return mapped_values
        else:
            if context.size > 0:
                return {None: agg.mapper(context)}
            else:
                return {}

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
