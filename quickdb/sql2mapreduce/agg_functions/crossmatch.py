from functools import lru_cache
from typing import Dict, List, NamedTuple

import numpy
import scipy.spatial.ckdtree

from quickdb.sql2mapreduce.agg import AggCall, AggContext
from quickdb.sql2mapreduce.sqlast.sqlast import Expression, SqlError


class CrossmatchResult(NamedTuple):
    obj_indices: numpy.ndarray
    fields: List


class CrossMatchAggCall(AggCall):
    '''
        SELECT
            crossmatch(catalog_coord, target_coord, max-radius, fields => (forced.coord, forced.i.psfflux_pfs))
        FROM
            pdr2_wide
    '''

    def __init__(self, args: List[Expression], named_args: Dict[str, Expression], agg_star: bool):
        if len(args) < 3:
            raise SqlError(f'function `crossmatch` accepts only 3 positional paramters')
        self._catalog_coord = args[0]
        self._target_coord = args[1]
        self._radius = args[2]
        self._fields = args[3:]
        if len(named_args) != 0:
            raise SqlError(f'Unknown named argument: {named_args.keys()} for function `crossmatch`')
        self._tree_cache = None

    def mapper(self, context: AggContext):
        cat1 = self._catalog_coord(context)
        cat2 = self._target_coord(context)
        radius = self._radius(context)
        if len(cat1) == 0:
            cat_indices = []
            obj_indices = []
        else:
            cat1_tree = make_tree(cat1)
            cat2_tree = self._tree_cache or make_tree(cat2)
            self._tree_cache = cat2_tree
            match = cat2_tree.query_ball_tree(cat1_tree, radius)
            cat_indices = [j for m in match for j in m]
            obj_indices = [i for i, m in enumerate(match) for j in m]
        # self._fields(context) -> List[ndarray]
        fields = [f(context)[cat_indices] for f in self._fields]  # type: ignore
        return CrossmatchResult(
            obj_indices=numpy.array(obj_indices, dtype=numpy.int64),
            fields=fields,
        )

    def reducer(self, a: CrossmatchResult, b: CrossmatchResult):
        return CrossmatchResult(
            obj_indices=numpy.concatenate((a.obj_indices, b.obj_indices)),
            fields=[numpy.concatenate((af, bf)) for af, bf in zip(a.fields, b.fields)],
        )

    def finalizer(self, a) -> CrossmatchResult:
        return a


def make_tree(coord):
    assert len(coord.shape) == 2
    n_dim = coord.shape[0]
    if n_dim == 2:
        A, D = coord
        COS_D = numpy.cos(D)
        X = COS_D * numpy.cos(A)
        Y = COS_D * numpy.sin(A)
        Z = numpy.sin(D)
        xyz = numpy.array([X, Y, Z]).T
    elif n_dim == 3:
        xyz = coord.T
    else:
        raise RuntimeError(f'invalid shape ({coord.shape}) of array for coord')
    return scipy.spatial.ckdtree.cKDTree(xyz)  # type: ignore
