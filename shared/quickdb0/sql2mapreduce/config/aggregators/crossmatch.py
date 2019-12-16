import numpy
from ...aggregator import Aggregator
from ... import sqlast
import scipy.spatial.ckdtree


class CrossmatchAggregator(Aggregator):
    @classmethod
    def check_ast_args(cls, ast):
        sqlast.check(
            len(ast.named_args) == 0 and len(ast.nameless_args) >= 3,
            'crossmatch() takes coord1, coord2, match-radius, columns to fetch...')

    @classmethod
    def call_map_args_pycode(cls, fc, pcb):
        return ', '.join(pcb(a) for a in fc.nameless_args)

    def map(self, cat1, cat2, radius, *select):
        if len(cat1) == 0:
            cat_indices = numpy.array([], dtype=numpy.int64)
            obj_indices = numpy.array([], dtype=numpy.int64)
        else:
            cat1_tree = self.make_tree(cat1)
            cat2_tree = self.make_tree(cat2)
            match = cat2_tree.query_ball_tree(cat1_tree, radius)
            cat_indices = numpy.array([j for m in match for j in m], dtype=numpy.int64)
            obj_indices = numpy.array([i for i, m in enumerate(match) for j in m], dtype=numpy.int64)
        self.obj_indices = obj_indices
        self.select = [s[cat_indices] for s in select]

    def reduce(self, other):
        self.obj_indices = numpy.hstack((self.obj_indices, other.obj_indices))
        self.select = [numpy.hstack((a.T, v.T)).T for a, v in zip(self.select, other.select)]

    def result(self):
        return self.obj_indices, self.select

    @staticmethod
    def make_tree(coord):
        assert len(coord.shape) == 2
        n_dim = coord.shape[1]
        if n_dim == 2:
            A, D = coord.T
            COS_D = numpy.cos(D)
            X = COS_D * numpy.cos(A)
            Y = COS_D * numpy.sin(A)
            Z = numpy.sin(D)
            xyz = numpy.array([X, Y, Z]).T
        elif n_dim == 3:
            xyz = coord
        else:
            raise RuntimeError(f'invalid shape of array for coord in {coord}')
        return scipy.spatial.ckdtree.cKDTree(xyz)
