import os
from quickdb.test_config import REPO_DIR
from typing import Tuple
import unittest
from functools import lru_cache

import numpy

from quickdb.sql2mapreduce.sqlast.sqlast import Select, expression_class

from quickdb.sspcatalog.patch import Rerun
from .numpy_context import NumpyContext


@unittest.skipUnless(REPO_DIR, 'REPO_DIR is not set')
class TestNumpyContext(unittest.TestCase):
    def test_columnref(self):
        context = NumpyContext(self.patch)
        e = sql2expression('object_id')
        self.assertTrue(array_equal(
            e.evaluate(context),
            self.patch('object_id'),
        ))

    def test_binary_operator(self):
        context = NumpyContext(self.patch)
        e = sql2expression('2 * (object_id + object_id) + 1')
        self.assertTrue(array_equal(
            e.evaluate(context),
            4 * self.patch('object_id') + 1,
        ))

    def test_unary_operator(self):
        context = NumpyContext(self.patch)
        e = sql2expression('- object_id')
        self.assertTrue(array_equal(
            e.evaluate(context),
            - self.patch('object_id'),
        ))

    def test_between(self):
        object_id = self.patch('object_id')
        min = numpy.percentile(object_id, 0.2)
        max = numpy.percentile(object_id, 0.8)
        e = sql2expression(f'object_id BETWEEN {min} AND {max}')
        a = self.patch('object_id')[e.evaluate(NumpyContext(self.patch))]
        self.assertTrue(numpy.logical_and(min <= a, a <= max).all())

    def test_boolean_operation(self):
        class TestContext(NumpyContext):
            def columnref(self, ref: Tuple[str, ...]):
                a = numpy.arange(10)
                if ref[0] == 'even':
                    return a % 2 == 0
                elif ref[0] == 'odd':
                    return a % 2 == 1
        context = TestContext(self.patch)
        even = sql2expression('even').evaluate(context)
        self.assertTrue((even == numpy.array([b == 't' for b in 'tftftftftf'])).all())

        e = sql2expression('even AND odd')
        self.assertTrue((e.evaluate(context) == False).all())

        e = sql2expression('even OR odd')
        self.assertTrue((e.evaluate(context) == True).all())

        e = sql2expression('NOT even')
        self.assertTrue((e.evaluate(context) == numpy.array([b == 't' for b in 'ftftftftft'])).all())

    def test_funccall(self):
        e = sql2expression(''' flux2mag(forced.i.psfflux_flux) ''')
        context = NumpyContext(self.patch)
        self.assertTrue(array_equal(
            e.evaluate(context),
            57.543993733715695 * self.patch('forced.i.psfflux_flux'),
        ))

    def test_indirection(self):
        context = NumpyContext(self.patch)
        e = sql2expression('forced.coord[2]')
        self.assertTrue(array_equal(
            e.evaluate(context),
            self.patch('forced.coord')[2],  # type: ignore
        ))

    @property
    def patch(self):
        return find_patch_by_dirname(cached_rerun('pdr2_dud'), '9813-4,6')


def sql2expression(subsql: str):
    sql = f'''SELECT {subsql} FROM t'''
    select = Select(sql)
    return select.target_list[0].val


@lru_cache()
def cached_rerun(rerun_name: str):
    return Rerun(f'{REPO_DIR}/{rerun_name}')


@lru_cache()
def find_patch_by_dirname(rerun: Rerun, dirname: str):
    for p in rerun.patches:
        if os.path.basename(p._dirname) == dirname:
            return p
    raise RuntimeError(f'Patch not found: {dirname}')


def array_equal(a: numpy.ndarray, b: numpy.ndarray):
    return numpy.allclose(a, b, equal_nan=True)
