import os
from quickdb.sql2mapreduce.nonagg_functions import flux2mag
from quickdb.test_config import REPO_DIR
from typing import Tuple
import unittest
from functools import lru_cache

import numpy

from quickdb.sql2mapreduce.sqlast.sqlast import ColumnRefExpression, Select

from quickdb.sspcatalog.patch import Rerun
from .numpy_context import NumpyContext


@unittest.skipUnless(REPO_DIR, 'REPO_DIR is not set')
class TestNumpyContext(unittest.TestCase):
    def test_columnref(self):
        context = NumpyContext(self.patch)
        e = sql2expression('object_id')
        self.assertTrue(array_equal(
            e(context),
            self.patch('object_id'),
        ))

    def test_shared_value_ref(self):
        context = NumpyContext(self.patch, shared={'the_answer': 42})
        e = sql2expression('shared.the_answer + 1')
        self.assertEqual(e(context), 43)

    def test_binary_operator(self):
        context = NumpyContext(self.patch)
        e = sql2expression('2 * (object_id + object_id) + 1')
        self.assertTrue(array_equal(
            e(context),
            4 * self.patch('object_id') + 1,
        ))

    def test_unary_operator(self):
        context = NumpyContext(self.patch)
        e = sql2expression('- object_id')
        self.assertTrue(array_equal(
            e(context),
            - self.patch('object_id'),
        ))
        e = sql2expression('+ object_id')
        self.assertTrue(array_equal(
            e(context),
            self.patch('object_id'),
        ))

    def test_between(self):
        object_id = self.patch('object_id')
        min = numpy.percentile(object_id, 0.2)
        max = numpy.percentile(object_id, 0.8)

        e = sql2expression(f'object_id BETWEEN {min} AND {max}')
        a = self.patch('object_id')[e(NumpyContext(self.patch))]
        self.assertTrue(numpy.logical_and(min <= a, a <= max).all())

        e = sql2expression(f'object_id NOT BETWEEN {min} AND {max}')
        a = self.patch('object_id')[e(NumpyContext(self.patch))]
        self.assertTrue(numpy.logical_or(min > a, a > max).all())

    def test_boolean_operation(self):
        class TestContext(NumpyContext):
            def evaluate_ColumnRefExpression(self, e: ColumnRefExpression):
                fields = e.fields
                a = numpy.arange(10)
                if fields[0] == 'even':
                    return a % 2 == 0
                elif fields[0] == 'odd':
                    return a % 2 == 1
                elif fields[0] == 'mod3':
                    return a % 3
        context = TestContext(self.patch)
        even = sql2expression('even')(context)
        self.assertTrue((even == numpy.array([b == 't' for b in 'tftftftftf'])).all())

        e = sql2expression('even AND odd')
        self.assertTrue((e(context) == False).all())

        e = sql2expression('even OR odd')
        self.assertTrue((e(context) == True).all())

        e = sql2expression('NOT even')
        self.assertTrue((e(context) == numpy.array([b == 't' for b in 'ftftftftft'])).all())

        e = sql2expression('mod3 = 1')
        self.assertTrue((e(context) == numpy.array([b == 't' for b in 'ftfftfftff'])).all())

        e = sql2expression('mod3 = 1 OR mod3 = 2')
        self.assertTrue((e(context) == numpy.array([b == 't' for b in 'fttfttfttf'])).all())

        e = sql2expression('mod3 = 1 OR mod3 = 2 OR mod3 = 0')
        self.assertTrue((e(context) == numpy.array([b == 't' for b in 'tttttttttt'])).all())

        e = sql2expression('NOT (mod3 = 0) AND NOT (mod3 = 1)')
        self.assertTrue((e(context) == numpy.array([b == 't' for b in 'fftfftfftf'])).all())

        e = sql2expression('NOT (mod3 = 0) AND NOT (mod3 = 1) AND NOT (mod3 = 2)')
        self.assertTrue((e(context) == numpy.array([b == 't' for b in 'ffffffffff'])).all())

    def test_funccall(self):
        e = sql2expression(''' flux2mag(forced.i.psfflux_flux) ''')
        context = NumpyContext(self.patch)
        self.assertTrue(array_equal(
            e(context),
            flux2mag(self.patch('forced.i.psfflux_flux')),
        ))

    def test_indirection(self):
        context = NumpyContext(self.patch)
        e = sql2expression('forced.coord[2]')
        self.assertTrue(array_equal(
            e(context),
            self.patch('forced.coord')[2],  # type: ignore
        ))

    def test_row(self):
        context = NumpyContext(self.patch)
        e = sql2expression('(1, 2)')
        self.assertEqual(e(context), [1, 2])

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
