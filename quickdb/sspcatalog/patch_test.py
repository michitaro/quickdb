from .patch import Rerun
import os
import unittest

import numpy
numpy.seterr(all='ignore')


class TestPatch(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.rerun = Rerun('../work/repo/pdr2_dud')

    def test_skymap_id(self):
        patch = find_by_dirname(self.rerun, '9813-4,7')
        self.assertEqual(patch.skymap_id, 98130407)

    def test_size(self):
        patch = self.cosmos_patch
        object_id = numpy.load(f'{patch._dirname}/object_id.npy')
        self.assertEqual(patch.size, len(object_id))

    def test_column_float(self):
        patch = self.cosmos_patch
        # object_id
        self.assertTrue(array_equal(
            patch.column('object_id'),
            numpy.load(f'{patch._dirname}/object_id.npy'),
        ))
        # forced_universal
        self.assertTrue(array_equal(
            patch.column('forced.coord'),
            numpy.load(f'{patch._dirname}/forced/universal/coord.npy') / (180.0*3600.0 / numpy.pi),
        ))
        # forced_filter
        self.assertTrue(array_equal(
            patch.column('forced.i.psfflux_flux'),
            numpy.load(f'{patch._dirname}/forced/HSC-I/psfflux_flux.npy'),
        ))
        # meas_position
        self.assertTrue(array_equal(
            patch.column('meas.i_coord'),
            numpy.load(f'{patch._dirname}/meas/position/i_coord.npy') / (180.0*3600.0 / numpy.pi),
        ))
        # meas_filter
        self.assertTrue(array_equal(
            patch.column('meas.i.cmodel_flux'),
            numpy.load(f'{patch._dirname}/meas/HSC-I/cmodel_flux.npy'),
        ))
        # meas_filter (tuple colpath)
        self.assertTrue(array_equal(
            patch.column(('meas', 'i', 'cmodel_flux')),
            numpy.load(f'{patch._dirname}/meas/HSC-I/cmodel_flux.npy'),
        ))
        # meas_filter magnitude
        self.assertTrue(array_equal(
            patch.column(('meas', 'i', 'cmodel_mag')),
            flux2mag(numpy.load(f'{patch._dirname}/meas/HSC-I/cmodel_flux.npy')),
        ))

    def test_column_flag(self):
        patch = self.cosmos_patch
        # forced_filter.flags.convolvedflux_3_deconv: (1, 28)
        a = patch.column('forced.i.convolvedflux_3_deconv')
        b = numpy.load(f'{patch._dirname}/forced/HSC-I/flags-1.npy') & (1 << 28) != 0
        self.assertTrue((a == b).all())

    def test_column_absent_float(self):
        patch = self.no_z_patch
        array = patch.column('forced.z.psfflux_flux')
        self.assertTrue(numpy.isnan(array).all())
        self.assertEqual(len(array), patch.size)

    def test_column_absent_flag(self):
        patch = self.no_z_patch
        array = patch.column('forced.z.convolvedflux_2_15_flag_apcorr')
        self.assertTrue(array.all())
        self.assertEqual(len(array), patch.size)

    def test_call(self):
        patch = self.cosmos_patch
        self.assertTrue(array_equal(
            patch('forced.z.psfflux_flux'),
            patch.column('forced.z.psfflux_flux'),
        ))

    # def test_with_cache(self):
    #     patch = self.cosmos_patch
    #     with patch.clear_cache():
    #         hits0 = patch._npy_cache.cache.cache_info().hits
    #         for _ in range(3):
    #             patch('forced.i.psfflux_flux')
    #         self.assertEqual(patch._npy_cache.cache.cache_info().hits, hits0 + 3)
    #     self.assertEqual(patch._npy_cache.cache.cache_info().currsize, 0)

    def test_slice_with_boolean_array(self):
        patch = self.cosmos_patch
        is_star: numpy.ndarray = patch('forced.i.extendedness_value') < 0.5
        sliced = patch[is_star]
        self.assertEqual(sliced.size, is_star.sum())
        self.assertTrue(len(sliced('forced.i.extendedness_value')), sliced.size)
        self.assertTrue((sliced('forced.i.extendedness_value') < 0.5).all())

    def test_slice_with_integer_array(self):
        patch = self.cosmos_patch
        n = 3
        sliced = patch[numpy.arange(n)]
        self.assertTrue(array_equal(
            patch('forced.i.psfflux_flux')[:n],
            sliced('forced.i.psfflux_flux')))
        self.assertEqual(sliced.size, n)

    def test_slice_with_slice(self):
        patch = self.cosmos_patch
        sliced = patch[1:10:2]
        self.assertTrue(array_equal(
            patch('forced.i.psfflux_flux')[1:10:2],
            sliced('forced.i.psfflux_flux')))

    def test_slice_with_slice_stop_omitted(self):
        patch = self.cosmos_patch
        sliced = patch[1::2]
        self.assertTrue(array_equal(
            patch('forced.i.psfflux_flux')[1::2],
            sliced('forced.i.psfflux_flux')))

    def test_nested_slice_with_boolean_array(self):
        patch = self.cosmos_patch
        is_star: numpy.ndarray = patch('forced.i.extendedness_value') < 0.5
        sliced = patch[is_star]
        sliced2 = sliced[sliced('object_id') % 2 == 0]
        self.assertTrue((sliced2('object_id') % 2 == 0).all())
        self.assertTrue((sliced2('forced.i.extendedness_value') < 0.5).all())
        self.assertEqual(
            len(sliced2('object_id')),
            sliced2.size)
        self.assertEqual(
            (sliced('object_id') % 2 == 0).sum(),
            sliced2.size)

    def test_nested_slice_with_integer_array(self):
        patch = self.cosmos_patch
        is_star: numpy.ndarray = patch('forced.i.extendedness_value') < 0.5
        sliced = patch[is_star]
        n = 3
        sliced2 = sliced[numpy.arange(n)]
        self.assertTrue(array_equal(
            sliced2('object_id'),
            sliced('object_id')[:n],
        ))

    def test_nested_slice_for_coord(self):
        patch = self.cosmos_patch
        is_star: numpy.ndarray = patch('forced.i.extendedness_value') < 0.5
        sliced = patch[is_star]
        n = 3
        sliced2 = sliced[numpy.arange(n)]
        self.assertTrue(array_equal(
            sliced2('forced.coord'),
            sliced('forced.coord')[:, :n],  # type: ignore
        ))

    def test_nested_slice_with_slice(self):
        patch = self.cosmos_patch
        is_star: numpy.ndarray = patch('forced.i.extendedness_value') < 0.5
        sliced = patch[is_star]
        sliced2 = sliced[4::3]
        self.assertTrue(array_equal(
            sliced2('forced.i.psfflux_flux'),
            sliced('forced.i.psfflux_flux')[4::3],
        ))

    def test_nans_float(self):
        patch = self.no_z_patch
        self.assertTrue(numpy.isnan(patch('meas.z.cmodel_flux')).all())

    def test_nans_integer(self):
        patch = self.no_z_patch
        self.assertTrue((patch('meas.z.footprintarea_value') == -1).all())

    @property
    def cosmos_patch(self):
        return find_by_dirname(self.rerun, '9813-4,7')

    @property
    def no_z_patch(self):
        return find_by_dirname(self.rerun, '10054-3,5')


def array_equal(a: numpy.ndarray, b: numpy.ndarray):
    return numpy.allclose(a, b, equal_nan=True)


def find_by_dirname(rerun: Rerun, dirname: str):
    for p in rerun.patches:
        if os.path.basename(p._dirname) == dirname:
            return p
    raise RuntimeError(f'Patch not found: {dirname}')


def flux2mag(a: numpy.ndarray):
    # m_{\text{AB}}\approx -2.5\log _{10}\left({\frac {f_{\nu }}{\text{Jy}}}\right)+8.90
    # Jy = 3631 jansky
    return -2.5 * numpy.log10(a * (10**-9) / 3631.)
