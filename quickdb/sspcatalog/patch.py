import contextlib
import glob
import os
import pickle
from functools import lru_cache
from typing import Callable, List, Tuple, Union

import numpy

from ..utils.cached_property import cached_property
from .errors import ColumnNotFoundError, UserError

FILTER_ALIAS = {
    'g': 'HSC-G',
    'r': 'HSC-R',
    'i': 'HSC-I',
    'z': 'HSC-Z',
    'y': 'HSC-Y',
    'n387': 'NB0387',
    'n816': 'NB0816',
    'n921': 'NB0921',
}

PATCH_META = {
    'flags': {},
    'dtype': {
        'object_id': (numpy.dtype('bool'), (-1,)),
    },
}


class Rerun:
    def __init__(self, dirname: str):
        if not os.path.exists(dirname):
            raise UserError(f'No such rerun: {os.path.basename(dirname)}')  # pragma: no cover
        self._dirname = dirname

    @cached_property
    def meta(self):
        with open(f'{self._dirname}/meta.pickle', 'rb') as f:
            return pickle.load(f)

    @cached_property
    def patches(self) -> List['Patch']:
        return [Patch(self, dirname) for dirname in glob.glob(f'{self._dirname}/patches/*')]


Colpath = Union[Tuple[str, ...], str]


class Patch:
    '''
    Represents an objects catalog of one patch.

    Example:
        patch = pdr2_wide.patches[0]
        flux = pdr2_dud.patches[0].column('forced.i.psfflux_flux')
        print(flux.__class__) # => numpy.ndarray
    '''

    def __init__(self, rerun: Rerun, dirname: str, npy_cache=None):
        self._rerun = rerun
        self._dirname = dirname
        self._npy_cache: NpyCache = npy_cache or NpyCache()

    @cached_property
    def meta(self):
        with open(f'{self._dirname}/meta.pickle', 'rb') as f:
            return pickle.load(f)

    @cached_property
    def size(self) -> int:
        return self.meta['size']

    @cached_property
    def skymap_id(self):
        tract, patch = os.path.basename(self._dirname).split('-')
        y, x = patch.split(',')
        return 10000 * int(tract) + 100 * int(y) + int(x)

    def column(self, colpath: Colpath) -> numpy.ndarray:
        '''
        Example:
            psfflux = patch.column('forced.i.psfflux_flux')
        '''
        array = self._column_loader(colpath)()
        return array

    def __getitem__(self, where: Union[numpy.ndarray, slice]):
        '''
        Example:
            sliced_patch = patch[patch('forced.i.flag1')]
            assert sliced_patch('forced.i.flag1').all()
        '''
        if isinstance(where, slice):
            start = 0 if where.start is None else where.start
            stop = min(self.size,  self.size if where.stop is None else where.stop)
            indices = numpy.arange(start, stop, where.step)
        else:
            assert len(where.shape) == 1
            if where.dtype.kind == 'b':
                indices, = numpy.nonzero(where)
            else:
                assert where.dtype.kind == 'i'
                indices = where
        return SlicedPatch(self, indices)

    def __call__(self, colref: str) -> numpy.ndarray:
        '''
        Just a syntax sugar for :attr:`~Patch.column`
        '''
        return self.column(colref)

    @contextlib.contextmanager
    def clear_cache(self):
        yield self
        self._npy_cache.cache.cache_clear()

    @lru_cache(maxsize=None)
    def _column_loader(self, colpath: Colpath) -> Callable[[], numpy.ndarray]:
        '''
        Loads a npy file specified by colpath
        '''
        ref: Tuple[str, ...]
        if isinstance(colpath, tuple):
            ref = colpath
        else:
            ref = tuple(colpath.split('.'))
        if len(ref) == 1:
            meta = PATCH_META
            dirname = self._dirname
        elif len(ref) == 2:
            if ref[0] not in ['forced', 'meas']:  # pragma: no cover
                raise ColumnNotFoundError(f'No such table: {ref[0]}')
            meta, dirname = {
                'forced': (self._rerun.meta['forced_universal'], f'{self._dirname}/forced/universal'),
                'meas': (self._rerun.meta['meas_position'], f'{self._dirname}/meas/position'),
            }[ref[0]]
        elif len(ref) == 3:
            if ref[0] not in ['forced', 'meas']:  # pragma: no cover
                raise ColumnNotFoundError(f'No such table: {ref[0]}')
            if ref[1] not in FILTER_ALIAS.keys():  # pragma: no cover
                raise ColumnNotFoundError(f'No such filter: {ref[1]}')
            filtername = FILTER_ALIAS[ref[1]]
            meta, dirname = {
                'forced': (self._rerun.meta['forced_filter'], f'{self._dirname}/forced/{filtername}'),
                'meas': (self._rerun.meta['meas_filter'], f'{self._dirname}/meas/{filtername}'),
            }[ref[0]]
        else:  # pragma: no cover
            raise ColumnNotFoundError(f'Invalid column specification: {colpath}')
        colname = ref[-1]
        if os.path.exists(dirname):
            if colname in meta['flags']:
                i, j = meta['flags'][colname]
                return lambda: self._npy_cache[f'{dirname}/flags-{i}.npy'] & (1 << j) != 0
            else:
                if colname not in meta['dtype']:  # pragma: no cover
                    raise ColumnNotFoundError(f'No such column: {colname}')
                return lambda: self._npy_cache[f'{dirname}/{colname}.npy']
        else:
            if colname in meta['flags']:
                dtype, shape = numpy.dtype('bool'), [self.size]
            else:
                if colname not in meta['dtype']:  # pragma: no cover
                    raise ColumnNotFoundError(f'No such column: {colname}')
                dtype, shape = meta['dtype'][colname]
                shape = [self.size, *shape[1:]]
            return lambda: nans(dtype, shape)


class SlicedPatch(Patch):
    def __init__(self, patch: Patch, indices: numpy.ndarray):
        '''
        Makes a sliced patch.

        Args:
            where: numpy.ndarray(dtype=bool)
        '''
        assert indices.dtype.kind == 'i' and len(indices.shape) == 1, f'invalid indices: {indices}'
        super().__init__(patch._rerun, patch._dirname, npy_cache=...)
        self._patch = patch
        self._indices = indices  # indices of objects in the original patch.

    def __getitem__(self, where: Union[numpy.ndarray, slice]) -> 'SlicedPatch':
        return SlicedPatch(self._patch, self._indices[where])

    def column(self, colpath: str) -> numpy.ndarray:
        array = self._patch.column(colpath)
        return array[..., self._indices]  # type: ignore

    @cached_property
    def size(self):
        return len(self._indices)


def nans(dtype, shape: List[int]) -> numpy.ndarray:
    a = numpy.empty(shape, dtype)
    if dtype.kind == 'f':
        a.fill(numpy.nan)
    elif dtype.kind == 'i':
        a.fill(-1)
    elif dtype.kind == 'b':
        a.fill(True)
    else:  # pragma: no cover
        raise RuntimeError(f'unknown dtype.kind: {dtype.kind}')
    return a


class NpyCache:
    @lru_cache(maxsize=None)
    def __getitem__(self, filename: str) -> numpy.ndarray:
        return numpy.load(filename)

    @property
    def cache(self):
        return self.__getitem__
