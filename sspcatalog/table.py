import numpy
import pickle
import os
import glob
from .memoize import memoize, cached_property
from . import serialize


column_alias = {
    'g': 'HSC-G', 
    'r': 'HSC-R', 
    'i': 'HSC-I',
    'z': 'HSC-Z', 
    'y': 'HSC-Y',
    'is_primary': 'detect_is-primary',
}


class Table(object):
    def __init__(self, worker, patch_dir, slice=None, npy_loader=None):
        self.worker = worker
        self._patch_dir = patch_dir
        self._slice = slice
        self.npy_loader = npy_loader or NpyLoader()

    def __getitem__(self, k):
        if isinstance(k, slice):
            if self._slice is not None:
                k = self._slice[k]
            else:
                k = numpy.arange(len(self))[k]
        else:
            assert isinstance(k, numpy.ndarray)
            assert len(k.shape) == 1
            if k.dtype == numpy.dtype('bool'):
                k, = numpy.where(k)
            # now k is an integer array for indices
            if self._slice is not None:
                k = self._slice[k]
        return Table(self.worker, self._patch_dir, k, self.npy_loader)

    def column(self, colref):
        if len(colref) == 1:
            if colref[0] == 'object_id':
                return self.slice(serialize.load(f'{self._patch_dir}/object_id.npy'))
            return self.table_info(colref[0])
        else:
            table_ref, column_ref = self._resolve_colref(colref)
            if column_ref.endswith('_mag'):
                return nanojansky2abmag(self._bin_table(table_ref).column(f'{column_ref[:-4]}_flux'))
            else:
                return self._bin_table(table_ref).column(column_ref)

    def table_info(self, key):
        value = None
        if key == 'host':
            value = self.worker.host
        elif key == 'patch':
            value = os.path.basename(self._patch_dir)
        else:
            raise RuntimeError(f'no such table_info key: {key}')
        return numpy.full(len(self), value)

    def null_check(self, colref, yes):
        table_ref, _ = self._resolve_colref(colref)
        is_null = self._bin_table(table_ref).is_null()
        return (is_null == yes and numpy.ones or numpy.zeros)(len(self), dtype=bool)

    @cached_property
    def rerun_meta(self):
        with open(f'{self._patch_dir}/../../meta.pickle', 'rb') as f:
            return pickle.load(f)

    @memoize
    def _resolve_colref(self, colref):
        ref = tuple(column_alias.get(r, r) for r in colref)
        return ref[:-1], ref[-1]

    @memoize
    def __len__(self):
        if self._slice is None:
            if self.pdr2:
                return self.meta['size']
            else:
                return self.meta['n_objects']
        else:
            return len(self._slice)

    # TODO: cleanup
    @cached_property
    def pdr2(self):
        return 'size' in self.meta

    @cached_property
    def meta(self):
        with open(f'{self._patch_dir}/meta.pickle', 'rb') as f:
            return pickle.load(f)

    @memoize
    def _bin_table(self, dirname):
        assert isinstance(dirname, tuple)
        assert all('.' not in p for p in dirname) and all('/' not in p for p in dirname)
        dirname = '/'.join((self._patch_dir,) + dirname)
        return BinTable(dirname, self)

    def slice(self, data):
        if self._slice is not None:
            return data[self._slice]
        else:
            return data


class BinTable(object):
    def __init__(self, dirname, table):
        self._dirname = dirname
        self.table = table

    @cached_property
    def meta(self):
        fname = f'{self._dirname}/meta.pickle'
        with open(fname, 'rb') as f:
            return pickle.load(f)

    # TODO cleanup
    def column(self, name):
        if name in self.flags:
            return self.flag(name)
        fname = f'{self._dirname}/{name}.npy'
        if os.path.exists(fname):
            return self.table.slice(self.table.npy_loader.load(fname))
        else:
            if self.table.pdr2:
                return self._pdr2_dummy(name)
            else:
                return self.table.slice(self._dummy(name))
    
    @cached_property
    def pdr2_meta(self):
        ttype, fname = self._dirname.split('/')[-2:]
        if ttype == 'forced':
            if fname == 'universal':
                return self.table.rerun_meta['forced_universal']
            return self.table.rerun_meta['forced_filter']
        if ttype == 'meas':
            if fname == 'position':
                return self.table.rerun_meta['meas_position']
            return self.table.rerun_meta['meas_position']
        raise RuntimeError(f'Unknown table table: {ttype}')


    def _pdr2_dummy(self, name):
        if name in self.pdr2_meta['dtype']:
            dtype, shape = self.pdr2_meta['dtype'][name]
            shape = list(shape)
            shape[0] = len(self.table)
            return nans(shape, dtype)
        else:
            raise RuntimeError(f'Unknown column {name}')
        
    
    def _dummy(self, name):
        array_meta = self.meta.get('missing')
        if array_meta is not None:
            if name in array_meta:
                shape, dtype = array_meta[name]
                return nans(shape, dtype)
        raise RuntimeError(f'no such file: {self._dirname}/{name}.npy')

    def flag(self, name):
        i, j = self.flags[name]
        flag = self.column(f'flags-{i}')
        return flag & (1 << j) != 0

    @property
    def flags(self):
        # TODO cleanup
        if self.table.pdr2:
            return self.pdr2_meta['flags']
        return self.meta['flag']

    # TODO cleanup
    @cached_property
    def flags_meta(self):
        with open(f'{self._dirname}/flags-meta.pickle', 'rb') as f:
            return pickle.load(f)

    @memoize
    def is_null(self):
        return os.path.exists(self._dirname)


def nans(shape, dtype):
    a = numpy.empty(shape, dtype)
    if dtype.kind == 'f':
        a.fill(numpy.nan)
    elif dtype.kind == 'i':
        a.fill(-1)
    elif dtype.kind == 'u':
        numpy.invert(a, a)
    else:
        raise RuntimeError(f'unknown dtype.kind: {dtype.kind}')
    return a


def nanojansky2abmag(flux):
    # m_{\text{AB}}\approx -2.5\log _{10}\left({\frac {f_{\nu }}{\text{Jy}}}\right)+8.90
    # Jy = 3631 jansky
    return -2.5 * numpy.log10(flux * (10**-9) / 3631.)
    # return -2.5 * numpy.log10(flux) - 31.4000656223
    # return 2.5 * (32 - numpy.log10(flux)) - 48.6


class NpyLoader(object):
    def load(self, path):
        return serialize.load(path)


if __name__ == '__main__':
    table = Table('../data/repo/pdr1_udeep/10054-0,0')
    print(table.column(('ref', 'id')))
    print(table.meta)
    import ipdb ; ipdb.set_trace()
    0
