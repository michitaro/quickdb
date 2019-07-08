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

    @memoize
    def column(self, colref):
        if len(colref) == 1:
            return self.table_info(colref[0])
        else:
            table_ref, column_ref = self._resolve_colref(colref)
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

    @memoize
    def null_check(self, colref, yes):
        table_ref, column_ref = self._resolve_colref(colref)
        is_null = self._bin_table(table_ref).is_null()
        return (is_null == yes and numpy.ones or numpy.zeros)(len(self), dtype=bool)

    @memoize
    def _resolve_colref(self, colref):
        ref = tuple(column_alias.get(r, r) for r in colref)
        return ref[:-1], ref[-1]

    @memoize
    def __len__(self):
        if self._slice is None:
            return self.meta['n_objects']
        else:
            return len(self._slice)

    @cached_property
    def meta(self):
        with open(f'{self._patch_dir}/meta.pickle', 'rb') as f:
            return pickle.load(f)

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

    @memoize
    def column(self, name):
        if name in self.flags:
            return self.flag(name)
        fname = f'{self._dirname}/{name}.npy'
        if os.path.exists(fname):
            return self.table.slice(self.table.npy_loader.load(fname))
        else:
            return self.table.slice(self._dummy(name))

    @memoize
    def _dummy(self, name):
        array_meta = self.meta.get('missing')
        if array_meta is not None:
            if name in array_meta:
                shape, dtype = array_meta[name]
                return nans(shape, dtype)
        raise RuntimeError(f'no such file: {self._dirname}/{name}.npy')

    @memoize
    def flag(self, name):
        i, j = self.flags[name]
        flag = self.column(f'flags-{i}')
        return flag & (1 << j) != 0

    @cached_property
    def flags(self):
        return self.meta['flag']

    @memoize
    def is_null(self):
        return not os.path.exists(f'{self._dirname}/id.npy')


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


class NpyLoader(object):
    @memoize
    def load(self, path):
        import fcntl
        with open('/dev/shm/quickdb', 'wb') as f:
            try:
                fcntl.flock(f, fcntl.LOCK_EX)
                return serialize.load(path)
            finally:
                fcntl.flock(f, fcntl.LOCK_UN)
    # def load(self, path):
    #     import fcntl
    #     with open('/dev/shm/quickdb', 'wb') as f:
    #         try:
    #             fcntl.flock(f, fcntl.LOCK_EX)
    #             return serialize.load(path)
    #         finally:
    #             fcntl.flock(f, fcntl.LOCK_UN)


if __name__ == '__main__':
    table = Table('../data/repo/pdr1_udeep/10054-0,0')
    print(table.column(('ref', 'id')))
    print(table.meta)
    import ipdb ; ipdb.set_trace()
    0
