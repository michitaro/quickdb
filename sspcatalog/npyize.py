import argparse
import pickle
import os
import re
import sys
import numpy
import astropy.io.fits as afits
from . import serialize


def npyize(src_fname, out_dir):
    with afits.open(src_fname) as hdul:
        hdu = hdul[1]
        os.makedirs(out_dir, exist_ok=True)
        extract_non_flag_columns(hdu, out_dir)
        flag_meta = extract_flags(hdu, out_dir)
    with open(f'{out_dir}/meta.pickle', 'wb') as f:
        pickle.dump({ 'flag': flag_meta }, f)


def extract_non_flag_columns(hdu, out_dir):
    data = hdu.data
    for name in data.dtype.names:
        if name != 'flags':
            column = data.field(name)
            serialize.save(f'{out_dir}/{name}.npy', column)


def extract_flags(hdu, out_dir):
    # TTYPE1  = 'flags   '   / bits for all Flag fields; see also TFLAGn
    # TFORM1  = '61X     '   / format of field
    n_bits = 32
    dtype = f'uint{n_bits}'
    h = hdu.header
    n_rows = h['NAXIS2']
    assert h['TTYPE1'] == 'flags'
    assert re.match('\d+X$', h['TFORM1'])
    n_flags = int(h['TFORM1'][:-1])
    n_files = (n_flags - 1) // n_bits + 1
    flag_meta = {}
    for i in range(n_files):
        bit_field = numpy.zeros(n_rows, dtype=dtype)
        flags = hdu.data.field('flags')
        for j, f in enumerate(flags.T[i*n_bits : (i+1)*n_bits]):
            bit_field |= numpy.array(1 & f, dtype=dtype) << j
            flag_name = h[f'TFLAG{n_bits * i + j + 1}']
            flag_meta[flag_name] = [i, j]
        serialize.save(f'{out_dir}/flags-{i}.npy', bit_field)
    return flag_meta


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--out', '-o', required=True)
    parser.add_argument('source')
    args = parser.parse_args()

    npyize(args.source, args.out)
