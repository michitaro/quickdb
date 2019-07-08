import argparse
import glob
import os
import multiprocessing
import logging ; logging.basicConfig(level=logging.INFO)
import itertools
import re
import numpy
import pickle
from . import npyize


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--out', '-o', required=True)
    parser.add_argument('--parallel', '-j', type=int)
    parser.add_argument('deepCoadd_results')
    args = parser.parse_args()

    src_root = args.deepCoadd_results

    os.makedirs(f'{args.out}/patches', exist_ok=True)

    with multiprocessing.Pool(args.parallel) as pool:
        pool.starmap(process_ref, zip(
            glob.iglob(f'{src_root}/*/*/*/ref-*.fits*'),
            itertools.repeat(args.out),
        ))
        pool.starmap(process_forced, zip(
            glob.iglob(f'{src_root}/*/*/*/forced_src-*.fits*'),
            itertools.repeat(args.out),
        ))
        repair_blank_files(args.out, pool)
        pool.map(make_patch_meta, glob.iglob(f'{args.out}/patches/*'))
    make_root_meta(args.out)


def process_ref(src_fname, out_root):
    logging.info(f'converting {src_fname}...')
    bname = os.path.basename(src_fname) # ref-10055-1,0.fits
    _, tract, patch = os.path.basename(bname.split('.')[0]).split('-')
    out = f'{out_root}/patches/{tract}-{patch}/ref'
    npyize.npyize(src_fname, out)


def process_forced(src_fname, out_root):
    logging.info(f'converting {src_fname}...')
    bname = os.path.basename(src_fname) # forced_src-HSC-G-10055-2,0.fits
    m = re.match('forced_src-(.*?)-(\d+)-(\d+,\d+)\.fits', bname)
    assert m
    filterName, tract, patch = m.groups()
    out = f'{out_root}/patches/{tract}-{patch}/forced/{filterName}'
    npyize.npyize(src_fname, out)


def repair_blank_files(out, pool):
    FILTERS = set(os.path.basename(f) for f in glob.iglob(f'{out}/patches/*/forced/*'))
    template_dirs = {f: os.path.dirname(next(iter(glob.iglob(f'{out}/patches/*/forced/{f}/*.npy')))) for f in FILTERS}
    template = {}
    for filterName, d in template_dirs.items():
        with open(f'{d}/meta.pickle', 'rb') as f:
            meta = pickle.load(f)
        array_meta = {}
        for npy in glob.iglob(f'{d}/*.npy'):
            data = numpy.load(npy)
            array_meta[os.path.basename(npy[:-4])] = (data.shape[1:], data.dtype)
        template[filterName] = {
            'flag': meta['flag'],
            'array_meta': array_meta,
        }
    pool.starmap(repair_blank_file1, zip(
        glob.iglob(f'{out}/patches/*'),
        itertools.repeat(template),
    ))


def repair_blank_file1(patch_dir, forced_template):
    # logging.info(f'reparing {patch_dir}...')
    with open(f'{patch_dir}/meta.pickle', 'rb') as f:
        meta = pickle.load(f)
        n_objects = meta['n_objects']
    for filterName, t in forced_template.items():
        for _ in glob.iglob(f'{patch_dir}/forced/{filterName}/*.npy'):
            # at least 1 *.npy file is there
            break
        else:
            # no *.npy files
            os.makedirs(f'{patch_dir}/forced/{filterName}', exist_ok=True)
            with open(f'{patch_dir}/forced/{filterName}/meta.pickle', 'wb') as f:
                pickle.dump({
                    'n_objects': n_objects,
                    'flag': t['flag'],
                    'missing': {name: (tuple((n_objects,) + shape), dtype) for name, (shape, dtype) in t['array_meta'].items()},
                }, f)


def make_patch_meta(patch_dir):
    assert os.path.exists(f'{patch_dir}/ref')
    n_objects = len(numpy.load(f'{patch_dir}/ref/id.npy'))

    with open(f'{patch_dir}/meta.pickle', 'wb') as f:
        pickle.dump({
            'n_objects': n_objects,
        }, f)


def make_root_meta(out):
    stat = {}
    for patch_dir in glob.iglob(f'{out}/patches/*'):
        with open(f'{patch_dir}/meta.pickle', 'rb') as f:
            patch_meta = pickle.load(f)
        stat[os.path.basename(patch_dir)] = patch_meta
    with open(f'{out}/meta.pickle', 'wb') as f:
        pickle.dump(stat, f)


if __name__ == '__main__':
    main()
