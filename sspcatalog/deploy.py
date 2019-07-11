from datarake import config
import argparse
import subprocess
import glob
import logging ; logging.basicConfig(level=logging.INFO)
import os
import itertools


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--rerun')
    parser.add_argument('--verbose', '-v', action='store_true')
    parser.add_argument('data')
    args = parser.parse_args()

    if args.rerun is None:
        args.rerun = os.path.basename(args.data)
    n_workers = len(config.workers)

    allPatches = glob.glob(f'{args.data}/patches/*')
    for i, worker in enumerate(config.workers):
        logging.info(f' ==== {worker.host} ====')
        subprocess.check_call([
            'ssh', worker.host, f'mkdir -p {worker.work_dir}/repo/{args.rerun}/patches',
        ])
        for f in set(os.listdir(args.data)) - {'patches'}:
            subprocess.check_call(['rsync', '--delete', '-av', f'{args.data}/{f}', f'{worker.host}:{worker.work_dir}/repo/{args.rerun}/{f}'])
        s = len(allPatches) * i // n_workers
        e = len(allPatches) * (i + 1) // n_workers
        files = allPatches[s:e]
        deploy(worker, args.rerun, files, args.verbose)



files_to_deploy = '''
    *.pickle id.npy coord.npy flags-*.npy
    cmodel_flux.npy cmodel_flux_err.npy flux_sinc.npy flux_sinc_err.npy
    classification_extendedness.npy
    shape_sdss*.npy
    variance
'''.split()


def deploy(worker, rerun, files, verbose):
    assert len(worker.work_dir) > 0
    remote_patches = set(subprocess.check_output([
        'ssh', worker.host, f'ls {worker.work_dir}/repo/{rerun}/patches',
    ]).decode('utf-8').split())
    patches_to_delete = remote_patches - set(os.path.basename(f) for f in files)
    if len(patches_to_delete) > 0:
        subprocess.check_call([
            'ssh', worker.host, f'cd {worker.work_dir}/repo/{rerun}/patches ; rm -r {" ".join(patches_to_delete)}',
        ])
    batch_n = 32
    for i, batch_files in enumerate(batch(files, batch_n)):
        logging.info(f'deploying {i} / {len(files) // batch_n}...')
        subprocess.check_call(
            ['rsync', '-a', *(['-v'] if verbose else []), '--delete', '--include', '*/'] + 
            list(itertools.chain.from_iterable((('--include', p) for p in files_to_deploy))) +
            [ '--exclude', '*' ] +
            batch_files + [f'{worker.host}:{worker.work_dir}/repo/{rerun}/patches']
        )


def batch(iterable, size):
    # https://stackoverflow.com/a/8290514/2741327
    from itertools import islice, chain
    sourceiter = iter(iterable)
    while True:
        batchiter = islice(sourceiter, size)
        try:
            yield list(chain([next(batchiter)], batchiter))
        except StopIteration:
            return

if __name__ == '__main__':
    main()
