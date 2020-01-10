import pickle
import os
import importlib
import sys
import warnings
import logging
import itertools
import pickle
import numpy
import multiprocessing
sys.path.append(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'db-meas-forced'))

logging.basicConfig(level=logging.INFO)

warnings.filterwarnings('ignore', r'^File may have been truncated')
warnings.filterwarnings('ignore', r'^invalid value encountered', RuntimeWarning)


meas = importlib.import_module('db-meas-forced.create-table-meas')
forced = importlib.import_module('db-meas-forced.create-table-forced')
lib = meas.lib  # ( == forced.lib)


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--out', '-o', required=True)
    parser.add_argument('--parallel', '-j', type=int)
    parser.add_argument('rerun_dir')

    args = parser.parse_args()

    rerun_dir = args.rerun_dir
    filters = lib.common.get_existing_filters(rerun_dir)

    if args.parallel == 1:
        for tract in lib.common.get_existing_tracts(rerun_dir):
            process_tract(rerun_dir, tract, filters, args.out)
    else:
        tracts = lib.common.get_existing_tracts(rerun_dir)
        with multiprocessing.Pool(args.parallel) as pool:
            R = itertools.repeat
            pool.starmap(process_tract, zip(R(rerun_dir), tracts, R(filters), R(args.out)))


def process_tract(rerun_dir, tract, filters, out):
    assert meas.get_existing_patches(rerun_dir, tract) == forced.get_existing_patches(rerun_dir, tract)
    for patch in forced.get_existing_patches(rerun_dir, tract):
        logging.info(f'processing tract={tract}, patch={patch}...')
        if not patch_done(out, tract, patch):
            process_patch_meas(rerun_dir, tract, patch, filters, out)
            process_patch_forced(rerun_dir, tract, patch, filters, out)
            patch_meta(tract, patch, out)


def patch_done(out, tract, patch):
    patch_dir = f'{out}/{tract}/{patch}'
    return os.path.exists(f'{patch_dir}/meta.pickle')


def process_patch_meas(rerun_dir, tract, patch, filters, out):
    position, multibands = transformed_patch_meas(rerun_dir, tract, filters, patch)
    meas_dir = f'{out}/{tract}/{patch}/meas'
    save_table(position, f'{meas_dir}/position')
    for filter_name, meas_tables in multibands.items():
        for table in meas_tables:
            save_table(table, f'{meas_dir}/{filter_name}')


def patch_meta(tract, patch, out):
    patch_dir = f'{out}/{tract}/{patch}'
    meta_file = f'{patch_dir}/meta.pickle'
    meta = {
        'size': len(numpy.load(f'{patch_dir}/object_id.npy')),
        'filters': [f for f in os.listdir(f'{patch_dir}/meas') if f != 'position'],
    }
    with open(f'{meta_file}.inprogress', 'wb') as f:
        pickle.dump(meta, f)
    os.rename(f'{meta_file}.inprogress', meta_file)


def process_patch_forced(rerun_dir, tract, patch, filters, out):
    object_id, universals, multibands = transformed_patch_forced(rerun_dir, tract, filters, patch)
    patch_dir = f'{out}/{tract}/{patch}'
    forced_dir = f'{patch_dir}/forced'
    save_array(f'{patch_dir}/object_id.npy', object_id)
    for table in universals.values():
        save_table(table, f'{forced_dir}/universal')
    for tables in multibands.values():
        for table, filter in tables:
            save_table(table, f'{forced_dir}/{filter}')


def transformed_patch_meas(rerun_dir, tract, filters, patch):
    catPaths = {}
    for filter in filters:
        catPath = meas.get_catalog_path(rerun_dir, tract, patch, filter)
        if lib.common.path_exists(catPath):
            catPaths[filter] = catPath
    tablePosition = None
    tableMeas = {}
    if len(catPaths) == 0:
        return None, None
    for filter, catPath in catPaths.items():
        tablePosition, mult = meas.get_catalog_schema_from_file(catPath, tablePosition)
        tableMeas[filter] = []
        for table in mult.values():
            table.transform(rerun_dir, tract, patch, filter, tablePosition.coords[filter])
            tableMeas[filter].append(table)
    object_id = tablePosition.object_id
    tablePosition.transform(rerun_dir, tract, patch, "", None)
    return tablePosition, tableMeas


def transformed_patch_forced(rerun_dir, tract, filters, patch):
    catPaths = {}
    for filter in filters:
        catPath = forced.get_catalog_path(rerun_dir, tract, patch, filter)
        if lib.common.path_exists(catPath):
            catPaths[filter] = catPath
    refPath = forced.get_ref_path(rerun_dir, tract, patch)
    universals, object_id, coord = forced.get_ref_schema_from_file(refPath)
    for table in itertools.chain(universals.values()):
        table.transform(rerun_dir, tract, patch, "", coord)
    multibands = {}
    for filter, catPath in catPaths.items():
        for table in forced.get_catalog_schema_from_file(catPath, object_id).values():
            table.transform(rerun_dir, tract, patch, filter, coord)
            if table.name not in multibands:
                multibands[table.name] = []
            multibands[table.name].append((table, filter))
    return object_id, universals, multibands


def save_table(table, out):
    os.makedirs(f'{out}', exist_ok=True)
    flags = {}
    for name, fmt, cols in table.get_backend_field_data(''):
        col = cols[0]
        if len(cols) == 1 and col.dtype == 'bool':
            flags[name] = col
        else:
            save_array(f'{out}/{name}.npy', col if len(cols) == 1 else cols)

    class meta:
        flags_meta = save_flags(flags, out)
    return meta


def save_flags(flags, out):
    bits = 32
    dtype = 'uint32'
    template = next(iter(flags.values()))
    meta = {}
    for group_id, group in enumerate(each_slice(sorted(flags.keys()), bits)):
        array = numpy.zeros_like(template, dtype=dtype)
        for col_id, col_name in enumerate(group):
            meta[col_name] = (group_id, col_id)
            col = flags[col_name]
            array |= numpy.array(numpy.where(col, 1 << col_id, 0), dtype=dtype)
        save_array(f'{out}/flags-{group_id}.npy', array)
    with open(f'{out}/flags-meta.pickle', 'wb') as f:
        pickle.dump(meta, f)
    return meta


def each_slice(iterable, n):
    values = []
    for v in iterable:
        values.append(v)
        if len(values) == n:
            yield values
            values = []
    if len(values) > 0:
        yield values


def save_array(fname, array):
    numpy.save(fname, array)


def save_pickle(data, outfile):
    with open(outfile, 'wb') as f:
        pickle.dump(data, f)


if __name__ == '__main__':
    main()

