import concurrent.futures


def map(f, items, n_parallel=None):
    with concurrent.futures.ThreadPoolExecutor(max_workers=n_parallel or len(items)) as pool:
        return pool.map(f, items)


def starmap(f, items, n_parallel=None):
    with concurrent.futures.ThreadPoolExecutor(max_workers=n_parallel or len(items)) as pool:
        return pool.map(_unpack_args(f), items)


def _unpack_args(f):
    def g(args):
        return f(*args)
    return g
