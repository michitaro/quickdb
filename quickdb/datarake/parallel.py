import builtins

def parallelMap(f, items, nProcs=None):
    if nProcs == 1:
        return builtins.map(f, items)
    else:
        import multiprocessing
        name = parallelMap.i
        parallelMap.scope[name] = (f, items)
        parallelMap.i += 1
        pool = multiprocessing.Pool(nProcs)
        try:
            result = pool.map(_parallel_func, ((name, i) for i in range(len(items))))
        finally:
            pool.close()
        del parallelMap.scope[name]
        return result

def starmap(f, items, nProcs=None):
    def f2(args):
        return f(*args)
    return parallelMap(f2, items, nProcs)


parallelMap.i = 0
parallelMap.scope = {}

def _parallel_func(arg):
    name, index = arg
    f, items = parallelMap.scope[name]
    return f(items[index])


map = parallelMap
