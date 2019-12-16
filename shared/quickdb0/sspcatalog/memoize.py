class memoize(object):
    def __init__(self, func):
        self.__doc__ = getattr(func, '__doc__')
        self.__name__ = func.__name__
        self.func = func

    def __get__(self, obj, klass):
        if obj is None:
            return self
        key = self
        if key not in obj.__dict__:
            obj.__dict__[key] = {}
        cache = obj.__dict__[key]
        def f(*args, **kwargs):
            assert len(kwargs) == 0
            if args not in cache:
                cache[args] = self.func(obj, *args)
            return cache[args]
        return f


class cached_property(object):
    def __init__(self, func):
        self.__doc__ = getattr(func, '__doc__')
        self.func = func

    def __get__(self, obj, klass):
        if obj is None:
            return self
        value = obj.__dict__[self.func.__name__] = self.func.__get__(obj, klass)()
        return value


# if False:
#     def memoize(f):
#         return f

#     def cached_property(f):
#         property(f)
