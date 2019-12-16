import time
import contextlib


class Timer(object):
    def __init__(self):
        self._times = {}

    @contextlib.contextmanager
    def __call__(self, name):
        start = time.time()
        yield
        duration = time.time() - start
        self._times[name] = duration

    def __getitem__(self, name):
        return self._times[name]

    def asdict(self):
        return self._times
