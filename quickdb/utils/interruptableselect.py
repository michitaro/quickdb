import os
from select import select
from typing import List


class InterruptableSelect:
    '''
        Example:
            import threading

            s = get_socket()

            with InterruptableSelect([s], [], []) as select:
                select.wait()
                threading.Timer(2., select.interrupt).start()
    '''

    def __init__(self, rlist: List, wlist: List, xlist: List, timeout: float = None):
        self._rlist = rlist
        self._wlist = wlist
        self._xlist = xlist
        self._timeout = timeout
        self._enter = False

    def __enter__(self):
        self._enter = True
        self._pipe = os.pipe()
        return self

    def __exit__(self, *args):
        for p in self._pipe:
            os.close(p)

    def _ensure_enter(self):
        assert self._enter
        self._enter = True

    def wait(self):
        self._ensure_enter()
        r, _ = self._pipe
        rlist, wlist, xlist = select(self._rlist + [r], self._wlist, self._xlist, self._timeout)
        if r in rlist:
            raise SelectInterrupted()
        return [rlist, wlist, xlist]

    def interrupt(self):
        self._ensure_enter()
        os.write(self._pipe[1], b'\0')


class SelectInterrupted(RuntimeError):
    ...
