from contextlib import contextmanager
import threading
from typing import Callable


class SafeEvent(threading.Event):
    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.set()


@contextmanager
def wait_for_safe_event(ev: SafeEvent, cb: Callable):
    assert isinstance(ev, SafeEvent)
    done = False
    ev2 = threading.Event()

    def f():
        ev.wait()
        ev2.set()

    def g():
        ev2.wait()
        if not done:
            cb()

    threading.Thread(target=f).start()
    g_th = threading.Thread(target=g)
    g_th.start()
    yield
    done = True
    ev2.set()
    g_th.join()
