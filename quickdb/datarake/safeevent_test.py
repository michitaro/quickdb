import time
import unittest

from quickdb.datarake.safeevent import SafeEvent, wait_for_safe_event


class TestThreadThings(unittest.TestCase):
    def test_wait_for_event(self):
        a = [1]

        with SafeEvent() as ev:
            with wait_for_safe_event(ev, a.clear):
                ev.set()
                time.sleep(0.5)

        self.assertEqual(len(a), 0)

    def test_wait_for_event_2(self):
        a = [1]

        with SafeEvent() as ev:
            with wait_for_safe_event(ev, a.clear):
                ...
        self.assertEqual(len(a), 1)
