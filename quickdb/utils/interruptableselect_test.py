from quickdb.utils.interruptableselect import InterruptableSelect, SelectInterrupted
import unittest
import os
import threading


class TestInterruptableSelect(unittest.TestCase):
    def test_interruptableselect_interrupt(self):
        p = os.pipe()
        with InterruptableSelect([p[0]], [], [], timeout=0.5) as select:
            threading.Timer(0.25, select.interrupt).start()
            with self.assertRaises(SelectInterrupted):
                select.wait()

    def test_interruptableselect_no_interrupt(self):
        p = os.pipe()
        with InterruptableSelect([p[0]], [], [], timeout=0.25) as select:
            self.assertEqual(
                select.wait(),
                [[], [], []],
            )            
