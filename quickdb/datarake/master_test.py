import os
from quickdb.datarake.interface import Progress
import unittest

from tqdm import tqdm

from . import master


@unittest.skipUnless(os.environ.get('CLUSTER_TEST'), '$CLUSTER_SET is not set')
class TestMaster(unittest.TestCase):
    def test_master(self):
        make_env = '''
            import numpy
        
            rerun = 'pdr2_wide'

            def flux2mag(a: numpy.ndarray):
                # nanojansky -> magnitude
                return 57.543993733715695 * a
        
            def mapper(patch):
                mag = flux2mag(patch('forced.i.psfflux_flux'))
                return numpy.histogram(mag, bins=50, range=(0, 30))

            def reducer(a, b):
                return a[0] + b[0], a
        '''

        with tqdm(total=100) as pbar:
            def progress(p: Progress):
                pbar.reset(total=p.total)
                pbar.n = p.done
                pbar.refresh()
            result = master.run_make_env(make_env, {}, progress)

        print(result[0])
