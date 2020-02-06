from typing import Callable, Dict, Tuple

import numpy


def flux2mag(a: numpy.ndarray):
    # m_{\text{AB}}\approx -2.5\log _{10}\left({\frac {f_{\nu }}{\text{Jy}}}\right)+8.90
    # Jy = 3631 jansky
    return -2.5 * numpy.log10(a * (10**-9) / 3631.)

nonagg_functions: Dict[Tuple[str, ...], Callable[..., numpy.ndarray]] = {
    ('flux2mag', ): flux2mag,
    ('isnan', ): numpy.isnan,
}
