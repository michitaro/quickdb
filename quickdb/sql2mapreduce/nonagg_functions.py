from typing import Callable, Dict, Tuple

import numpy


def flux2mag(a: numpy.ndarray):
    # nanojansky -> magnitude
    return 57.543993733715695 * a


nonagg_functions: Dict[Tuple[str, ...], Callable[..., numpy.ndarray]] = {
    ('flux2mag', ): flux2mag,
    ('isnan', ): numpy.isnan,
}
