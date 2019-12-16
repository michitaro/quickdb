import numpy


def flux_to_mag(flux):
    mag0 = 27
    return -5./2. * numpy.log10(flux) + mag0


def flux_error_to_mag_error(flux, flux_error):
    return 5. * flux_error / (2. * numpy.log(10) * flux)


def mag_to_flux(mag):
    mag0 = 27
    return 10 ** (2. / 5. * (mag0 - mag))


def is_finite(a):
    return numpy.isfinite(a)


mapping = {
    ('f2m',): 'flux_to_mag',
    ('flux_to_mag',): 'flux_to_mag',
    ('fe2me',): 'flux_error_to_mag_error',
    ('flux_error_to_mag_error',): 'flux_error_to_mag_error',
    ('mag_to_flux',): 'mag_to_flux',
    ('m2f',): 'mag_to_flux',
    ('is_finite',): 'is_finite',
}
