import numpy
import scipy.special


def logical_and(*args):
    p = args[0]
    for a in args[1:]:
        p = numpy.logical_and(p, a)
    return p


def logical_or(*args):
    p = args[0]
    for a in args[1:]:
        p = numpy.logical_or(p, a)
    return p


def between(a, b, c):
    return numpy.logical_and(b <= a, a <= c)


def factorial(a):
    return scipy.factorial.factorial(a)
