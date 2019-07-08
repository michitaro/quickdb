import numpy

def save(outfile, array):
    return numpy.save(outfile, native_byteorder(array), allow_pickle=False)

def load(infile):
    return numpy.load(infile)

def native_byteorder(a):
    if a.dtype.byteorder != '=':
        return a.byteswap().newbyteorder('=')
    else:
        return a
