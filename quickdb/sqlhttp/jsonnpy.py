import json
import logging
import numpy
import types
import io


def dump(obj, out_stream):
    arrays = []

    class Encoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, (numpy.ndarray, numpy.float32, numpy.int64)):
                id = len(arrays)
                arrays.append(obj)
                return {'__numpy.ndarray__': True, 'id': id}
            if isinstance(obj, types.GeneratorType):
                return list(obj)
            if isinstance(obj, numpy.bool_):
                return not (obj == False)
            return json.JSONEncoder.default(self, obj)
    try:
        layout = json.dumps(obj, cls=Encoder).encode()
    except:
        logging.error(f'jsonnpy error: {obj}')
        raise
    out_stream.write(f'{ len(layout) }\n'.encode())
    out_stream.write(layout)
    npz = _npz_dumps(arrays)
    out_stream.write(f'{ len(npz) }\n'.encode())
    out_stream.write(npz)


def _npz_dumps(arrays):
    bio = io.BytesIO()
    numpy.savez(bio, *arrays)
    return bio.getvalue()


def _npz_loads(b: bytes):
    bio = io.BytesIO(b)
    return numpy.load(bio)


def dumps(obj):
    buf = io.BytesIO()
    dump(obj, buf)
    return buf.getvalue()


def load(in_stream):
    layout_size = int(in_stream.readline())
    layout_json = in_stream.read(layout_size)
    npz_size = int(in_stream.readline())
    arrays = _npz_loads(in_stream.read(npz_size))
    def as_array(d):
        if '__numpy.ndarray__' in d:
            return arrays[f'arr_{ d["id"] }'] # type: ignore
        return d
    layout = json.loads(layout_json, object_hook=as_array)
    return layout


def loads(s):
    buf = io.BytesIO(s)
    return load(buf)
