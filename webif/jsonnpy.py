import json
import numpy
import types
import io


def dump(obj, out_stream):
    arrays = []
    class Encoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, numpy.ndarray):
                id = len(arrays)
                arrays.append(obj)
                return {'__numpy.ndarray__': True, 'id': id}
            if isinstance(obj, types.GeneratorType):
                return list(obj)
            if isinstance(obj, numpy.bool_):
                return not (obj == False)
            return json.JSONEncoder.default(self, obj)
    layout = json.dumps(obj, cls=Encoder).encode('utf-8')
    out_stream.write(f'{ len(layout) }\n'.encode('utf-8'))
    out_stream.write(layout)
    arrays.append(0)
    numpy.savez(out_stream, *arrays)


def dumps(obj):
    buf = io.BytesIO()
    dump(obj, buf)
    return buf.getbuffer()


def load(in_stream):
    size_str = in_stream.readline()
    layout_size = int(size_str)
    layout_json = in_stream.read(layout_size)
    arrays = numpy.load(in_stream, allow_pickle=False)
    def as_array(d):
        if '__numpy.ndarray__' in d:
            return arrays[f'arr_{ d["id"] }']
        return d
    layout = json.loads(layout_json, object_hook=as_array)
    return layout


def loads(s):
    buf = io.BytesIO(s)
    return load(buf)
