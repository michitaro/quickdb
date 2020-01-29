import os
import hashlib
import functools
import textwrap


def hash(value):
    m = hashlib.sha512()
    m.update(value + keychain.password)
    return bytes(m.hexdigest(), 'utf-8')


def evaluate(pycode, context=None):
    if context is None:
        context = {}
    pycode = textwrap.dedent(pycode)
    exec(pycode, context)
    return context


def reduce(reducer, values, initial):
    if initial is None:
        return functools.reduce(reducer, values)
    else:
        return functools.reduce(reducer, values, initial)


def load_file(fname):
    import importlib.util
    import uuid
    spec = importlib.util.spec_from_file_location(f'm{uuid.uuid1().hex}', fname)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class Keychain:
    @property
    @functools.lru_cache(None)
    def password(self):
        password_path = f'{os.path.dirname(__file__)}/secrets/password'
        assert os.stat(password_path).st_mode & 0o077 == 0
        with open(password_path) as f:
            password = bytes(f.read().strip(), 'utf-8')
            assert len(password) >= 256
        return password

keychain = Keychain()
