import textwrap


def evaluate(pycode, shared=None):
    if shared is None:
        shared = {}
    pycode = textwrap.dedent(pycode)
    exec(pycode, shared)
    return shared
