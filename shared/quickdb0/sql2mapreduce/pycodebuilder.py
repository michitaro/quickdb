class PycodeBuilder(object):
    def __init__(self, stmt, t, a='a'):
        self.stmt = stmt
        self.t = t
        self.a = a

    def __call__(self, ast):
        raise NotImplementedError()

    @classmethod
    def is_series(cls, ast):
        raise NotImplementedError()
