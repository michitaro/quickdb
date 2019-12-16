from . import sqlast


class Aggregator(object):
    n_args = -1
    required_args = []

    def __init__(self, group_value):
        self.group_value = group_value

    def call_map_args_pycode(cls, fc, pcb):
        raise NotImplementedError()

    @classmethod
    def check_ast_args(cls, ast):
        cls._add_missing_ast_args(ast)
        if cls.n_args >= 0:
            sqlast.check(
                len(ast.nameless_args) == cls.n_args,
                f'{ ".".join(ast.name_tuple) }() takes exactly { cls.n_args } argument ({ len(ast.nameless_args) } given)')
        for name in cls.required_args:
            sqlast.check(
                name in ast.named_args,
                f'{ ".".join(ast.name_tuple) }() requires argument {name}')

    @classmethod
    def _add_missing_ast_args(cls, ast):
        pass

    @classmethod
    def build_call_ast(cls, *args):
        from . import config
        r_mapping = {v: k for k, v in config.aggregators.mapping.items()}
        funcname = [sqlast.String.build(str=name) for name in r_mapping[cls]]
        return sqlast.FuncCall.build(funcname=funcname, args=list(args))
