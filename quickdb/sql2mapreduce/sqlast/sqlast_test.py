import math
import unittest
from typing import Any, List, Set, Tuple, Type, cast

from .sqlast import (BetweenExpression, BinaryOperationExpression,
                     BoolExpression, ColumnRefExpression, ConstExpression,
                     Context, Expression, FuncCallExpression,
                     IndirectionExpression, RowExpr, RowExpression, Select,
                     UnaryOperationExpression, SharedValueRefExpression)


class Test_Select(unittest.TestCase):
    def test_targetlist(self):
        sql = '''
            SELECT
                forced.i.col1,
                col2 as "column_2"
            FROM
                pdr2_dud
        '''
        select = Select(sql)
        self.assertIsNone(select.target_list[0].name)
        self.assertEqual(
            select.target_list[0].val(self.context),
            ('columnref', ('forced', 'i', 'col1')),
        )
        self.assertEqual(select.target_list[1].name, 'column_2')
        self.assertEqual(
            select.target_list[1].val(self.context),
            ('columnref', ('col2', )),
        )

    def test_from_clause(self):
        sql = '''
            SELECT object_id FROM
                pdr2_dud as "t"
        '''
        select = Select(sql)
        self.assertEqual(select.from_clause.relname, 'pdr2_dud')
        self.assertEqual(select.from_clause.alias, 't')

    def test_where_clause(self):
        sql = '''
            SELECT object_id FROM pdr2_dud
            WHERE
                forced.isprimary
        '''
        select = Select(sql)
        where = cast(Expression, select.where_clause)
        self.assertEqual(where(self.context), ('columnref', ('forced', 'isprimary')))

    def test_order_clause(self):
        sql = '''
            SELECT object_id FROM pdr2_dud
            ORDER BY
                object_id,
                skymap_id DESC
        '''
        select = Select(sql)
        sort0 = select.sort_clause[0]
        sort1 = select.sort_clause[1]
        self.assertEqual(sort0.node(self.context), ('columnref', ('object_id',)))
        self.assertFalse(sort0.reverse)
        self.assertEqual(sort1.node(self.context), ('columnref', ('skymap_id',)))
        self.assertTrue(sort1.reverse)

    def test_limit_count(self):
        sql = '''
            SELECT object_id FROM pdr2_dud
            LIMIT 100 OFFSET 200
        '''
        select = Select(sql)
        self . assertEqual(select.limit_count, 100)

    def test_limit_offset(self):
        sql = '''
            SELECT object_id FROM pdr2_dud
            OFFSET 200
        '''
        select = Select(sql)
        self.assertEqual(select.limit_offset, 200)

    def test_group_clause(self):
        sql = '''
            SELECT object_id FROM pdr2_dud
            GROUP BY
                is_a_star
        '''
        select = Select(sql)
        self.assertEqual(
            select.group_clause[0](self.context),
            ('columnref', ('is_a_star',)),
        )

    def test_no_optional_clauses(self):
        sql = '''
            SELECT object_id FROM pdr2_dud
        '''
        select = Select(sql)
        self.assertIsNone(select.where_clause)
        self.assertIsNone(select.sort_clause)
        self.assertIsNone(select.limit_count)
        self.assertIsNone(select.limit_offset)

    def test_select_can_be_pickled(self):
        sql = '''
            SELECT object_id FROM pdr2_dud
        '''
        select = Select(sql)
        import pickle
        pickle.loads(pickle.dumps(select))  # should not raise any error

    @property
    def context(self):
        class TestContext(Context):
            def evaluate_ColumnRefExpression(self, e: ColumnRefExpression):
                return ('columnref', e.fields)

        return TestContext()


def subsql2expression(subsql: str):
    sql = f'''SELECT {subsql} FROM t'''
    select = Select(sql)
    return select.target_list[0].val


class TestRepr(unittest.TestCase):
    def test_repr(self):
        # just for coverage
        str(subsql2expression('''
            a + b > 0 AND NOT (c NOT BETWEEN c AND d[2]) OR x.y.z(arg1, named_arg => 3, myrow => (1, 2), COUNT(*)) AND shared.w + (- value)
        '''))

class Test_ColumnRef(unittest.TestCase):
    def test_columnref(self):
        sql = '''SELECT x.y from t'''
        select = Select(sql)
        self.assertEqual(
            select.target_list[0].val(self.context),
            ('columnref', ('x', 'y')),
        )

    @property
    def context(self):
        class TestContext(Context):
            def evaluate_ColumnRefExpression(self, e: ColumnRefExpression):
                return ('columnref', e.fields)
        return TestContext()


class Test_SharedValueRef(unittest.TestCase):
    def test_shared_value_ref(self):
        sql = '''SELECT shared.x from t'''
        select = Select(sql)
        self.assertEqual(
            select.target_list[0].val(self.context),
            ('shared', 'x'),
        )

    @property
    def context(self):
        class TestContext(Context):
            def evaluate_SharedValueRefExpression(self, e: SharedValueRefExpression):
                return ('shared', e.name)
        return TestContext()


class Test_builtin_constants(unittest.TestCase):
    def test_pi(self):
        sql = '''SELECT pi from t'''
        select = Select(sql)
        self.assertEqual(
            select.target_list[0].val(self.context),
            math.pi,
        )

    @property
    def context(self):
        class TestContext(Context):
            def evaluate_ConstExpression(self, e: ConstExpression):
                return e.value
        return TestContext()


class Test_ConstExpression(unittest.TestCase):
    def test_string(self):
        sql = '''SELECT 'hello world' FROM t'''
        select = Select(sql)
        self.assertEqual(select.target_list[0].val(self.context), 'hello world')

    def test_float(self):
        sql = '''SELECT 3.14 FROM t'''
        select = Select(sql)
        self.assertEqual(select.target_list[0].val(self.context), 3.14)

    def test_int(self):
        sql = '''SELECT 42 FROM t'''
        select = Select(sql)
        self.assertEqual(select.target_list[0].val(self.context), 42)

    @property
    def context(self):
        class TestContext(Context):
            def evaluate_ConstExpression(self, e: ConstExpression):
                return e.value
        return TestContext()


class Test_BinaryOperationExpression(unittest.TestCase):
    def test_binary_operation(self):
        select = Select('SELECT 0 = 1 FROM t')
        self.assertEqual(select.target_list[0].val(self.context), ('=', 0, 1))

    def test_binary_operations(self):
        ops = '''
        = <> < <= > >= + - * / ** //
        '''.split()
        for op in ops:
            select = Select(f'SELECT 0 {op} 1 FROM t')
            self.assertEqual(select.target_list[0].val(self.context), (op, 0, 1))

    @property
    def context(self):
        class TestContext(Context):
            def evaluate_BinaryOperationExpression(self, e: BinaryOperationExpression):
                return (e.name, e.a(self), e.b(self))

            def evaluate_ConstExpression(self, e: ConstExpression):
                return e.value
        return TestContext()


class Test_BetweenExpression(unittest.TestCase):
    def test_between(self):
        select = Select('''SELECT 'x' BETWEEN 'y' AND 'z' FROM t''')
        self.assertEqual(select.target_list[0].val(self.context), ('between', 'x', 'y', 'z', False))

    def test_not_between(self):
        select = Select('''SELECT 'x' NOT BETWEEN 'y' AND 'z' FROM t''')
        self.assertEqual(select.target_list[0].val(self.context), ('between', 'x', 'y', 'z', True))

    @property
    def context(self):
        class TestContext(Context):
            def evaluate_BetweenExpression(self, e: BetweenExpression):
                return ('between', e.a(self), e.b(self), e.c(self), e.negate)

            def evaluate_ConstExpression(self, e: ConstExpression):
                return e.value
        return TestContext()


class Test_UnaryOperation(unittest.TestCase):
    def test_unary_operation(self):
        sql = ''' SELECT - 'hello world' FROM t'''
        select = Select(sql)
        self.assertEqual(
            select.target_list[0].val(self.context),
            ('unary', '-', 'hello world')
        )

    def test_unary_operations(self):
        ops = '- + ! ~ @'.split()
        for op in ops:
            sql = f''' SELECT {op} 'hello world' FROM t'''
            select = Select(sql)
            self.assertEqual(
                select.target_list[0].val(self.context),
                ('unary', op, 'hello world')
            )

    @property
    def context(self):
        class TestContext(Context):
            def evaluate_UnaryOperationExpression(self, e: UnaryOperationExpression):
                return ('unary', e.name, e.a(self))

            def evaluate_ConstExpression(self, e: ConstExpression):
                return e.value

        return TestContext()


class Test_BoolExpr(unittest.TestCase):
    def test_and(self):
        sql = ''' SELECT 'x' AND 'y' FROM t '''
        select = Select(sql)
        self.assertTrue(select.target_list[0].val(self.context), ('AND', ['x', 'y']))

    def test_or(self):
        sql = ''' SELECT 'x' OR 'y' FROM t '''
        select = Select(sql)
        self.assertTrue(select.target_list[0].val(self.context), ('OR', ['x', 'y']))

    def test_not(self):
        sql = ''' SELECT NOT 'x' FROM t '''
        select = Select(sql)
        self.assertTrue(select.target_list[0].val(self.context), ('NOT', ['x']))

    @property
    def context(self):
        class TestContext(Context):
            def evaluate_BoolExpression(self, e: BoolExpression):
                return e.name, e.args

            def evaluate_ConstExpression(self, e: ConstExpression):
                return e.value

        return TestContext()


class Test_FuncCall(unittest.TestCase):
    def test_funccall(self):
        sql = ''' SELECT my.func('x') FROM t '''
        select = Select(sql)
        self.assertEqual(
            select.target_list[0].val(self.context),
            ('funccall', (('my', 'func'), ['x'], {}, False)),
        )

    def test_funccall_with_named_args(self):
        sql = ''' SELECT my.func('w', arg1 => 'x', 'y', arg2 => (3, 4)) FROM t '''
        select = Select(sql)
        self.assertEqual(
            select.target_list[0].val(self.context),
            ('funccall', (('my', 'func'), ['w', 'y'], {'arg1': 'x', 'arg2': [3, 4]}, False)),
        )

    @property
    def context(self):
        class TestContext(Context):
            def evaluate_FuncCallExpression(self, e: FuncCallExpression):
                return ('funccall', (e.name, [a(self) for a in e.args], {k: v(self) for k, v in e.named_args.items()}, e.agg_star))

            def evaluate_ConstExpression(self, e: ConstExpression):
                return e.value

            def evaluate_RowExpression(self, e: RowExpression):
                return [a(self) for a in e.args]

        return TestContext()


class Test_Indirection(unittest.TestCase):
    def test_funccall(self):
        sql = ''' SELECT x.y[42] FROM t '''
        select = Select(sql)
        self.assertEqual(
            select.target_list[0].val(self.context),
            ('indirection', ('columnref', ('x', 'y')), 42),
        )

    @property
    def context(self):
        class TestContext(Context):
            def evaluate_ColumnRefExpression(self, e: ColumnRefExpression):
                return ('columnref', e.fields)

            def evaluate_IndirectionExpression(self, e: IndirectionExpression):
                return ('indirection', e.arg(self), e.index)

        return TestContext()


class Test_A_Star(unittest.TestCase):
    def test_astar(self):
        sql = ''' SELECT COUNT(*) FROM t'''
        select = Select(sql)
        self.assertEqual(
            select.target_list[0].val(self.context),
            ('funccall', ('count', ), [], {}, True),
        )

    @property
    def context(self):
        class TestContext(Context):
            def evaluate_FuncCallExpression(self, e: FuncCallExpression):
                return ('funccall', e.name, e.args, e.named_args, e.agg_star)

        return TestContext()


class Test_walk(unittest.TestCase):
    def test_walk(self):
        def expression(subsql: str):
            sql = f'''SELECT {subsql} FROM t'''
            select = Select(sql)
            return select.target_list[0].val

        def check_walked(subsql: str, targets: List[Tuple[Type[Expression], Any]]):
            walked: List[Expression] = []

            def cb(e: Expression):
                walked.append(e)

            expression(subsql).walk(cb)

            for e in walked:
                for i, (cls, value) in enumerate(targets):
                    if isinstance(e, cls):
                        if e(self.context) == value:
                            targets.pop(i)
                            break

            self.assertEqual(targets, [])

        # binary operation
        check_walked(''' 0 + 1 ''', [(ConstExpression, 0), (ConstExpression, 1)])
        # unary operation
        check_walked(''' - 'x' ''', [(ConstExpression, 'x')])
        # columnref
        check_walked(''' forced.i ''', [])
        # shared value
        check_walked(''' shared.x ''', [])
        # between
        check_walked(''' 'a' BETWEEN 'b' AND 'c' ''', [(ConstExpression, 'a'), (ConstExpression, 'b'), (ConstExpression, 'c')])
        # bool
        check_walked(''' 'a' AND 'b' ''', [(ConstExpression, 'a'), (ConstExpression, 'b')])
        # funccall
        check_walked(''' x.y(1, arg => 2) ''', [
            (ConstExpression, 1),
            (ConstExpression, 2),
        ])
        # row
        check_walked(''' x.y(range => (0, 1)) ''', [
            (RowExpression, [0, 1]),
            (ConstExpression, 0),
            (ConstExpression, 1),
        ])
        # indirection
        check_walked(''' a[0] ''', [
            (ColumnRefExpression, ('columnref', ('a', ))),
        ])

    def check_walk_break_if(self):
        walked_e: Set[Expression] = set()

        def walk(e: Expression):
            walked_e.add(e)

        def break_if(e: Expression):
            return isinstance(e, RowExpr)

        sql = ''' SELECT (1, 2) FROM t'''
        select = Select(sql)
        select.target_list[0].val.walk(walk, break_if)
        walked_v = {e(self.context) for e in walked_e}
        self.assertIn([1, 2], walked_v)
        self.assertNotIn(1, walked_v)
        self.assertNotIn(2, walked_v)

    @property
    def context(self):
        class TestContext(Context):
            def evaluate_ConstExpression(self, e: ConstExpression):
                return e.value

            def evaluate_ColumnRefExpression(self, e: ColumnRefExpression):
                return ('columnref', e.fields)

            def evaluate_RowExpression(self, e: RowExpression):
                return [a(self) for a in e.args]
        return TestContext()
