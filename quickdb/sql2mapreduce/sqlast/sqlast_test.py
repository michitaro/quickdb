from quickdb.sql2mapreduce.sqlast.sqlast import RowExpr
from typing import Any, Callable, Dict, Iterable, List, Set, Tuple, Type, cast
import unittest

from .sqlast import A_Const, ColumnRef, Context, Expression, NamedArgExpr, Select, Row, UnaryOperationExpression


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
            select.target_list[0].val.evaluate(self.context),
            ('columnref', ('forced', 'i', 'col1')),
        )
        self.assertEqual(select.target_list[1].name, 'column_2')
        self.assertEqual(
            select.target_list[1].val.evaluate(self.context),
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
        self.assertEqual(where.evaluate(self.context), ('columnref', ('forced', 'isprimary')))

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
        self.assertEqual(sort0.node.evaluate(self.context), ('columnref', ('object_id',)))
        self.assertFalse(sort0.reverse)
        self.assertEqual(sort1.node.evaluate(self.context), ('columnref', ('skymap_id',)))
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
            select.group_clause[0].evaluate(self.context),
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
            def columnref(self, ref: Tuple[str, ...]):
                return ('columnref', ref)

        return TestContext()


class Test_ColumnRef(unittest.TestCase):
    def test_columnref(self):
        sql = '''SELECT x.y from t'''
        select = Select(sql)
        self.assertEqual(
            select.target_list[0].val.evaluate(self.context),
            ('columnref', ('x', 'y')),
        )

    @property
    def context(self):
        class TestContext(Context):
            def columnref(self, ref: Tuple[str, ...]):
                return ('columnref', ref)
        return TestContext()


class Test_A_Const(unittest.TestCase):
    def test_string(self):
        sql = '''SELECT 'hello world' FROM t'''
        select = Select(sql)
        self.assertEqual(select.target_list[0].val.evaluate(self.context), 'hello world')

    def test_float(self):
        sql = '''SELECT 3.14 FROM t'''
        select = Select(sql)
        self.assertEqual(select.target_list[0].val.evaluate(self.context), 3.14)

    def test_int(self):
        sql = '''SELECT 42 FROM t'''
        select = Select(sql)
        self.assertEqual(select.target_list[0].val.evaluate(self.context), 42)

    @property
    def context(self):
        return Context()


class Test_BinaryOperationExpression(unittest.TestCase):
    def test_binary_operation(self):
        select = Select('SELECT 0 = 1 FROM t')
        self.assertEqual(select.target_list[0].val.evaluate(self.context), ('=', 0, 1))

    def test_binary_operations(self):
        ops = '''
        = <> < <= > >= + - * / ** //
        '''.split()
        for op in ops:
            select = Select(f'SELECT 0 {op} 1 FROM t')
            self.assertEqual(select.target_list[0].val.evaluate(self.context), (op, 0, 1))

    @property
    def context(self):
        class TestContext(Context):
            def binary_operation(self, name: str, left: Expression, right: Expression):
                return (name, left, right)
        return TestContext()


class Test_BetweenExpression(unittest.TestCase):
    def test_between(self):
        select = Select('''SELECT 'x' BETWEEN 'y' AND 'z' FROM t''')
        self.assertEqual(select.target_list[0].val.evaluate(self.context), ('between', 'x', 'y', 'z'))

    @property
    def context(self):
        class TestContext(Context):
            def between(self, a, b, c):
                return ('between', a, b, c)
        return TestContext()


class Test_UnaryOperation(unittest.TestCase):
    def test_unary_operation(self):
        sql = ''' SELECT - 'hello world' FROM t'''
        select = Select(sql)
        self.assertEqual(
            select.target_list[0].val.evaluate(self.context),
            ('unary', '-', 'hello world')
        )

    def test_unary_operations(self):
        ops = '- + ! ~ @'.split()
        for op in ops:
            sql = f''' SELECT {op} 'hello world' FROM t'''
            select = Select(sql)
            self.assertEqual(
                select.target_list[0].val.evaluate(self.context),
                ('unary', op, 'hello world')
            )

    @property
    def context(self):
        class TestContext(Context):
            def unary_operation(self, name: str, right):
                return ('unary', name, right)

        return TestContext()


class Test_BoolExpr(unittest.TestCase):
    def test_and(self):
        sql = ''' SELECT 'x' AND 'y' FROM t '''
        select = Select(sql)
        self.assertTrue(select.target_list[0].val.evaluate(self.context), ('AND', ['x', 'y']))

    def test_or(self):
        sql = ''' SELECT 'x' OR 'y' FROM t '''
        select = Select(sql)
        self.assertTrue(select.target_list[0].val.evaluate(self.context), ('OR', ['x', 'y']))

    def test_not(self):
        sql = ''' SELECT NOT 'x' FROM t '''
        select = Select(sql)
        self.assertTrue(select.target_list[0].val.evaluate(self.context), ('NOT', ['x']))

    @property
    def context(self):
        class TestContext(Context):
            def boolean_operation(self, op, args):
                return (op, args)

        return TestContext()


class Test_funccall(unittest.TestCase):
    def test_funccall(self):
        sql = ''' SELECT my.func('x') FROM t '''
        select = Select(sql)
        self.assertEqual(
            select.target_list[0].val.evaluate(self.context),
            ('funccall', (('my', 'func'), ['x'], {}, False)),
        )

    def test_funccall_with_named_args(self):
        sql = ''' SELECT my.func('w', arg1 => 'x', 'y', arg2 => (3, 4)) FROM t '''
        select = Select(sql)
        self.assertEqual(
            select.target_list[0].val.evaluate(self.context),
            ('funccall', (('my', 'func'), ['w', 'y'], {'arg1': 'x', 'arg2': Row(3, 4)}, False)),
        )

    @property
    def context(self):
        class TestContext(Context):
            def funccall(self, name: Tuple[str, ...], args: List, named_args: Dict, agg_star: bool, expressoin: 'Expression'):
                return ('funccall', (name, args, named_args, agg_star, ))

        return TestContext()


class Test_Index(unittest.TestCase):
    def test_funccall(self):
        sql = ''' SELECT x.y[42] FROM t '''
        select = Select(sql)
        self.assertEqual(
            select.target_list[0].val.evaluate(self.context),
            ('indirection', ('columnref', ('x', 'y')), 42),
        )

    @property
    def context(self):
        class TestContext(Context):
            def columnref(self, ref: Tuple[str, ...]):
                return ('columnref', ref)

            def indirection(self, arg, index):
                return ('indirection', arg, index)

        return TestContext()


class Test_A(unittest.TestCase):
    def test_astar(self):
        sql = ''' SELECT COUNT(*) FROM t'''
        select = Select(sql)
        self.assertEqual(
            select.target_list[0].val.evaluate(self.context),
            ('funccall', ('count', ), [], {}, True),
        )

    @property
    def context(self):
        class TestContext(Context):
            def funccall(self, name: Tuple[str, ...], args: List, named_args: Dict, agg_star: bool, expressoin: 'Expression'):
                return ('funccall', name, args, named_args, agg_star)

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
                        if e.evaluate(self.context) == value:
                            targets.pop(i)
                            break

            self.assertEqual(targets, [])

        # binary operation
        check_walked(''' 0 + 1 ''', [(A_Const, 0), (A_Const, 1)])
        # unary operation
        check_walked(''' - 'x' ''', [(A_Const, 'x')])
        # columnref
        check_walked(''' forced.i ''', [])
        # between
        check_walked(''' 'a' BETWEEN 'b' AND 'c' ''', [(A_Const, 'a'), (A_Const, 'b'), (A_Const, 'c')])
        # bool
        check_walked(''' 'a' AND 'b' ''', [(A_Const, 'a'), (A_Const, 'b')])
        # funccall
        check_walked(''' x.y(1, arg => 2) ''', [
            (A_Const, 1),
            (A_Const, 2),
        ])
        # row
        check_walked(''' x.y(range => (0, 1)) ''', [
            (RowExpr, Row(0, 1)),
            (A_Const, 0),
            (A_Const, 1),
        ])
        # indirection
        check_walked(''' a[0] ''', [
            (ColumnRef, ('columnref', ('a', ))),
        ])

    @property
    def context(self):
        class TestContext(Context):
            def columnref(self, ref: Tuple[str, ...]):
                return ('columnref', ref)
        return TestContext()
