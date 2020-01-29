from wsgiref.util import setup_testing_defaults
from wsgiref.simple_server import make_server

from . import jsonnpy
import sql2mapreduce.aggr
import sql2mapreduce.nonaggr
import datarake.master
from sql2mapreduce import sqlast


def app(environ, start_response):
    try:
        request_body_size = int(environ.get('CONTENT_LENGTH', 0))
    except ValueError:
        request_body_size = 0
    assert request_body_size < 1_000_000

    request = jsonnpy.loads(environ['wsgi.input'].read(request_body_size))

    error = None
    result = None
    time = {}
    try:
        result = exec_sql(request['sql'], shared=request.get('shared'), time=time)
    except:
        import traceback
        error = traceback.format_exc()

    status = '200 OK'
    headers = [('Content-type', 'application/hscssp-jsonnpy')]
    start_response(status, headers)

    response_body = bytes(jsonnpy.dumps({'result': result, 'error': error, 'time': time}))
    return [response_body]


def exec_sql(sql, shared={}, time=None):
    if is_aggr_call(sql):
        result, target_names = exec_aggr_call(sql, {'shared': shared}, time)
        if len(result) == 0:
            return
        n_cols = len(result[0][1])
        return [('group', [row[0] for row in result])] + [(target_names[j],  [row[1][j] for row in result]) for j in range(n_cols)]
    else:
        result, target_names = exec_nonaggr_sql(sql, {'shared': shared}, time)
        return ((n, len(s.shape) > 1 and list(s) or s) for i, (s, n) in enumerate(zip(result['select'], target_names)))


def exec_nonaggr_sql(sql, context, time):
    make_env, target_names = sql2mapreduce.nonaggr.sql2make_env(sql)
    return datarake.master.run(make_env, context, time), target_names


def exec_aggr_call(sql, context, times={}):
    make_envs, stmt, target_names = sql2mapreduce.aggr.sql2make_envs(sql)
    agg_results = []
    context['AggregateResult'] = agg_results
    results = None
    for i, make_env in enumerate(make_envs):
        time = {}
        times[i] = time
        results = datarake.master.run(make_env, context, time=time)
        agg_results.append(results)
    pcb = sql2mapreduce.config.PycodeBuilder(stmt, 't', 'a')

    if results is None:
        raise RuntimeError('Nothing to evaluate')

    items = []
    for g, a in results.items():
        items.append((a.group_value, [eval(pcb(t.val), dict(context, a=a)) for t in stmt.targetList]))
    return items, target_names


def is_aggr_call(sql):
    stmts = sqlast.parse_sql(sql)
    sqlast.check(len(stmts) == 1, 'Too many statements')
    stmt = stmts[0].stmt
    sqlast.check(isinstance(stmt, sqlast.SelectStmt), f'Not supported statement: {stmt.__class__.__name__}')
    aggr_calls = []
    def f(ast):
        if sql2mapreduce.aggr.is_aggr_call(ast):
            aggr_calls.append(ast)
    stmt.walk(f)
    return len(aggr_calls) > 0


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, default=8002)
    args = parser.parse_args()

    with make_server('', args.port, app) as httpd:
        print(f"Serving on port {args.port}...")
        httpd.serve_forever()
