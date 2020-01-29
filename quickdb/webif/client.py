import http.client
from . import jsonnpy
from . import config
from collections import OrderedDict

def post_sql(sql, shared={}):
    headers = {"Content-type": "application/hscssp-jsonnpy", "Accept": "application/hscssp-jsonnpy"}
    conn = http.client.HTTPConnection(config.webserver)

    request_body = jsonnpy.dumps({'sql': sql, 'shared': shared})
    conn.request("POST", '', body=request_body)
    response = conn.getresponse()

    if response.status != 200:
        raise RuntimeError(response.reason)

    response = jsonnpy.loads(response.read())
    conn.close()

    if response['error'] is not None:
        raise RuntimeError(response['error'])

    return response


def dataframe(sql, shared={}, time=False):
    import pandas
    from pprint import pprint
    res = post_sql(sql, shared=shared)
    if time:
        pprint(res['time'])
    return pandas.DataFrame.from_dict(OrderedDict(res['result']))
