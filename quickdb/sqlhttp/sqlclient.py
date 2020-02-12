import http.client
from quickdb.datarake.interface import Progress
import time
from collections import OrderedDict
from typing import Dict, List, NamedTuple

from quickdb.datarake.interface import ProgressCB

from . import jsonnpy
from . import config

def post_sql(sql, shared: Dict=None, progress: ProgressCB = None, polling_interval=1):
    conn = http.client.HTTPConnection(config.server_addr)
    try:
        conn.request("POST", '/jobs',
                     body=jsonnpy.dumps({'sql': sql, 'shared': shared or {}, 'deferred': not not progress}),
                     headers={"Content-type": "application/hscssp-jsonnpy", "Accept": "application/hscssp-jsonnpy"})
        response = conn.getresponse()
        if response.status != 200:
            raise RuntimeError(f'HTTP Error: {response.reason}')
        res = jsonnpy.loads(response.read())
    finally:
        conn.close()

    if progress:
        res = _wait_job(res['job_id'], progress, polling_interval)

    if res['status'] == 'error':
        raise RuntimeError(res['reason'])
    assert res['status'] == 'done', f'Unknown status: {res["status"]}'
    return QueryResult(
        res['result']['target_names'],
        res['result']['target_list']
    )


def _wait_job(job_id: str, progress: ProgressCB, polling_interval: float):
    def get():
        try:
            conn = http.client.HTTPConnection(config.server_addr)
            conn.request("GET", f'/jobs/{job_id}',
                         headers={"Content-type": "application/hscssp-jsonnpy", "Accept": "application/hscssp-jsonnpy"})
            response = conn.getresponse()
            if response.status != 200:
                raise RuntimeError(f'HTTP Error: {response.reason}')
            res = jsonnpy.loads(response.read())
        finally:
            conn.close()
        return res

    last_total = 1
    while True:
        res = get()
        if res['status'] in {'done', 'error'}:
            progress(Progress(done=last_total, total=last_total))
            return res
        if res['status'] == 'running':
            p = res['progress']
            if p:
                last_total = p['total']
                progress(Progress(done=p['done'], total=p['total']))
        else:
            assert res['status'] == 'done', f'Unknown status: {res["status"]}'
        time.sleep(polling_interval)


def post_sql_with_tqdm(sql: str, shared: Dict=None, polling_interval=0.1, ncols=None):
    import contextlib
    from tqdm import tqdm

    @contextlib.contextmanager
    def progress_bar():
        with tqdm(total=1, ncols=ncols) as pbar:
            def progress(p):
                pbar.total = p.total
                pbar.n = p.done
                pbar.refresh()
            yield progress

    with progress_bar() as progress:
        return post_sql(sql, shared, progress, polling_interval)


class QueryResult(NamedTuple):
    target_names: List[str]
    target_list: List

    def dataframe(self):
        import pandas
        return pandas.DataFrame.from_dict(OrderedDict(zip(self.target_names, self.target_list)))
