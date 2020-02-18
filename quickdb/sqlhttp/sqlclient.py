import contextlib
import http.client
import signal
import time
from collections import OrderedDict
from contextlib import contextmanager
from typing import Callable, Dict, List, NamedTuple

from quickdb.datarake.interface import Progress, ProgressCB

from . import config, jsonnpy


def post_sql(sql, shared: Dict = None, progress: ProgressCB = None, polling_interval=1):
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
    def kick(method: str):
        try:
            conn = http.client.HTTPConnection(config.server_addr)
            conn.request(method, f'/jobs/{job_id}',
                         headers={"Content-type": "application/hscssp-jsonnpy", "Accept": "application/hscssp-jsonnpy"})
            response = conn.getresponse()
            if response.status != 200:
                raise RuntimeError(f'HTTP Error: {response.reason}')
            res = jsonnpy.loads(response.read())
        finally:
            conn.close()
        return res

    with trap_keyboard_interrupt(lambda *args: kick('DELETE')):
        last_total = 1
        while True:
            res = kick('GET')
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


@contextlib.contextmanager
def _progress_bar(ncols=None):
    from tqdm import tqdm
    with tqdm(total=1, ncols=None) as pbar:
        def progress(p: Progress):
            pbar.total = p.total
            pbar.n = p.done
            pbar.refresh()
        yield progress


def post_sql_with_tqdm(sql: str, shared: Dict = None, polling_interval=0.1, ncols: int = None):
    with _progress_bar(ncols) as progress:
        return post_sql(sql, shared, progress, polling_interval)


class PartialData(NamedTuple):
    progress: Progress
    target_list: List


def post_sql_streaming(sql, shared: Dict = None):
    conn = http.client.HTTPConnection(config.server_addr)
    try:
        conn.request("POST", '/jobs',
                     body=jsonnpy.dumps({'sql': sql, 'shared': shared or {}, 'streaming': True}),
                     headers={"Content-type": "application/hscssp-jsonnpy", "Accept": "application/hscssp-jsonnpy"})
        response = conn.getresponse()
        if response.status != 200:
            raise RuntimeError(f'HTTP Error: {response.reason}')
        while True:
            msg = jsonnpy.load(response)
            msg_type = msg['type']
            if msg_type == 'progress':
                p = msg['progress']
                yield PartialData(
                    progress=Progress(done=p['done'], total=p['total']),
                    target_list=p['data'][0],
                )
            elif msg_type == 'error':
                raise RuntimeError(msg['reason'])
            else:
                assert msg_type == 'end', f'Unknown message type: {msg}'
                break
    finally:
        conn.close()


def post_sql_streaming_with_tqdm(sql: str, shared: Dict = None, ncols: int = None):
    with _progress_bar(ncols) as progress:
        for pd in post_sql_streaming(sql, shared):
            progress(pd.progress)
            yield pd


@contextmanager
def trap_keyboard_interrupt(cb: Callable):
    default_handler = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGINT, cb)
    yield
    signal.signal(signal.SIGINT, default_handler)


class QueryResult(NamedTuple):
    target_names: List[str]
    target_list: List

    def dataframe(self):
        import pandas
        return pandas.DataFrame.from_dict(OrderedDict(zip(self.target_names, self.target_list)))
