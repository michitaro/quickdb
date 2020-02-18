import os
import queue
import secrets
import threading
import traceback
from typing import Dict, NamedTuple

from flask import Flask, Response, abort, make_response, request

from quickdb.datarake import master
from quickdb.datarake.interface import Progress, ProgressCB
from quickdb.datarake.safeevent import SafeEvent
from quickdb.sql2mapreduce import agg_test, run_sql
from quickdb.sql2mapreduce.sqlast.sqlast import SqlError
from quickdb.sspcatalog.errors import UserError

from . import jsonnpy

app = Flask(__name__)

run_make_env = agg_test.run_make_env if os.environ.get('TEST') else master.run_make_env


jobs: Dict[str, 'Job'] = {}


@app.route('/jobs', methods=['POST'])
def create_job():
    if request.content_type != 'application/hscssp-jsonnpy':
        abort(400)
    req = jsonnpy.loads(request.data)
    if req.get('streaming'):
        return streaming_response(req)
    job = Job(req['sql'], req.get('shared', {}))
    if req.get('deferred'):
        return jsonnpy_response({'job_id': job.id})
    else:
        job.wait()
        return jsonnpy_response(resonse_for(job))


def streaming_response(req: Dict):
    def g():
        with SafeEvent() as interrupt:
            q = queue.Queue()

            def on_progress(p: Progress):
                q.put(p)

            def job_thread():
                try:
                    run_sql(req['sql'], run_make_env, req.get('shared', {}), progress=on_progress, interrupt_notifiyer=interrupt, streaming=True)
                except (UserError, SqlError) as error:
                    q.put(RuntimeError(str(error)))
                except Exception as error:
                    q.put(RuntimeError(traceback.format_exc()))
                finally:
                    q.put(None)

            th = threading.Thread(target=job_thread)
            th.start()
            try:
                while True:
                    msg = q.get()
                    if msg is None:
                        yield jsonnpy.dumps({'type': 'end'})
                        break
                    elif isinstance(msg, Progress):
                        yield jsonnpy.dumps({'type': 'progress', 'progress': msg._asdict()})
                    else:
                        assert isinstance(msg, Exception), f'Unknwon message type: {repr(msg)}'
                        yield jsonnpy.dumps({'type': 'error', 'reason': str(msg)})
            except GeneratorExit:
                interrupt.set()
            th.join()

    return Response(g(), mimetype='application/hscssp-jsonnpy')


@app.route('/jobs/<job_id>')
def show_job(job_id: str):
    job = jobs.get(job_id)
    if job is None:
        abort(404)
    else:
        return jsonnpy_response(resonse_for(job))


@app.route('/jobs/<job_id>', methods=['DELETE'])
def stop_job(job_id: str):
    job = jobs.get(job_id)
    if job and job.interrupt:
        job.interrupt.set()
        return jsonnpy_response({})


def jsonnpy_response(data):
    return make_response((jsonnpy.dumps(data), 200, {'Content-Type': 'application/hscssp-jsonnpy'}))


def resonse_for(job: 'Job'):
    progress = job.progress
    result = job.result
    error = job.error
    if error is not None:
        return {
            'status': 'error',
            'reason': error,
        }
    if result is not None:
        jobs.pop(job.id, None)
        return {
            'status': 'done',
            'result': result._asdict(),
        }
    else:
        return {
            'status': 'running',
            'progress': progress and progress._asdict(),
        }


class Job:
    def __init__(self, sql: str, shared: Dict, on_progress: ProgressCB = None):
        self.id = secrets.token_hex(16)
        self._sql = sql
        self._shared = shared
        self._event = threading.Event()
        self.progress = None
        self.result = None
        self.error = None
        self.interrupt = None
        self._on_progress = on_progress
        th = threading.Thread(target=self._run, args=(self,))
        th.start()
        self._th = th
        jobs[self.id] = self

    def _update_progress(self, p: Progress):
        self.progress = p
        self._on_progress and self._on_progress(p)

    def _run(self, job):
        try:
            with SafeEvent() as interrupt:
                self.interrupt = interrupt
                self.result = run_sql(self._sql, run_make_env, shared=self._shared, progress=self._update_progress, interrupt_notifiyer=interrupt)
        except (UserError, SqlError) as e:
            self.error = str(e)
        except:
            self.error = traceback.format_exc()
        finally:
            self._event.set()
            threading.Timer(30., self._delete).start()

    def _delete(self):
        jobs.pop(self.id, None)

    def wait(self):
        self._event.wait()
        self._delete()
