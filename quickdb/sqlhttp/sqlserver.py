from quickdb.sql2mapreduce.sqlast.sqlast import SqlError
import secrets
import traceback
import threading
from typing import Dict

from flask import Flask, abort, request, make_response

from quickdb.datarake.interface import Progress
from quickdb.datarake import master
from quickdb.sql2mapreduce import run_sql
from quickdb.sql2mapreduce import agg_test
from quickdb.sspcatalog.errors import UserError

from . import jsonnpy

app = Flask(__name__)

run_make_env = app.config['TESTING'] and agg_test.run_make_env or master.run_make_env

jobs: Dict[str, 'Job'] = {}


@app.route('/jobs', methods=['POST'])
def create_job():
    if request.content_type != 'application/hscssp-jsonnpy':
        abort(400)
    req = jsonnpy.loads(request.data)
    job = Job(req['sql'], req.get('shared', {}))
    if req.get('deferred'):
        return jsonnpy_response({'job_id': job.id})
    else:
        job.wait()
        return jsonnpy_response(resonse_for(job))


@app.route('/jobs/<job_id>')
def show_job(job_id: str):
    job = jobs.get(job_id)
    if job is None:
        abort(404)
    else:
        return jsonnpy_response(resonse_for(job))
        


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
    def __init__(self, sql: str, shared: Dict):
        self.id = secrets.token_hex(16)
        self._sql = sql
        self._shared = shared
        self._event = threading.Event()
        self.progress = None
        self.result = None
        self.error = None
        jobs[self.id] = self
        th = threading.Thread(target=self._run)
        th.start()
        self._th = th

    def _update_progress(self, p: Progress):
        self.progress = p

    def _run(self):
        try:
            self.result = run_sql(self._sql, run_make_env, shared=self._shared, progress=self._update_progress)
        except (UserError, SqlError) as e:
            self.error = str(e)
        except:
            self.error = traceback.format_exc()
        finally:
            self._event.set()
        threading.Timer(30., self._delete)

    def _delete(self):
        jobs.pop(self.id, None)

    def wait(self, timeout=30):
        self._event.wait(timeout)
        self._delete()
