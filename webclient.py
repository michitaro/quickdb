import json
import pickle
import sys
from pprint import pprint
from urllib.request import Request, urlopen

base_url = 'http://localhost:32001'


class DockerCluster:
    def __init__(self):
        with urlopen(Request(f'{base_url}/clusters', method='POST')) as f:
            res = json.load(f)
        self.id = res['id']

    def stop(self):
        with urlopen(Request(f'{base_url}/clusters/{self.id}', method='DELETE')) as f:
            res = json.load(f)
        assert 'error' not in res

    def query(self, make_env: str, *, context={}, time=False):
        res = self._query({
            'make_env': make_env,
            'context': context,
        })
        if 'error' in res:
            raise RuntimeError(res['error'])
        if time:
            pprint(time, stream=sys.stderr)
        return res['result']

    def _query(self, req):
        with urlopen(Request(f'{base_url}/clusters/{self.id}/query', data=pickle.dumps(req), method='POST')) as f:
            return pickle.load(f)
