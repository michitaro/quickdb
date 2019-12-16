import json
import os
import secrets
import shutil
import signal
import socket
import traceback
from typing import List, Optional

import docker
from docker.models.containers import Container as DockerContainer

from . import config
from .utils import socketjson
from .utils.chdir import chdir
from .utils.daemonprocess import DaemonProcess
from .utils.socketrelay import socket_relay


class DockerCluster:
    '''
    Represents a docker cluster,
    that consists 1 master container, several worker containers
    and a directory including some socket files and metadata for itself.
    '''

    def __init__(self):
        self._container: Optional[DockerContainer] = None
        self._worker_proxy_pids: List[int] = []

    def _start(self):
        '''
        Starts a new docker cluster.
        '''
        self.id = self.uid()
        try:
            self._start_master_container()
            self._start_worker_containers()
        except:
            self._cleanup()
            raise
        self._save_meta()

    def _from_id(self, id: str):
        '''
        Make a DockerCluster instance from an ID of existing (running) docker cluster.
        '''
        self.id = id
        meta = self._load_meta()
        self._worker_proxy_pids = meta['worker_proxy_pids']
        try:
            self._container = self._get_container_by_id(meta['container_id'])
        except:
            self._cleanup()
            raise

    def _prepare_dirs(self):
        for dirname in [self.socketdir, self.logdir]:
            os.makedirs(dirname, exist_ok=True)
            os.chmod(dirname, 0o777)

    def _start_master_container(self) -> DockerContainer:
        self._prepare_dirs()
        client = docker.from_env()
        container: DockerContainer = client.containers.run(
            config.master_docker.image,
            command=config.master_docker.command,
            auto_remove=True,
            detach=True,
            environment=config.master_docker.environment,
            volumes={**config.master_docker.volumes, **{
                self.socketdir: {'bind': '/sockets', 'mode': 'rw'},
                self.logdir: {'bind': '/log', 'mode': 'rw'},
            }},
        )
        self._container = container

    def _start_worker_containers(self):
        # TODO: execute in parallel by concurrent.futures
        for wn in config.worker_nodes:
            with socket.socket() as s:
                s.connect((wn.host, wn.port))
                socketjson.send_json(s, {'type': 'start_container', 'cluster_id': self.id}, sync=True)
            proxy_pid = self._socket_file_proxy(wn)
            self._worker_proxy_pids.append(proxy_pid)

    def _get_container_by_id(self, container_id: str) -> DockerContainer:
        client = docker.from_env()
        return client.containers.get(container_id)

    @property
    def basedir(self):
        return f'{config.master_node.workdir}/dockerclusters/{self.id}'

    @property
    def socketdir(self):
        return f'{self.basedir}/sockts'

    @property
    def logdir(self):
        return f'{self.basedir}/log'

    def _load_meta(self):
        metafile = f'{self.basedir}/meta.json'
        with open(metafile, 'r') as f:
            return json.load(f)

    def _save_meta(self):
        metafile = f'{self.basedir}/meta.json'
        with open(metafile, 'w') as f:
            return json.dump({
                'id': self.id,
                'container_id': self._container.id,
                'worker_proxy_pids': self._worker_proxy_pids,
            }, f)

    @classmethod
    def start(cls):
        dc = cls()
        dc._start()
        return dc

    @classmethod
    def from_id(cls, id: str):
        dc = cls()
        dc._from_id(id)
        return dc

    def stop(self):
        self._cleanup()

    def _cleanup(self):
        try:
            if self._container:
                # TODO: execute in another thread? This takes a while.
                self._container.stop(timeout=1)
        except:
            traceback.print_exc()
        # TODO: execute in parallel
        for wn in config.worker_nodes:
            try:
                with socket.socket() as s:
                    s.connect((wn.host, wn.port))
                    socketjson.send_json(s, {'type': 'stop_container', 'cluster_id': self.id}, sync=True)
            except:
                traceback.print_exc()
        for pid in self._worker_proxy_pids:
            try:
                os.kill(pid, signal.SIGTERM)
            except:
                traceback.print_exc()
        shutil.rmtree(self.basedir, ignore_errors=True)

    @staticmethod
    def uid():
        while True:
            uid = secrets.token_hex()
            if os.path.exists(f'{config.master_node.workdir}/{uid}'):
                continue
            return uid

    def _socket_file_proxy(self, wn: config.WorkerNodeConfig):
        def process():
            with chdir(self.socketdir):
                with socket.socket(socket.AF_UNIX) as s:
                    socketfile = f'./worker-{wn.host}.sock'
                    s.bind(socketfile)
                    os.chmod(socketfile, 0o777)
                    s.listen()
                    while True:
                        s1, _ = s.accept()
                        try:
                            with socket.socket() as s2:
                                s2.connect((wn.host, wn.port))
                                socketjson.send_json(s2, {'type': 'proxy', 'cluster_id': self.id}, sync=True)
                                res = socketjson.recv_json(s2, sync=True)
                                if res['status'] == 'NG':
                                    raise RuntimeError(f'Remote Error@{wn.host}: {res["error"]}')
                                socket_relay(s1, s2)
                        except:
                            traceback.print_exc()
                        finally:
                            s1.close()
        return DaemonProcess(target=process).pid
