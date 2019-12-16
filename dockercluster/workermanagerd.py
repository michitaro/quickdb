import argparse
import json
import os
import shutil
import socket
import threading
import traceback
from typing import Optional

import docker
from docker.models.containers import Container as DockerContainer

from .logger import logger
from . import config
from .utils import socketjson
from .utils.chdir import chdir
from .utils.daemonprocess import DaemonProcess
from .utils.socketrelay import socket_relay


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, default=config.workermanagerd.port)
    parser.add_argument('--bind', default='0.0.0.0')
    args = parser.parse_args()

    with socket.socket() as s0:
        s0.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s0.bind((args.bind, args.port))
        s0.listen()
        logger.info(f'listening on {args.bind}:{args.port}...')
        while True:
            s1, _ = s0.accept()
            threading.Thread(target=process_request, args=(s1,)).start()


def process_request(s: socket.socket):
    '''
    Most requests will be done in less than a second.
    '''
    logger.info(f'connection from {s.getpeername()}')
    res = socketjson.recv_json(s, sync=True)
    if res['type'] == 'start_container':
        return start_container(res['cluster_id'])
    if res['type'] == 'stop_container':
        return stop_container(res['cluster_id'])
    if res['type'] == 'proxy':
        return proxy(res['cluster_id'], s)
    raise RuntimeError(f'Unknown request type: {res["type"]}')


def start_container(cluster_id: str):
    logger.info(f'starting container: {cluster_id}...')
    WorkerContainer.start(cluster_id)


def stop_container(cluster_id: str):
    logger.info(f'stopping container: {cluster_id}...')
    wc = WorkerContainer.from_cluster_id(cluster_id)
    wc.cleanup()


def proxy(cluster_id: str, source: socket.socket):
    def process(s0: socket.socket):
        logger.info(f'starting proxy: {cluster_id}...')
        wc = WorkerContainer(cluster_id)
        with chdir(wc.socketdir):
            with socket.socket(socket.AF_UNIX) as s1:
                try:
                    s1.connect(f'./master.sock')
                except:
                    logger.warning(traceback.format_exc())
                    socketjson.send_json(s0, {'status': 'NG', 'error': traceback.format_exc()}, sync=True)
                else:
                    socketjson.send_json(s0, {'status': 'OK'}, sync=True)
                    socket_relay(s0, s1)
                finally:
                    s0.close()
    DaemonProcess(target=process, args=(source, ))


class WorkerContainer:
    def __init__(self, cluster_id: str):
        self.cluster_id = cluster_id
        self._container: Optional[DockerContainer] = None

    def _prepare_dirs(self):
        for dirname in [self.socketdir, self.logdir]:
            os.makedirs(dirname, exist_ok=True)
            os.chmod(dirname, 0o777)

    def _start(self):
        self._prepare_dirs()
        try:
            client = docker.from_env()
            self._container: DockerContainer = client.containers.run(
                config.worker_docker.image,
                command=config.worker_docker.command,
                auto_remove=True,
                detach=True,
                environment=config.worker_docker.environment(config.this_node),
                volumes={**config.worker_docker.volumes(config.this_node), **{
                    self.socketdir: {'bind': '/sockets', 'mode': 'rw'},
                    self.logdir: {'bind': '/log', 'mode': 'rw'},
                }}
            )
        except:
            self.cleanup()
            raise
        self._save_meta()

    def _from_cluster_id(self):
        meta = self._load_meta()
        try:
            self._container = self._get_container(meta['container_id'])
        except:
            self.cleanup()
            raise

    @property
    def basedir(self):
        return f'{config.this_node.workdir}/dockerclusters/{self.cluster_id}'

    @property
    def socketdir(self):
        return f'{self.basedir}/sockets'

    @property
    def logdir(self):
        return f'{self.basedir}/log'

    def _save_meta(self):
        with open(f'{self.basedir}/meta.json', 'w') as f:
            json.dump({
                'cluster_id': self.cluster_id,
                'container_id': self._container.id,
            }, f)

    def _load_meta(self):
        with open(f'{self.basedir}/meta.json', 'r') as f:
            return json.load(f)

    def _get_container(self, container_id: str) -> DockerContainer:
        client = docker.from_env()
        return client.containers.get(container_id)

    @classmethod
    def start(cls, cluster_id: str):
        wc = cls(cluster_id, )
        wc._start()
        return wc

    @classmethod
    def from_cluster_id(cls, cluster_id: str):
        wc = cls(cluster_id)
        wc._from_cluster_id()
        return wc

    def cleanup(self):
        if self._container:
            try:
                self._container.stop(timeout=1)
            except:
                traceback.print_exc()
        shutil.rmtree(self.basedir, ignore_errors=True)


if __name__ == '__main__':
    main()
