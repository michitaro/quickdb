import os
import subprocess


class workermanagerd:
    port = 32000


class WorkerNodeConfig:
    def __init__(
        self, *,
        hostname: str, host: str, workdir: str,
        ssh_user: str, codedir: str,
        python_path: str,
        pipenv_path: str,
        datadir: str,
        port=workermanagerd.port
    ):
        self.host = host
        self.port = port
        self.workdir = workdir
        self.ssh_user = ssh_user
        self.codedir = codedir
        self.python_path = python_path
        self.pipenv_path = pipenv_path
        self.datadir = datadir
        self.hostname = hostname  # result of hostname command. will be used to determin config.this_node


worker_nodes = [WorkerNodeConfig(
    hostname=f'hdr-db{i:02d}',
    host=f'hdr-db{i:02d}',
    workdir='/db1/koike/hsc/quickdb/work',
    ssh_user='hsc',
    codedir='/db1/koike/hsc/code',
    python_path='/home/hsc/anaconda3/bin/python',
    pipenv_path='/home/hsc/anaconda3/bin/pipenv',
    datadir='/db1/koike/repo',
# ) for i in range(5, 6)]
) for i in range(5, 13)]


class master_node:
    workdir = '/tmp/quickdb'


class master_docker:
    image = 'anaconda3'
    # command = ['sh', '-c', '''tail -f /dev/null''']
    command = ['sh', '-c', '''cd /shared/quickdb0 && /opt/conda/bin/python -m datarake.masterd 2>&1 > /log/masterd''']
    
    volumes = {
        f'{os.path.dirname(__file__)}/../shared': {'bind': '/shared', 'mode': 'ro'},
    }
    environment = {
    }


class worker_docker:
    image = 'anaconda3'
    # command = ['sh', '-c', '''tail -f /dev/null''']
    command = ['sh', '-c', '''cd /shared/quickdb0 && /opt/conda/bin/python -m datarake.workerd 2>&1 > /log/workerd''']

    @staticmethod
    def volumes(wn: WorkerNodeConfig):
        return {
            f'{wn.codedir}/shared': {'bind': '/shared', 'mode': 'ro'},
            f'{wn.datadir}': {'bind': '/data', 'mode': 'ro'},
        }

    @staticmethod
    def environment(wn: WorkerNodeConfig):
        return {
        }


hostname = subprocess.check_output(['hostname']).decode().strip()
this_node: WorkerNodeConfig = {wn.hostname: wn for wn in worker_nodes}.get(hostname)
