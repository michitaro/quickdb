# QuickDB

## Overview

QuickDB is an implementation of distributed database of map-reduce architecture in Python.
With this system, users can give mapper and reducer in Python code directly to the system to process data.
It also has an interface of SQL.

This system consists of one master node and multiple worker nodes.
Data to be processed is distributed and stored in worker nodes.
When a processing request occurs, the master node scatter the request to each worker node.
Each worker node process the request simultaneously, and the result is collected to the master node.

## Installation

Both master process and worker process work in user mode instead of root mode.
It is recommended to enable ssh connection from master node to worker node without password for managing worker process. From here we assume that we go through the instruction with user `quickdb`.

### Master node

The following instructions shoud be done on the master node.

1. Install Python 3
    * We recommend using [Anaconda](https://anaconda.org).
    * Python will be installed in `/home/quickdb/anaconda3` when using Anaconda.

1. Check out `quickdb`
    ```bash
    $ mkdir -p ~/quickdb/python_path
    $ cd ~/quickdb/python_path
    $ git clone git@github.com:michitaro/quickdb.git .
    ```

1. Generate password
    * Worker node receives processing request from master node and performs processing.
    Generate a password to authenticate the master node.
    ```bash
    $ mkdir -p quickdb/datarake/secrets
    $ openssl rand -hex 128 > quickdb/datarake/secrets/password
    $ chmod 600 quickdb/datarake/secrets/password
    ```

1. Make configurations for worker nodes.
    * System configuration is kept in `datarake/config.py`.
    * Change `config.master_addr` and `config.workers[*]` according to your environment.
        * Quickdb's code itself and data to process will be saved in `config.workers[*].work_dir`.
        ```bash
        $ cp datarake/config.py.sample datarake/config.py
        $ vi datarake/config.py
        ```
    * `config.py` will be transferred to the worker node later.

### Worker node

The following instructions shoud be done on each worker node.

1. Install Python 3
    * We recommend using [Anaconda](https://anaconda.org).
    * Python will be installed in `/home/quickdb/anaconda3` when using Anaconda.

1. Make sure that the work directory on the worker node is empty. The contents of the work direcotry might will overwritten by quickdb.

1. Make sure that port 2935 can be accessed from the outside.
    (`outside` means computers other than this node)

### Connection test with worker
The following instructions shoud be done on the master node.

1. Transfer password file and configuration file.
    ```bash
    $ cd ~/quickdb/python_path
    $ python -m quickdb.datarake.batch --update-code
    ```

1. Confirm that the password file is transferred to the worker node.
    ```bash
    $ python -m quickdb.datarake.batch -- ls -l python_path/datarake/secrets
    ```

1. Start a worker process
    ```bash
    $ python -m quickdb.datarake.workerctrl start
    # wait a few seconds
    $ python -m quickdb.datarake.workerctrl status
    # You will see outputs something like this
    {'quickdb-worker-1.example.com': '1140'}
    {'quickdb-worker-2.example.com': '3429'}
    {'quickdb-worker-3.example.com': '1330'}
    {'quickdb-worker-4.example.com': '8113'}
    ```
    If something goes wrong, you get the following result:
    ```bash
    {'quickdb-worker-1.example.com': ''}
    {'quickdb-worker-2.example.com': ''}
    {'quickdb-worker-3.example.com': ''}
    {'quickdb-worker-4.example.com': ''}
    ```
    You can see the error log on each worker node as this.
    ```bash
    $ tail ~/quickdb/python_path/log
    ```

1. Stop the worker process
    * Before distributing the data, stop the worker processes.
    ```bash
    $ python -m quickdb.datarake.workerctrl stop
    # wait a few seconds
    $ python -m quickdb.datarake.workerctrl status
    # You will see outputs something like this
    {'quickdb-worker-1.example.com': ''}
    {'quickdb-worker-2.example.com': ''}
    {'quickdb-worker-3.example.com': ''}
    {'quickdb-worker-4.example.com': ''}
    ```

### Distribute the data

Data to be processed can be distributed to worker nodes by the following command.
```bash
$ python -m quickdb.sspcatalog.deploy $SOMEWHERE/releases/pdr2_udeep
$ python -m quickdb.sspcatalog.deploy $SOMEWHERE/releases/pdr2_wide
```

### Test Distributed Processing
The following instructions shoud be done on the master node.

1. Start worker processes
    ```bash
    $ python -m quickdb.datarake.workerctrl start
    # wait a few seconds
    $ python -m quickdb.datarake.workerctrl status
    # You will see outputs something like this
    {'quickdb-worker-1.example.com': '1140'}
    {'quickdb-worker-2.example.com': '3429'}
    {'quickdb-worker-3.example.com': '1330'}
    {'quickdb-worker-4.example.com': '8113'}
    ```

1. Test scattering `mapper` and `reducer` to workers
    * This is equivalent to `SELECT COUNT(*) FROM $DEFAULT_RERUN`.
    ```bash
    $ python -m quickdb.datarake.master
    32818438
    ```

### Start SQL Server for End Users
The following instructions should be done on the master node.
1. Install dependent packages.

    These packages are necessary to deal with SQL and need to be installed on only master node.
    ```bash
    $ pip install flask pglast
    ```
    * You can also use `pipenv` to manage dependent packages. See `Pipfile`.

1. Start server for SQL interface
    ```bash
    $ FLASK_APP=quickdb.sqlhttp.sqlserver flask run --port 8002 --host 0.0.0.0
    ```
    It is possible to use other application servers for WSGI such as gunicorn or uWSGI. See [here](https://flask.palletsprojects.com/en/1.1.x/deploying/wsgi-standalone/).

1. Confirm that port 8002 can be accessed from the outside.

### Test SQL interface for end users
The following instructions should be done on your laptop.

1. Check out `quickdb`
    ```bash
    git clone git@github.com:michitaro/quickdb.git
    cd quickdb
    ```

1. Setting of connection destination
    ```bash
    cp quickdb/sqlhttp/{config.py.sample,config.py}
    vi quickdb/sqlhttp/config.py # change server address for master node
    ```

1. Launch Jupyter Notebook
    ```bash
    jupyter lab examples/sqlclient.ipynb
    ```

### Upgrade Codes

To upgrade codes, run the following command on the master node.
```bash
$ cd ~/quickdb/python_path
$ git fetch origin
$ git reset --hard
$ python -m quickdb.datarake.batch --update-code
```


### Unit tests
Unit tests are also available. These tests can be run on a single node. The node must have data repository of `pdr2_dud` to run the tests. Set `REPO_DIR` in `quickdb/test_config.py` to point to the directory including `pdr2_dud`.

```bash
pipenv install
make test # or make coverage
```

### ~~Building Sphinx Docs~~
```
make docs
open sphix/_build/html/index.html
```

* [Google style comments](https://sphinxcontrib-napoleon.readthedocs.io/en/latest/) are supported.
* Compiled docs are available [here](https://michitaro.github.io/quickdb/).
* Push your local sphinx-docs to gh-pages:
    ```bash
    make gh-pages
    ```