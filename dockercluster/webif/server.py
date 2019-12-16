import argparse
import contextlib
import os
import socket

from flask import Flask, Response, request

from ..dockercluster import DockerCluster

app = Flask(__name__)


@app.route('/clusters', methods=['POST'])
def create_cluster():
    dc = DockerCluster.start()
    return {'id': dc.id}


@app.route('/clusters/<id>', methods=['DELETE'])
def delete_cluster(id):
    dc = DockerCluster.from_id(id)
    dc.stop()
    return {}


@app.route('/clusters/<id>/query', methods=['POST'])
def query(id):
    dc = DockerCluster.from_id(id)
    with socket.socket(socket.AF_UNIX) as s:
        with chdir(dc.socketdir):
            s.connect('webif.sock')
            rfile = s.makefile('rb')
            wfile = s.makefile('wb', buffering=False)
            try:
                wfile.write(request.get_data())
                return Response(rfile.read(), mimetype='application/octet-stream')
            finally:
                wfile.close()
                rfile.close()


@contextlib.contextmanager
def chdir(dirname):
    cwd = os.getcwd()
    try:
        os.chdir(dirname)
        yield os.getcwd()
    finally:
        os.chdir(cwd)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, default=32001)
    parser.add_argument('--bind', default='127.0.0.1')
    parser.add_argument('--threaded', action='store_true', default=False)
    parser.add_argument('--debug', '-d', action='store_true', default=False)
    args = parser.parse_args()
    app.run(host=args.bind, port=args.port, threaded=args.threaded, debug=args.debug)


if __name__ == '__main__':
    main()
