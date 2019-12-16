import argparse
import traceback
import os
import pickle
import socket

from . import master


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--socket', default='/sockets/webif.sock')
    args = parser.parse_args()

    with socket.socket(socket.AF_UNIX) as s0:
        s0.bind(args.socket)
        os.chmod(args.socket, 0o777)
        s0.listen()
        while True:
            s1, _ = s0.accept()
            try:
                rfile = s1.makefile('rb')
                wfile = s1.makefile('wb', buffering=0)
                try:
                    req = pickle.load(rfile)
                    try:
                        res = master.run(req['make_env'], req.get('context', {}))
                    except:
                        pickle.dump({'error': traceback.format_exc()}, wfile)
                    else:
                        pickle.dump(res, wfile)
                finally:
                    wfile.close()
                    rfile.close()
            except:
                traceback.print_exc()
            finally:
                s1.close()


if __name__ == '__main__':
    main()
