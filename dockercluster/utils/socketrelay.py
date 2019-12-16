import socket
from select import select


def socket_relay(s0: socket.socket, s1: socket.socket):
    '''
    Relays s0 and s1 bidirectionally.
    It is the caller's responsibility to close s0 and s1.
    '''

    BUFSIZE = 8192

    class DirectedRelay:
        def __init__(self, src, dst):
            self.src = src
            self.dst = dst
            self.buf = b''
            self.src_closed = False
            self.dst_shutdown = False

    r01 = DirectedRelay(s0, s1)
    r10 = DirectedRelay(s1, s0)
    src2r = {
        s0: r01,
        s1: r10,
    }
    dst2r = {
        s0: r10,
        s1: r01,
    }
    while not (r01.dst_shutdown and r10.dst_shutdown):
        rlist0 = \
            ([] if r01.src_closed else [s0]) + \
            ([] if r10.src_closed else [s1])
        wlist0 = \
            ([s1] if len(r01.buf) > 0 else []) + \
            ([s0] if len(r10.buf) > 0 else [])
        rlist1, wlist1, xlist1 = select(rlist0, wlist0, [s0, s1])
        assert len(xlist1) == 0
        for s in wlist1:
            r = dst2r[s]
            bytes_sent = r.dst.send(r.buf)
            r.buf = r.buf[bytes_sent:]
        for s in rlist1:
            r = src2r[s]
            buf = r.src.recv(BUFSIZE)
            r.buf += buf
            if len(buf) == 0:
                r.src_closed = True
        for r in [r01, r10]:
            if r.src_closed and not r.dst_shutdown and len(r.buf) == 0:
                r.dst.shutdown(socket.SHUT_WR)
                r.dst_shutdown = True


def test():
    '''
    Connection to args.bind:args.port will be forwareded to args.remote_host:args.remote_port
    '''

    import argparse
    from ..logger import logger

    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, default=8889)
    parser.add_argument('--bind', default='127.0.0.1')
    parser.add_argument('remote_host')
    parser.add_argument('remote_port', type=int)
    args = parser.parse_args()

    serversocket = socket.socket()
    serversocket.bind((args.bind, args.port))
    serversocket.listen()

    while True:
        logger.info(f'waiting for connection on {args.bind}:{args.port}')
        clientsocket, _ = serversocket.accept()
        with socket.socket() as s1:
            s1.connect((args.remote_host, args.remote_port))
            socket_relay(clientsocket, s1)


if __name__ == '__main__':
    test()
