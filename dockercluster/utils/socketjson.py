'''
send/recv json in sockets
'''
import re
import socket
import json

BUFSIZE = 8192


def _safe_recv(s: socket.socket, bufsize: int):
    buf = s.recv(bufsize)
    if len(buf) == 0:
        raise RuntimeError('Unexpected EOT')
    return buf


def _recv_sized_chunk(s: socket.socket, *, sync=False) -> bytes:
    '''
    get bytes in the format bellow from socket s
    format: len(payload) + ":" + payload
    '''
    buf = b''
    while True:
        buf += _safe_recv(s, BUFSIZE)
        m = re.match(rb'(\d+):', buf[:32])
        if m:
            payload_len = int(m.group(1))
            buf = buf[len(m.group(0)):]
            r = payload_len
            payload = buf[:r]
            rest = buf[r:]
            while len(payload) < payload_len:
                buf = _safe_recv(s, BUFSIZE)
                r = payload_len - len(payload)
                payload += buf[:r]
                rest = buf[r:]
            assert len(rest) == 0
            break
        if len(buf) > 32:
            raise RuntimeError(f'invalid header: {buf}')
    if sync:
        s.send(b'\x00')
    return payload


def _send_sized_chunk(s: socket.socket, paylod: bytes, *, sync=False):
    s.sendall(f'{len(paylod)}:'.encode() + paylod)
    if sync:
        _safe_recv(s, 1)


def recv_json(s: socket.socket, *, sync=False):
    payload = _recv_sized_chunk(s, sync=sync)
    return json.loads(payload)


def send_json(s: socket.socket, msg, *, sync=False):
    payload = json.dumps(msg).encode()
    _send_sized_chunk(s, payload, sync=sync)
