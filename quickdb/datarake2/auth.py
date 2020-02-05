import hashlib
import os
import secrets
import socket
import socketserver
from typing import IO

from quickdb.utils.cached_property import cached_property

from . import config


class AuthError(RuntimeError):
    pass


def authenticate(handler: socketserver.StreamRequestHandler):
    '''
    on worker
    '''
    try:
        if handler.connection.family != socket.AF_UNIX: # type: ignore
            if config.master_addr != handler.client_address[0]:
                raise AuthError(f'connection from {handler.client_address} is not allowed')
        nonce = bytes(f'{secrets.randbits(512):0128x}', 'utf-8')
        handler.wfile.write(nonce + '\n'.encode())
        handler.wfile.flush()
        hashed = handler.rfile.readline(1024).strip()
        if hashed != safe_digest(nonce):
            raise AuthError(f'invalid credentials')
    except AuthError as error:
        handler.wfile.write(f'ng: {error}\n'.encode())
        handler.wfile.flush()
        raise
    handler.wfile.write('ok\n'.encode())
    handler.wfile.flush()


def safe_digest(value, salt=None):
    m = hashlib.sha512()
    m.update(value + (salt or keychain.password))
    return bytes(m.hexdigest(), 'utf-8')


def knock(wfile: IO[bytes], rfile: IO[bytes], salt=None):
    '''
    on master
    '''
    nonce = rfile.readline().strip()
    wfile.write(safe_digest(nonce, salt) + '\n'.encode())
    auth_line = rfile.readline().decode()
    if auth_line.startswith('ng:'):
        reason = auth_line.split(':', 1)[1]
        raise AuthError(reason)
    assert auth_line == 'ok\n'


class Keychain:
    @cached_property
    def password(self):
        password_path = f'{os.path.dirname(__file__)}/secrets/password'
        assert os.stat(password_path).st_mode & 0o077 == 0
        with open(password_path) as f:
            password = bytes(f.read().strip(), 'utf-8')
            assert len(password) >= 256
        return password


keychain = Keychain()
