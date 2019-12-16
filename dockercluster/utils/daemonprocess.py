import os
from multiprocessing import Process, Pipe


class DaemonProcess:
    def __init__(self, target, **kwargs):
        p0, p1 = Pipe(duplex=False)
        def process():
            pid = os.fork()
            if pid != 0:
                p1.send(pid)
                p1.close()
                return
            p1.close()
            target(*kwargs.get('args', ()))
        p = Process(target=process)
        p.start()
        self.pid: int = p0.recv()
        p.join()

    def stop(self):
        os.kill(self.pid)


def test():
    def target(t: int):
        import time
        time.sleep(t)
        print('done')

    dp = DaemonProcess(target=target, args=(3,))
    print(dp.pid)


if __name__ == '__main__':
    test()
