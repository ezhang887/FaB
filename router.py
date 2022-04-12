import random

import gevent  # type: ignore
from gevent.queue import Queue  # type: ignore


def simple_router(N, maxdelay=0.01, seed=None):
    """Builds a set of connected channels, with random delay
    @return (receives, sends)
    """
    rnd = random.Random(seed)
    queues = [Queue() for _ in range(N)]

    def makeSend(i):
        def _send(j, o):
            delay = rnd.random() * maxdelay
            gevent.spawn_later(delay, queues[j].put, (i, o))

        return _send

    def makeRecv(j):
        def _recv():
            (i, o) = queues[j].get()
            return (i, o)

        return _recv

    return ([makeSend(i) for i in range(N)], [makeRecv(j) for j in range(N)])
