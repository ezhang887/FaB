from itertools import count
import random

import gevent  # type: ignore
from gevent.queue import Queue  # type: ignore

from typing import Optional

total_bytes_sent = 0

def simple_router(
    N, recv_timeout: int = 5, maxdelay: float = 0.01, seed: Optional[int] = None
):
    """Builds a set of connected channels, with random delay
    @return (receives, sends)
    """
    rnd = random.Random(seed)
    queues = [Queue() for _ in range(N)]

    def makeSend(i: int):
        def _send(j: int, o: bytes):
            global total_bytes_sent
            total_bytes_sent += len(o)
            
            delay = rnd.random() * maxdelay
            gevent.spawn_later(delay, queues[j].put, o)

        return _send

    def makeRecv(j: int):
        def _recv():
            try:
                o = queues[j].get(timeout=recv_timeout)
                return o
            except gevent.queue.Empty:
                # Timeout!
                return None

        return _recv

    return ([makeSend(i) for i in range(N)], [makeRecv(j) for j in range(N)])


def get_bytes_sent():
    return total_bytes_sent

def reset_bytes_sent():
    global total_bytes_sent
    total_bytes_sent = 0