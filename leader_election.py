from config import SystemConfig
from collections import defaultdict

from typing import Dict, Callable, List, Any
from messages import create_message, MessageType

import math


class LeaderElection:
    def __init__(
        self,
        initial_leader: int,
        system_config: SystemConfig,
        multicast: Callable[[bytes], None],
    ):
        self.regency = initial_leader
        self.system_config = system_config
        self.suspects: Dict[int, int] = defaultdict(lambda: 0)
        self.multicast = multicast
        # TODO: what is type of proof?
        self.proofs: List[Any] = []

    def get_regency(self) -> int:
        return self.regency

    def get_leader(self) -> int:
        return self.get_regency() % self.system_config.P

    def suspect(self, regency):
        msg_to_send = create_message(MessageType.SUSPECT, regency=regency)
        self.multicast(msg_to_send)

    def on_suspect(self, suspect_message):
        assert suspect_message["type"] == MessageType.SUSPECT

        regency = suspect_message["regency"]
        self.suspects[regency] += 1

        for r, count in self.suspects.items():
            if count > math.ceil((self.system_config.P + self.system_config.f) / 2):
                self.regency += 1
                # TODO: probably need more here..
                self.proofs.clear()
                self.suspects.clear()

    def consider(self, proof):
        self.proofs.append(proof)
