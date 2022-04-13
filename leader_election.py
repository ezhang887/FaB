from config import SystemConfig
from collections import defaultdict

from typing import Dict, Callable, List, Any
from messages import create_message, MessageType

import math


class LeaderElection:
    def __init__(
        self,
        node_id: int,
        system_config: SystemConfig,
        multicast: Callable[[bytes], None],
    ):
        self.node_id = node_id
        self.regency = system_config.leader_id
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

        new_leader = False

        for r, count in self.suspects.items():
            if count > math.ceil((self.system_config.P + self.system_config.f) / 2):
                new_leader = True
                break

        if new_leader:
            # TODO: probably need more here..
            self.proofs.clear()
            self.suspects.clear()
            self.regency += 1

    def consider(self, proof):
        self.proofs.append(proof)

    def is_leader(self):
        return self.get_leader() == self.node_id
