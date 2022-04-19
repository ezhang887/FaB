from config import NodeConfig, SystemConfig
from collections import defaultdict
import logging

from typing import Dict, Callable, Set, Tuple
from messages import create_signed_message, decode_signature, MessageType

import math

NodeId = int
Signature = bytes

class LeaderElection:
    def __init__(
        self,
        node_config: NodeConfig,
        system_config: SystemConfig,
        multicast: Callable[[bytes], None],
    ):
        self.node_config = node_config
        self.regency = system_config.leader_id
        self.system_config = system_config
        self.suspects: Dict[int, Dict[NodeId, Signature]] = defaultdict(lambda: dict())
        self.multicast = multicast
        self.proof: Dict[NodeId, Signature] = None

    def get_regency(self) -> int:
        return self.regency

    def get_leader(self) -> int:
        return self.get_regency() % self.system_config.P

    def suspect(self, regency):
        msg_to_send = create_signed_message(
            MessageType.SUSPECT, 
            sender_id=self.node_config.node_id, 
            signature=self.node_config.signing_key.sign(str(regency).encode()),
            regency=regency, 
        )
        self.multicast(msg_to_send)

    def on_suspect(self, suspect_message, signer_id):
        assert suspect_message["type"] == MessageType.SUSPECT

        regency = suspect_message["regency"]
        if regency < self.regency: # Ignore timeouts for old leaders
            return

        # Verify signature on message
        vk = self.system_config.all_nodes[signer_id].verifying_key
        signature = decode_signature(suspect_message["signature"])
        if not vk.verify(signature, str(suspect_message["regency"]).encode()):
            logging.warn(f"Signature verification failed for {suspect_message}")
            return

        self.suspects[regency][signer_id] = suspect_message["signature"]
        
        if len(self.suspects[regency]) > math.ceil((self.system_config.P + self.system_config.f) / 2):
            self.proof = self.suspects[regency]
            del self.suspects[regency]
            self.regency = regency + 1
            logging.debug(f"Node {self.node_config.node_id} is updating regency to {self.regency}" +
                f" with proof {self.proof}")

    def consider(self, new_regency, proof):
        if new_regency == self.system_config.leader_id:
            # Initial leader doesn't need proof
            return True

        if len(proof) < 0: # Not enough proposers suspected the leader
            return False

        for (signer_id, signature) in proof.values():
            vk = self.system_config.all_nodes[signer_id].verifying_key
            if not vk.verify(signature, str(new_regency).encode()):
                # Signature doesn't match
                return False
        
        # Since a quorum of proposers suspected the last leader, update regency
        if new_regency > self.regency:
            self.regency = new_regency
            self.proof = proof
        return True

    def is_leader(self):
        return self.get_leader() == self.node_config.node_id
