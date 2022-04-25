from collections import defaultdict
import math
import logging

from typing import Dict, Callable, Set, Tuple

from messages import Message, decode_signature, MessageType, parse_message_from_dict
from utils.config import NodeConfig, SystemConfig
from utils.types import RegencyNumber, NodeId


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
        self.suspects: Dict[RegencyNumber, Dict[NodeId, Dict]] = defaultdict(lambda: dict())
        self.multicast = multicast
        self.proof: Dict[NodeId, Dict] = None

    def get_regency(self) -> int:
        return self.regency

    def get_leader(self) -> int:
        return self.get_regency() % self.system_config.P

    def suspect(self, regency):
        msg_to_send = Message(
            MessageType.SUSPECT, 
            sender_id=self.node_config.node_id,
            regency=regency, 
        )
        msg_to_send.sign(self.node_config.signing_key)
        self.multicast(msg_to_send)

    def on_suspect(self, suspect_message):
        assert suspect_message.type == MessageType.SUSPECT

        regency = suspect_message.get_field("regency")
        if regency < self.regency: # Ignore timeouts for old leaders
            return

        # Verify signature on message
        signer_id = suspect_message.sender_id
        vk = self.system_config.all_nodes[signer_id].verifying_key
        if not suspect_message.verify(vk):
            logging.warn(f"Signature verification failed for {suspect_message}")
            return

        self.suspects[regency][signer_id] = suspect_message.__dict__()
        
        if len(self.suspects[regency]) >= math.ceil((self.system_config.P + self.system_config.f + 1) / 2):
            self.proof = list(self.suspects[regency].values())
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

        for encoded_suspect_message in proof:
            try:
                suspect_message = parse_message_from_dict(encoded_suspect_message)
            except Exception as e:
                logging.warn(f"Failed to decode proof: {e}")
                return False

            signer_id = suspect_message.sender_id
            vk = self.system_config.all_nodes[signer_id].verifying_key
            if not suspect_message.verify(vk):
                # Signature doesn't match
                logging.warn(f"Signature verification failed for {suspect_message}")
                return False
        
        # Since a quorum of proposers suspected the last leader, update regency
        if new_regency > self.regency:
            self.regency = new_regency
            self.proof = proof
        return True

    def is_leader(self):
        return self.get_leader() == self.node_config.node_id
