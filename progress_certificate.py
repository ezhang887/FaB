import logging
import math
from typing import Dict, Tuple

from utils.config import SystemConfig
from utils.types import NodeId, RegencyNumber   
from messages import Message, MessageType

class CommitProof:
    def __init__(self, system_config: SystemConfig):
        self.accepted_values: Dict[NodeId, Tuple[int, RegencyNumber]] = {}
        self.system_config = system_config
    
    def add_part(self, accepted_message: Message):
        assert accepted_message.type == MessageType.ACCEPTED

        vk = self.system_config.all_nodes[accepted_message.sender_id].verifying_key
        if not accepted_message.verify(vk):
            logging.warn(f"Signature verification failed for {accepted_message}")
            return

        prev_accepted_value = self.accepted_values.get(accepted_message.sender_id, (-1, -1))
        
        if accepted_message.get_field("pnumber") > prev_accepted_value[1]: 
            accepted = (accepted_message.get_field("value"), accepted_message.get_field("pnumber"))
            self.accepted_values[accepted_message.sender_id] = accepted

    def valid(self, value: int, pnumber: RegencyNumber) -> bool:
        vote_count = sum(
            1 for node_id in self.accepted_values 
            if self.accepted_values[node_id] == (value, pnumber)
        )
        quorum_needed = math.ceil(
            (self.system_config.A + self.system_config.f + 1) / 2
        )

        return vote_count >= quorum_needed

class ProgressCertificate:
    def __init__(self):
        pass

    def get_quorum_locked_value(self) -> int:
        return None
    
    def vouches_for(self, value: int, pnumber: RegencyNumber) -> bool:
        return False
