import logging
import math
from typing import Dict, Tuple

from utils.config import SystemConfig
from utils.types import NodeId, RegencyNumber   
from messages import Message, MessageType

class CommitProof:
    def __init__(self, system_config: SystemConfig):
        self.accepted_values: Dict[NodeId, Message] = {}
        self.system_config = system_config
    
    def add_part(self, accepted_message: Message):
        assert accepted_message.type == MessageType.ACCEPTED

        vk = self.system_config.all_nodes[accepted_message.sender_id].verifying_key
        if not accepted_message.verify(vk):
            logging.warn(f"Signature verification failed for {accepted_message}")
            return

        if (
            accepted_message.sender_id not in self.accepted_values
            or (self.accepted_values[accepted_message.sender_id].get_field("pnumber")
                < accepted_message.get_field("pnumber"))
        ):
            self.accepted_values[accepted_message.sender_id] = accepted_message

    def valid(self, value: int, pnumber: RegencyNumber) -> bool:
        vote_count = 0

        for accepted_message in self.accepted_values.values():
            accepted = (accepted_message.get_field("value"), accepted_message.get_field("pnumber"))
            if (value, pnumber) == accepted:
                vote_count += 1
        
        quorum_needed = math.ceil(
            (self.system_config.A + self.system_config.f + 1) / 2
        )

        return vote_count >= quorum_needed

    def __dict__(self) -> Dict:
        return {id: message.__dict__() for id, message in self.accepted_values.items()}
class ProgressCertificate:
    def __init__(self):
        pass

    def get_quorum_locked_value(self) -> int:
        return None
    
    def vouches_for(self, value: int, pnumber: RegencyNumber) -> bool:
        return False
