import logging
import math
import sys
from typing import List, Dict, Tuple

from utils.config import SystemConfig
from utils.types import NodeId, RegencyNumber   
from messages import Message, MessageType, parse_message, parse_message_from_dict

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

    def as_list(self) -> List:
        return [message.__dict__() for message in self.accepted_values.values()]

    @classmethod
    def decode(cls, system_config: SystemConfig, accepted_message_dicts: List[Dict]):
        commit_proof = CommitProof(system_config)

        try:
            for message_dict in accepted_message_dicts:
                message = parse_message_from_dict(message_dict)
                if message is not None:
                    commit_proof.add_part(message)
        except Exception as e:
            logging.warn(f"Failed to decode commit proof: {e}")
            return CommitProof(system_config)
        
        return commit_proof


class ProgressCertificate:
    def __init__(self, system_config: SystemConfig):
        self.replies: Dict[NodeId, Message] = {}
        self.system_config = system_config
    
    def add_part(self, reply_message: Message):
        assert reply_message.type == MessageType.REPLY

        vk = self.system_config.all_nodes[reply_message.sender_id].verifying_key
        if not reply_message.verify(vk):
            logging.warn(f"Signature verification failed for {reply_message}")
            return

        self.replies[reply_message.sender_id] = reply_message

    def has_quorum(self) -> bool:
        return len(self.replies) >= self.system_config.A - self.system_config.f

    def vouches_for(self, value: int, pnumber: RegencyNumber) -> bool:
        counts: Dict[int, int] = {}
        for reply in self.replies.values():
            accepted_value = reply.get_field("accepted_value")
            if accepted_value is not None:
                counts[accepted_value] = counts.get(accepted_value, 0) + 1
        
        for reply_value, count in counts.items():
            if (
                reply_value != value 
                and count >= math.ceil((self.system_config.A - self.system_config.f + 1) / 2)
            ):
                return False

        for reply in self.replies.values():
            commit_proof = CommitProof.decode(self.system_config, reply.get_field("commit_proof"))
            accepted_value = reply.get_field("accepted_value")

            if value != accepted_value and commit_proof.valid(accepted_value, pnumber):
                return False

        return True

    def get_value_to_propose(self, original_proposal: int, pnumber: int) -> int:
        # print([reply.get_field("accepted_value") for reply in self.replies.values()])
        accepted_values = set(
            reply.get_field("accepted_value") for reply in self.replies.values()
        )
        
        for accepted_value in accepted_values:
            if accepted_value is not None and self.vouches_for(accepted_value, pnumber):
                return accepted_value
        
        return original_proposal    

    def as_list(self) -> List[Dict]:
        return [message.__dict__() for message in self.replies.values()]

    @classmethod
    def decode(cls, system_config: SystemConfig, reply_dicts: List[Dict]):
        progress_cert = ProgressCertificate(system_config)

        try:
            for reply_dict in reply_dicts:
                message = parse_message_from_dict(reply_dict)
                if message is not None:
                    progress_cert.add_part(message)
        except Exception as e:
            logging.warn(f"Failed to decode progress certificate: {e}")
            return ProgressCertificate(system_config)
        
        return progress_cert

