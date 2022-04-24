import base64
import json
import logging
from enum import Enum

from typing import Optional, Dict


class MessageType(Enum):
    PROPOSE = "PROPOSE"
    SATISFIED = "SATISFIED"
    ACCEPTED = "ACCEPTED"
    LEARNED = "LEARNED"
    PULL = "PULL"
    SUSPECT = "SUSPECT"
    QUERY = "QUERY"
    REPLY = "REPLY"
    COMMITPROOF = "COMMITPROOF"


REQUIRED_FIELDS_FOR_TYPE = {
    MessageType.PROPOSE: ["value", "pnumber", "progress_cert"],
    MessageType.ACCEPTED: ["value", "pnumber"],
    MessageType.LEARNED: ["value", "pnumber"],
    MessageType.SUSPECT: ["regency"],
    MessageType.QUERY: ["pnumber", "election_proof"],
    MessageType.REPLY: ["accepted_value", "pnumber", "commit_proof"],
    MessageType.COMMITPROOF: ["commit_proof"]
}


class Message:
    def __init__(self, msg_type: MessageType, sender_id: int, **kwargs):
        self.type = msg_type
        self.sender_id = sender_id
        
        self.content = {}
        for field in REQUIRED_FIELDS_FOR_TYPE.get(msg_type, []):
            if field not in kwargs:
                raise ValueError(
                    f"Cannot find required parameter '{field}' in 'kwargs' when creating message of type '{msg_type.value}'"
                )
            self.content[field] = kwargs[field]
    
    def get_field(self, field):
        return self.content[field]

    def sign(self, signing_key):
        self.signature = signing_key.sign(json.dumps(self.content).encode())
    
    def verify(self, verifying_key):
        if not hasattr(self, "signature"):
            return False

        return verifying_key.verify(self.signature, json.dumps(self.content).encode())

    def __dict__(self) -> Dict:
        msg_as_dict = {
            "sender_id": self.sender_id,
            "type": self.type.value,
            "content": self.content
        }
        
        if hasattr(self, "signature"):
            encoded_signature = base64.b64encode(self.signature).decode('utf-8')
            msg_as_dict["signature"] = encoded_signature
        
        return msg_as_dict

    def __str__(self) -> str:
        return str(self.__dict__())

    def encode(self) -> bytes:
        return json.dumps(self.__dict__()).encode()


def decode_signature(signature: str) -> bytes:
    return base64.b64decode(signature.encode('utf-8'))

def parse_message(byte_data: Optional[bytes]) -> Optional[Message]:
    if byte_data is None:
        return None

    try:
        str_data = byte_data.decode("utf-8")
    except Exception:
        # logging.warn("Failed to decode bytes into string")
        return None
    
    try:
        dict_data = json.loads(str_data)
    except ValueError:
        logging.warn(f"Failed to decode json: {str_data}")
        return None

    return parse_message_from_dict(dict_data)

def parse_message_from_dict(dict_data: Dict) -> Optional[Message]:
    if "type" not in dict_data:
        logging.warn(f"Message does not contain 'type' field: {str_data}")
        return None

    try:
        msg_type = MessageType(dict_data["type"])
    except ValueError:
        logging.warn(f"Message has invalid type field: {str_data}")
        return None

    for field in REQUIRED_FIELDS_FOR_TYPE.get(msg_type, []):
        if field not in dict_data["content"]:
            logging.warn(
                f"Message has type '{msg_type.value}' but does not have field '{field}': {str_data}"
            )
            return None

    msg = Message(msg_type, dict_data["sender_id"], **dict_data["content"])
    if "signature" in dict_data:
        msg.signature = decode_signature(dict_data["signature"])

    return msg