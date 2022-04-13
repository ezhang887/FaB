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


REQUIRED_FIELDS_FOR_TYPE = {
    MessageType.PROPOSE: ["value", "pnumber", "progress_cert"],
    MessageType.ACCEPTED: ["value", "pnumber"],
    MessageType.LEARNED: ["value", "pnumber"],
    MessageType.SUSPECT: ["regency"],
}


def create_message(msg_type: MessageType, **kwargs) -> bytes:
    content = {}
    for t, required_fields in REQUIRED_FIELDS_FOR_TYPE.items():
        if msg_type == t:
            for field in required_fields:
                if field not in required_fields:
                    raise ValueError(
                        f"Cannot find required parameter '{field}' in 'required_fields' when creating message of type '{msg_type.value}'"
                    )
                if field not in kwargs:
                    raise ValueError(
                        f"Cannot find required parameter '{field}' in 'kwargs' when creating message of type '{msg_type.value}'"
                    )
                content[field] = kwargs[field]
            break
    content["type"] = msg_type.value
    return json.dumps(content).encode()


def parse_message(byte_data: Optional[bytes]) -> Optional[Dict]:
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

    if "type" not in dict_data:
        logging.warn(f"Message does not contain 'type' field: {str_data}")
        return None

    try:
        msg_type = MessageType(dict_data["type"])
    except ValueError:
        logging.warn(f"Message has invalid type field: {str_data}")
        return None
    dict_data["type"] = msg_type

    for t, required_fields in REQUIRED_FIELDS_FOR_TYPE.items():
        if msg_type == t:
            for field in required_fields:
                if field not in dict_data:
                    logging.warn(
                        f"Message has type '{msg_type.value}' but does not have field '{field}': {str_data}"
                    )
                    return None
            break

    return dict_data
