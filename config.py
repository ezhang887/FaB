import dataclasses
import ecdsa  # type: ignore

from typing import List


@dataclasses.dataclass
class NodeConfigPublic:
    node_id: int  # Id of the node
    is_acceptor: bool  # Is this node an acceptor?
    is_proposer: bool  # Is this node a proposer?
    is_learner: bool  # Is this node a learner?
    verifying_key: ecdsa.keys.VerifyingKey


@dataclasses.dataclass
class NodeConfig(NodeConfigPublic):
    # Keep private key out of the Public Config
    signing_key: ecdsa.keys.SigningKey


@dataclasses.dataclass
class SystemConfig:
    leader_id: int  # Leader of the system
    P: int  # Number of proposers
    A: int  # Number of acceptors
    L: int  # Number of learners
    f: int  # Number of faulty nodes
    all_nodes: List[NodeConfigPublic]  # List of all nodes in the system


def generate_public_configs(node_configs: List[NodeConfig]) -> List[NodeConfigPublic]:
    rv = []
    for n in node_configs:
        v = NodeConfigPublic(
            node_id=n.node_id,
            is_acceptor=n.is_acceptor,
            is_proposer=n.is_proposer,
            is_learner=n.is_learner,
            verifying_key=n.verifying_key,
        )
        rv.append(v)
    return rv
