import dataclasses

from typing import List


@dataclasses.dataclass
class NodeConfig:
    node_id: int  # Id of the node
    is_acceptor: bool = False  # Is this node an acceptor?
    is_proposer: bool = False  # Is this node a proposer?
    is_learner: bool = False  # Is this node a learner?


@dataclasses.dataclass
class SystemConfig:
    leader_id: int  # Leader of the system
    P: int  # Number of proposers
    A: int  # Number of acceptors
    L: int  # Number of learners
    f: int  # Number of faulty nodes
    all_nodes: List[NodeConfig]  # List of all nodes in the system
