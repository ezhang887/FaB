import random
from gevent.queue import Queue  # type: ignore

from router import simple_router
from node import Node
from config import NodeConfig, SystemConfig

from typing import List

f = 1
P = 3 * f + 1
A = 5 * f + 1
L = 3 * f + 1
N = max(P, A, L)

rnd = random.Random(None)
leader = rnd.randint(0, P - 1)
print("LEADER", leader)

node_configs: List[NodeConfig] = []
for i in range(N):
    node_config = NodeConfig(node_id=i)
    node_configs.append(node_config)

for i in range(P):
    node_configs[i].is_proposer = True
for i in range(A):
    node_configs[i].is_acceptor = True
for i in range(L):
    node_configs[i].is_learner = True

system_config = SystemConfig(
    leader_id=leader, P=P, A=A, L=L, f=f, all_nodes=node_configs
)

sends, recvs = simple_router(N)
leader_input = Queue(1)

nodes: List[Node] = []
for i in range(N):
    get_input = leader_input.get if i == leader else None
    node = Node(
        node_config=node_configs[i],
        system_config=system_config,
        receive_func=recvs[i],
        send_func=sends[i],
        get_input=get_input,
    )
    node.start()
    nodes.append(node)

leader_input.put(254)

for n in nodes:
    # TODO: currently only learners exit, proposers and acceptors don't. Change this?
    if n.node_config.is_learner:
        n.wait()
