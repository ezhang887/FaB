import random
from gevent.queue import Queue  # type: ignore

from router import simple_router
from node import Node
from config import NodeConfig, SystemConfig

from typing import List, Callable, Optional


def run_system(
    N: int,
    system_config: SystemConfig,
    node_configs: List[NodeConfig],
    sends: List[Callable[[int], Callable[[int, int], None]]],
    recvs: List[Callable[[int], Callable[[int, int], None]]],
    faulty_node_ids: Optional[List[int]] = None,
):
    leader_input = Queue(1)

    nodes: List[Node] = []
    for i in range(N):
        get_input = leader_input.get if i == system_config.leader_id else None
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
        if faulty_node_ids is not None and n.node_config.node_id in faulty_node_ids:
            continue

        # TODO: Currently acceptors don't return. Fix this?
        if n.node_config.is_learner:
            res = n.wait()
            assert res[0] == 254
            assert res[1] == system_config.leader_id
            print(f"Output from learner {n.node_config.node_id} is {res}")
        elif n.node_config.is_proposer:
            n.wait()


def simple_test(ommission: bool = False):
    f = 1
    P = 3 * f + 1
    A = 5 * f + 1
    L = 3 * f + 1
    N = A

    rnd = random.Random(None)
    leader = rnd.randint(0, P - 1)
    print("Leader is", leader)

    node_configs: List[NodeConfig] = []
    for i in range(N):
        node_config = NodeConfig(node_id=i)
        node_configs.append(node_config)

    for i in range(P):
        node_configs[i].is_proposer = True
    for i in range(A):
        node_configs[i].is_acceptor = True
    for i in range(N - 1, N - L - 1, -1):
        node_configs[i].is_learner = True

    system_config = SystemConfig(
        leader_id=leader, P=P, A=A, L=L, f=f, all_nodes=node_configs
    )
    sends, recvs = simple_router(N)
    faulty_nodes = None

    if ommission:
        nodes_to_sample = list(range(N))
        nodes_to_sample.remove(leader)
        faulty_nodes = rnd.sample(nodes_to_sample, f)

        for n in faulty_nodes:
            print(f"Faulty node: {node_configs[n]}")
            sends[n] = lambda *args: None

    run_system(N, system_config, node_configs, sends, recvs, faulty_nodes)


def simple_test_unique_roles(ommission: bool = False):
    """
    Same as simple_test, but each node has a unique role
    (e.g. a node is either a proposer or acceptor or learner)
    """
    f = 1
    P = 3 * f + 1
    A = 5 * f + 1
    L = 3 * f + 1
    N = P + A + L

    rnd = random.Random(None)
    leader = rnd.randint(0, P - 1)
    print("Leader is", leader)

    node_configs: List[NodeConfig] = []
    for i in range(N):
        node_config = NodeConfig(node_id=i)
        node_configs.append(node_config)

    for i in range(P):
        node_configs[i].is_proposer = True
    for i in range(A):
        node_configs[i + P].is_acceptor = True
    for i in range(L):
        node_configs[i + P + A].is_learner = True

    system_config = SystemConfig(
        leader_id=leader, P=P, A=A, L=L, f=f, all_nodes=node_configs
    )
    sends, recvs = simple_router(N)
    faulty_nodes = None

    if ommission:
        nodes_to_sample = list(range(N))
        nodes_to_sample.remove(leader)
        faulty_nodes = rnd.sample(nodes_to_sample, f)

        for n in faulty_nodes:
            print(f"Faulty node: {node_configs[n]}")
            sends[n] = lambda *args: None

    run_system(N, system_config, node_configs, sends, recvs, faulty_nodes)


simple_test()
simple_test_unique_roles()

simple_test(ommission=True)
simple_test_unique_roles(ommission=True)
