import random
from gevent.queue import Queue  # type: ignore
from ecdsa import SigningKey, VerifyingKey  # type: ignore
import logging

from router import simple_router
from node import Node
from config import NodeConfig, SystemConfig, generate_public_configs

from typing import List, Callable, Optional, Tuple

logging.basicConfig(
    filename='test.log', 
    filemode='w',
    format='%(asctime)s %(levelname)s:%(message)s', 
    level=logging.DEBUG
)

def generate_random_bytes(size=16) -> bytes:
    return bytes(random.getrandbits(8) for _ in range(size))


def generate_keys() -> Tuple[SigningKey, VerifyingKey]:
    sk = SigningKey.generate()
    vk = sk.verifying_key
    return sk, vk


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


def simple_test(
    ommission: bool = False, equivocation: bool = False, leader_ommission: bool = False
):
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
        sk, vk = generate_keys()
        node_config = NodeConfig(
            node_id=i,
            is_acceptor=False,
            is_proposer=False,
            is_learner=False,
            verifying_key=vk,
            signing_key=sk,
        )
        node_configs.append(node_config)

    for i in range(P):
        node_configs[i].is_proposer = True
    for i in range(A):
        node_configs[i].is_acceptor = True
    for i in range(N - 1, N - L - 1, -1):
        node_configs[i].is_learner = True

    system_config = SystemConfig(
        leader_id=leader,
        P=P,
        A=A,
        L=L,
        f=f,
        all_nodes=generate_public_configs(node_configs),
    )
    sends, recvs = simple_router(N)
    faulty_nodes = None

    if ommission or equivocation or leader_ommission:
        if leader_ommission:
            faulty_nodes = [leader]
        else:
            nodes_to_sample = list(range(N))
            nodes_to_sample.remove(leader)
            faulty_nodes = rnd.sample(nodes_to_sample, f)

        for n in faulty_nodes:
            print(f"Faulty node: {node_configs[n]}")
            orig_send = sends[n]
            if ommission or leader_ommission:
                # Randomly send or omit
                def new_send(j: int, o: bytes):
                    if rnd.random() < 0.5:
                        orig_send(j, o)

                sends[n] = new_send
            elif equivocation:
                # Randomly send random data or send original data
                def new_send(j: int, o: bytes):
                    if rnd.random() < 0.5:
                        orig_send(j, o)
                    else:
                        orig_send(j, generate_random_bytes())

                sends[n] = new_send

    run_system(N, system_config, node_configs, sends, recvs, faulty_nodes)


def simple_test_unique_roles(
    ommission: bool = False, equivocation: bool = False, leader_ommission: bool = False
):
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
        sk, vk = generate_keys()
        node_config = NodeConfig(
            node_id=i,
            is_acceptor=False,
            is_proposer=False,
            is_learner=False,
            verifying_key=vk,
            signing_key=sk,
        )
        node_configs.append(node_config)

    for i in range(P):
        node_configs[i].is_proposer = True
    for i in range(A):
        node_configs[i + P].is_acceptor = True
    for i in range(L):
        node_configs[i + P + A].is_learner = True

    system_config = SystemConfig(
        leader_id=leader,
        P=P,
        A=A,
        L=L,
        f=f,
        all_nodes=generate_public_configs(node_configs),
    )
    sends, recvs = simple_router(N)
    faulty_nodes = None

    if ommission or equivocation or leader_ommission:
        if leader_ommission:
            faulty_nodes = [leader]
        else:
            nodes_to_sample = list(range(N))
            nodes_to_sample.remove(leader)
            faulty_nodes = rnd.sample(nodes_to_sample, f)

        for n in faulty_nodes:
            print(f"Faulty node: {node_configs[n]}")
            orig_send = sends[n]
            if ommission or leader_ommission:
                # Randomly send or omit
                def new_send(j: int, o: bytes):
                    if rnd.random() < 0.5:
                        orig_send(j, o)

                sends[n] = new_send
            elif equivocation:
                # Randomly send random data or send original data
                def new_send(j: int, o: bytes):
                    if rnd.random() < 0.5:
                        orig_send(j, o)
                    else:
                        orig_send(j, generate_random_bytes())

                sends[n] = new_send

    run_system(N, system_config, node_configs, sends, recvs, faulty_nodes)


simple_test()
simple_test_unique_roles()

# Ommission: `f` non-leader nodes randomly don't send messages
simple_test(ommission=True)
simple_test_unique_roles(ommission=True)

# Equivocation: `f` non-leader nodes randomly send random bytes
simple_test(equivocation=True)
simple_test_unique_roles(equivocation=True)

# Leader randomly decides to not send messages
simple_test(leader_ommission=True)
simple_test_unique_roles(leader_ommission=True)
