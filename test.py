import random
from gevent.queue import Queue  # type: ignore
from ecdsa import SigningKey, VerifyingKey  # type: ignore
import logging

from router import simple_router
from node import Node
from utils.config import NodeConfig, SystemConfig, generate_public_configs

from typing import List, Callable, Optional, Tuple, Set

logging.basicConfig(
    filename="test.log",
    filemode="w",
    format="%(asctime)s %(levelname)s:%(message)s",
    level=logging.DEBUG,
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
    inputs = [Queue(1) for i in range(N)]
    output_queue = Queue()

    nodes: List[Node] = []
    for i in range(N):
        node = Node(
            node_config=node_configs[i],
            system_config=system_config,
            receive_func=recvs[i],
            send_func=sends[i],
            get_input=inputs[i].get,
            output_queue=output_queue,
        )
        node.start()
        nodes.append(node)

    for i in range(N):
        # TODO: give different original proposal values to different nodes
        inputs[i].put(254)

    pnumbers = set()
    nonfaulty_learners = set()
    for n in nodes:
        if faulty_node_ids is not None and n.node_config.node_id in faulty_node_ids:
            continue
        if not n.node_config.is_learner:
            continue
        nonfaulty_learners.add(n.node_config.node_id)

    # Wait for all learners to commit
    committed_learners: Set[int] = set()
    while sorted(committed_learners) != sorted(nonfaulty_learners):
        output = output_queue.get()
        node_id = output["node_id"]

        if node_id not in nonfaulty_learners:
            continue

        value = output["value"]
        print(f"Output from learner {node_id} is {value}")
        assert value[0] == 254
        pnumbers.add(value[1])
        committed_learners.add(node_id)

    for n in nodes:
        n.stop()

    # Make sure all learned values come from same view
    assert len(pnumbers) == 1


def simple_test(
    ommission: bool = False,
    equivocation: bool = False,
    leader_ommission: bool = False,
    leader_equivocation: bool = False,
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
        if leader_ommission or leader_equivocation:
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
            elif equivocation or leader_equivocation:
                # Randomly send random data or send original data
                def new_send(j: int, o: bytes):
                    if rnd.random() < 0.5:
                        orig_send(j, o)
                    else:
                        orig_send(j, generate_random_bytes())

                sends[n] = new_send

    run_system(N, system_config, node_configs, sends, recvs, faulty_nodes)


def simple_test_unique_roles(
    ommission: bool = False,
    equivocation: bool = False,
    leader_ommission: bool = False,
    leader_equivocation: bool = False,
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
        if leader_ommission or leader_equivocation:
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
            elif equivocation or leader_equivocation:
                # Randomly send random data or send original data
                def new_send(j: int, o: bytes):
                    if rnd.random() < 0.5:
                        orig_send(j, o)
                    else:
                        orig_send(j, generate_random_bytes())

                sends[n] = new_send

    run_system(N, system_config, node_configs, sends, recvs, faulty_nodes)


def view_change_test():
    """
    More complicated test case:
    Faulty leader sends same message to 5 out of 6 acceptors (one of them being malicious).
    The 4 non-faulty acceptors forward the same message to all learners.
    The faulty acceptor only forwards the message to one learner.
    This way, one learner will receive 5 ACCEPTED messages and learn the value, but the remaining learners only receive 4 (which is not enough).
    A view change will be triggered, and the new leader should propose the same original value
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

    # Proposers = 0, 1, 2, 3
    # Acceptors = 4, 5, 6, 7, 8, 9
    # Learners = 10, 11, 12, 13
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
    inputs = [Queue(1) for i in range(N)]
    output_queue = Queue()

    faulty_node_ids = set()

    nodes: List[Node] = []
    for i in range(N):
        is_faulty_leader = False
        is_faulty_acceptor = False

        if i == leader:
            # Faulty leader = 0
            is_faulty_leader = True
            faulty_node_ids.add(i)
        elif i == P:
            # Faulty acceptor = 4
            is_faulty_acceptor = True
            faulty_node_ids.add(i)

        node = Node(
            node_config=node_configs[i],
            system_config=system_config,
            receive_func=recvs[i],
            send_func=sends[i],
            get_input=inputs[i].get,
            output_queue=output_queue,
            is_faulty_leader=is_faulty_leader,
            is_faulty_acceptor=is_faulty_acceptor,
            disable_commit_proof=True,
        )
        node.start()
        nodes.append(node)

    for i in range(N):
        if i == leader:
            inputs[i].put(254)
        else:
            inputs[i].put(i)

    nonfaulty_learners = set()
    for n in nodes:
        if faulty_node_ids is not None and n.node_config.node_id in faulty_node_ids:
            continue
        if not n.node_config.is_learner:
            continue
        nonfaulty_learners.add(n.node_config.node_id)

    # Wait for all learners to commit
    committed_learners = set()
    while sorted(committed_learners) != sorted(nonfaulty_learners):
        output = output_queue.get()
        node_id = output["node_id"]

        if node_id not in nonfaulty_learners:
            continue

        value = output["value"]
        print(f"Output from learner {node_id} is {value}")
        assert value[0] == 254

        # Node 10 learned in original view (hardcoded in node.py)
        if node_id == 10:
            assert value[1] == system_config.leader_id
        # Other learners learn after view change
        else:
            assert value[1] == system_config.leader_id + 1

        committed_learners.add(node_id)

    for n in nodes:
        n.stop()


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

# Leader randomly decides to send random stuff
simple_test(leader_equivocation=True)
simple_test_unique_roles(leader_equivocation=True)

# View change test case - make sure safety is held
view_change_test()
