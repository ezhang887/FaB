from config import NodeConfig, SystemConfig
from messages import create_message, parse_message, MessageType

from typing import Callable, Optional
import gevent  # type: ignore
from gevent import Greenlet  # type: ignore
import math


class Node:
    def __init__(
        self,
        node_config: NodeConfig,
        system_config: SystemConfig,
        receive_func: Callable,
        send_func: Callable,
        get_input: Callable,
        timeout: int = 1,  # Units in seconds
    ):
        self.system_config = system_config
        self.node_config = node_config

        self.receive_func = receive_func
        self.send_func = send_func
        self.get_input = get_input

        self.is_leader = system_config.leader_id == node_config.node_id
        self.timeout = timeout

    def multicast(
        self, message: bytes, node_filter: Optional[Callable[[NodeConfig], bool]] = None
    ):
        for node in self.system_config.all_nodes:
            if node_filter is not None and node_filter(node):
                self.send_func(node.node_id, message)

    def multicast_acceptors(self, message: bytes):
        self.multicast(message, node_filter=lambda n: n.is_acceptor)

    def multicast_learners(self, message: bytes):
        self.multicast(message, node_filter=lambda n: n.is_learner)

    def multicast_proposers(self, message: bytes):
        self.multicast(message, node_filter=lambda n: n.is_proposer)

    def run(self):
        if self.node_config.is_proposer:
            satisfied_nodes = set()
            learned_nodes_proposer = set()

        if self.node_config.is_acceptor:
            accepted = None

        if self.node_config.is_learner:
            accepted_nodes = dict()
            learned_nodes = dict()
            learned = None

        started_send_pull = False
        started_send_proposals = False

        if self.is_leader:
            value_to_propose = self.get_input()

        while True:
            done = True
            # Make sure proposal duties and learner duties are completed before returning
            if self.node_config.is_proposer:
                if self.node_config.node_id not in satisfied_nodes:
                    done = False
            if self.node_config.is_learner:
                if learned is None:
                    done = False

            # If this node is only an acceptor, than don't return (TODO: FIX THIS?)
            if (
                self.node_config.is_acceptor
                and not self.node_config.is_proposer
                and not self.node_config.is_learner
            ):
                done = False

            if done:
                if self.node_config.is_learner:
                    return learned
                else:
                    return None

            # leader.onStart()
            if self.is_leader:
                assert self.node_config.is_proposer

                def send_proposals():
                    if len(satisfied_nodes) >= math.ceil(
                        (self.system_config.P + self.system_config.f + 1) / 2
                    ):
                        return

                    msg_to_send = create_message(
                        MessageType.PROPOSE,
                        value=value_to_propose,
                        pnumber=self.system_config.leader_id,
                        progress_cert="",
                    )
                    self.multicast_acceptors(msg_to_send)

                    # Re-schedule this later
                    gevent.spawn_later(self.timeout, send_proposals)

                if not started_send_proposals:
                    send_proposals()
                    started_send_proposals = True

            sender_id, msg_bytes = self.receive_func()
            message = parse_message(msg_bytes)
            assert message is not None

            # --------------------PROPOSER---------------------
            # proposer.onLearned()
            if self.node_config.is_proposer and message["type"] == MessageType.LEARNED:
                learned_nodes_proposer.add(sender_id)
                if len(learned_nodes_proposer) >= math.ceil(
                    (self.system_config.L + self.system_config.f + 1) / 2
                ):
                    self.multicast_proposers(create_message(MessageType.SATISFIED))

            # TODO: proposer.onStart()
            if self.node_config.is_proposer:
                pass

            # proposer.onSatisfied()
            if (
                self.node_config.is_proposer
                and message["type"] == MessageType.SATISFIED
            ):
                satisfied_nodes.add(sender_id)

            # --------------------ACCEPTOR---------------------
            # acceptor.onPropose()
            if self.node_config.is_acceptor and message["type"] == MessageType.PROPOSE:
                accepted = message["value"], message["pnumber"]
                msg_to_send = create_message(
                    MessageType.ACCEPTED,
                    value=message["value"],
                    pnumber=message["pnumber"],
                )
                self.multicast_learners(msg_to_send)

            # --------------------LEARNER---------------------
            # learner.onAccepted()
            if self.node_config.is_learner and message["type"] == MessageType.ACCEPTED:
                accepted_nodes[sender_id] = (message["value"], message["pnumber"])

                num_accepts = 0
                for v in accepted_nodes.values():
                    if v == (message["value"], message["pnumber"]):
                        num_accepts += 1

                if num_accepts >= math.ceil(
                    (self.system_config.A + 3 * self.system_config.f + 1) / 2
                ):
                    learned = (message["value"], message["pnumber"])
                    msg_to_send = create_message(
                        MessageType.LEARNED,
                        value=message["value"],
                        pnumber=message["pnumber"],
                    )
                    self.multicast_proposers(msg_to_send)

            # learner.onStart()
            if self.node_config.is_learner:

                def send_pull():
                    if learned is not None:
                        return

                    msg_to_send = create_message(MessageType.PULL)
                    self.multicast_learners(msg_to_send)

                    # Re-schedule this later
                    gevent.spawn_later(self.timeout, send_pull)

                if not started_send_pull:
                    send_pull()
                    started_send_pull = True

            # learner.onPull()
            if self.node_config.is_learner and message["type"] == MessageType.PULL:
                if learned is not None:
                    value, pnumber = learned
                    msg_to_send = create_message(
                        MessageType.LEARNED, value=value, pnumber=pnumber
                    )
                    self.send_func(sender_id, msg_to_send)

            # learner.onLearned()
            if self.node_config.is_learner and message["type"] == MessageType.LEARNED:
                learned_nodes[sender_id] = (message["value"], message["pnumber"])

                num_learns = 0
                for v in learned_nodes.values():
                    if v == (message["value"], message["pnumber"]):
                        num_learns += 1

                if num_learns >= self.system_config.f + 1:
                    learned = (message["value"], message["pnumber"])
                    # print(f"Learner {self.node_config.node_id} has LEARNED {learned}")

    def start(self):
        self.thread = Greenlet(self.run)
        self.thread.start()

    def wait(self):
        self.thread.join()
        return self.thread.value
