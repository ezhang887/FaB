from collections import defaultdict
from config import NodeConfig, NodeConfigPublic, SystemConfig
from messages import create_message, parse_message, MessageType
from leader_election import LeaderElection

from typing import Callable, Dict, Optional
import gevent  # type: ignore
from gevent import Greenlet  # type: ignore
from gevent.queue import Queue # type: ignore
import math
import logging


class Node:
    def __init__(
        self,
        node_config: NodeConfig,
        system_config: SystemConfig,
        receive_func: Callable,
        send_func: Callable,
        get_input: Callable,
        output_queue: Queue,
        timeout: int = 5,  # Timeout in seconds
    ):
        self.system_config = system_config
        self.node_config = node_config

        self.receive_func = receive_func
        self.send_func = send_func
        self.get_input = get_input
        self.output_queue = output_queue

        self.timeout = timeout
        self.leader_election = LeaderElection(
            node_config, self.system_config, self.multicast_proposers
        )

    def multicast(
        self,
        message: bytes,
        node_filter: Optional[Callable[[NodeConfigPublic], bool]] = None,
    ):
        for node in self.system_config.all_nodes:
            if node_filter is not None and node_filter(node):
                self.send_func(node.node_id, message)

    def commit(self, value):
        output = {
            "node_id": self.node_config.node_id,
            "value": value,
        }
        self.output_queue.put(output)

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
            replies_from_acceptors = defaultdict(lambda: dict())

        if self.node_config.is_acceptor:
            accepted = None

        if self.node_config.is_learner:
            accepted_nodes = dict()
            learned_nodes = dict()
            learned = None

        started_send_pull = False
        started_send_proposals = False
        started_send_queries = False
        started_suspect = False

        if self.leader_election.is_leader():
            value_to_propose = self.get_input()
        else:
            # TODO: fix this - don't hardcode, when new leader is elected use progress certificate to determine this
            value_to_propose = 254

        while True:
            # Make sure proposal/leader/learner duties are completed before returning
            done = True
            if self.node_config.is_proposer or self.leader_election.is_leader():
                if len(satisfied_nodes) < math.ceil(
                    (self.system_config.P + self.system_config.f + 1) / 2
                ):
                    done = False
            if self.node_config.is_proposer:
                if self.node_config.node_id not in satisfied_nodes:
                    done = False
            if self.node_config.is_learner:
                if learned is None:
                    done = False

            # If this node is only an acceptor, return when all other nodes have returned
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
            
            # --------------------LEADER---------------------
            # leader.onElected()
            if self.leader_election.is_leader():
                assert self.node_config.is_proposer

                def send_queries():
                    if len(replies_from_acceptors[self.leader_election.get_regency()]) >= (
                        self.system_config.A - self.system_config.f
                    ):
                        return
                    elif not self.leader_election.is_leader(): 
                        # Timed out, let new leader take over
                        return

                    msg_to_send = create_message(
                        MessageType.QUERY,
                        sender_id=self.node_config.node_id,
                        pnumber=self.system_config.leader_id,
                        election_proof=self.leader_election.proof
                    )
                    self.multicast_acceptors(msg_to_send)

                    # Re-schedule this later
                    gevent.spawn_later(self.timeout * 2, send_queries)

                if not started_send_queries:
                    send_queries()
                    started_send_queries = True

            # leader.onStart()
            if (
                self.leader_election.is_leader() 
                and len(replies_from_acceptors[self.leader_election.get_regency()]) >= (
                    self.system_config.A - self.system_config.f
                )
            ):
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
                    gevent.spawn_later(self.timeout * 2, send_proposals)

                if not started_send_proposals:
                    send_proposals()
                    started_send_proposals = True

            sender_id, msg_bytes = self.receive_func()
            message = parse_message(msg_bytes)

            # leader receives REPLY
            if (
                message is not None
                and self.leader_election.is_leader()
                and message["type"] == MessageType.REPLY
            ):
                # TODO: verify signature on reply
                replies_from_acceptors[self.leader_election.get_regency()][sender_id] = (
                    message["accepted_value"]
                )
            
            # --------------------PROPOSER---------------------
            # proposer.onLearned()
            if (
                message is not None
                and self.node_config.is_proposer
                and message["type"] == MessageType.LEARNED
            ):
                learned_nodes_proposer.add(sender_id)
                if len(learned_nodes_proposer) >= math.ceil(
                    (self.system_config.L + self.system_config.f + 1) / 2
                ):
                    self.multicast_proposers(create_message(MessageType.SATISFIED))

            # proposer.onStart()
            if self.node_config.is_proposer:

                def suspect_leader():
                    if len(learned_nodes_proposer) < math.ceil(
                        (self.system_config.L + self.system_config.f + 1) / 2
                    ):
                        self.leader_election.suspect(self.leader_election.get_regency())

                    gevent.spawn_later(self.timeout, suspect_leader)

                if not started_suspect:
                    gevent.spawn_later(self.timeout, suspect_leader)
                    started_suspect = True

            # proposer.onSatisfied()
            if (
                message is not None
                and self.node_config.is_proposer
                and message["type"] == MessageType.SATISFIED
            ):
                satisfied_nodes.add(sender_id)

            # If the proposer receives a SUSPECT message
            if (
                message is not None
                and self.node_config.is_proposer
                and message["type"] == MessageType.SUSPECT
            ):
                self.leader_election.on_suspect(message, sender_id)

            # --------------------ACCEPTOR---------------------
            # acceptor.onPropose()
            if (
                message is not None
                and self.node_config.is_acceptor
                and message["type"] == MessageType.PROPOSE
            ):
                accepted = message["value"], message["pnumber"]
                msg_to_send = create_message(
                    MessageType.ACCEPTED,
                    value=message["value"],
                    pnumber=message["pnumber"],
                )
                self.multicast_learners(msg_to_send)

            
            # acceptor.onQuery()
            if (
                message is not None
                and self.node_config.is_acceptor
                and message["type"] == MessageType.QUERY
            ):
                if (
                    self.leader_election.consider(message["pnumber"], message["election_proof"])
                    and self.leader_election.get_regency() == message["pnumber"]
                ):  
                    logging.debug(f"Node {self.node_config.node_id} received query {message}")
                    # TODO: this needs to be signed
                    msg_to_send = create_message(
                        MessageType.REPLY,
                        sender_id=self.node_config.node_id,
                        accepted_value=accepted,
                        pnumber=message["pnumber"],
                        commit_proof=""
                    )
                    # TODO: really only need to send to leader
                    self.multicast_proposers(msg_to_send)


            # --------------------LEARNER---------------------
            # learner.onAccepted()
            if (
                message is not None
                and self.node_config.is_learner
                and message["type"] == MessageType.ACCEPTED
            ):
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
                    self.commit(learned)

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
                    gevent.spawn_later(self.timeout, send_pull)
                    started_send_pull = True

            # learner.onPull()
            if (
                message is not None
                and self.node_config.is_learner
                and message["type"] == MessageType.PULL
            ):
                if learned is not None:
                    value, pnumber = learned
                    msg_to_send = create_message(
                        MessageType.LEARNED, value=value, pnumber=pnumber
                    )
                    self.send_func(sender_id, msg_to_send)

            # learner.onLearned()
            if (
                message is not None
                and self.node_config.is_learner
                and message["type"] == MessageType.LEARNED
            ):
                learned_nodes[sender_id] = (message["value"], message["pnumber"])

                num_learns = 0
                for v in learned_nodes.values():
                    if v == (message["value"], message["pnumber"]):
                        num_learns += 1

                if num_learns >= self.system_config.f + 1:
                    learned = (message["value"], message["pnumber"])
                    self.commit(learned)
                    # print(f"Learner {self.node_config.node_id} has LEARNED {learned}")

    def start(self):
        self.thread = Greenlet(self.run)
        self.thread.start()

    def stop(self):
        self.thread.kill()
