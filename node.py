from collections import defaultdict
from operator import is_
from progress_certificate import CommitProof, ProgressCertificate
from utils.config import NodeConfig, NodeConfigPublic, SystemConfig
from messages import Message, parse_message, MessageType
from leader_election import LeaderElection

from typing import Callable, Dict, Optional
import gevent  # type: ignore
from gevent import Greenlet  # type: ignore
import math
import logging

from utils.types import NodeId


class Node:
    def __init__(
        self,
        node_config: NodeConfig,
        system_config: SystemConfig,
        receive_func: Callable,
        send_func: Callable,
        get_input: Callable,
        timeout: int = 5,  # Timeout in seconds
    ):
        self.system_config = system_config
        self.node_config = node_config

        self.receive_func = receive_func
        self.send_func = send_func
        self.get_input = get_input

        self.timeout = timeout
        self.leader_election = LeaderElection(
            node_config, self.system_config, self.multicast_proposers
        )

    def multicast(
        self,
        message: Message,
        node_filter: Optional[Callable[[NodeConfigPublic], bool]] = None,
    ):
        encoded_message = message.encode()
        for node in self.system_config.all_nodes:
            if node_filter is not None and node_filter(node):
                self.send_func(node.node_id, encoded_message)

    def multicast_acceptors(self, message: Message):
        self.multicast(message, node_filter=lambda n: n.is_acceptor)

    def multicast_learners(self, message: Message):
        self.multicast(message, node_filter=lambda n: n.is_learner)

    def multicast_proposers(self, message: Message):
        self.multicast(message, node_filter=lambda n: n.is_proposer)

    def run(self):
        if self.node_config.is_proposer:
            satisfied_nodes = set()
            learned_nodes_proposer = set()
            progress_cert = ProgressCertificate(self.system_config)
            original_proposal = self.get_input()

        if self.node_config.is_acceptor:
            accepted = None
            tentative_commit_proof = CommitProof(self.system_config)
            commit_proof = CommitProof(self.system_config)

        if self.node_config.is_learner:
            accepted_nodes = dict()
            learned_nodes = dict()
            learned = None
            commit_proof_from_acceptors: Dict[NodeId, CommitProof] = {}

        started_send_pull = False
        started_send_proposals = False
        started_send_queries = False
        started_suspect = False

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
            
            msg_bytes = self.receive_func()
            message = parse_message(msg_bytes)

            if message is not None:
                logging.debug(f"Node {self.node_config.node_id} received {message.__dict__()}")

            # --------------------LEADER---------------------
            # leader.onElected()
            if self.leader_election.is_leader():
                assert self.node_config.is_proposer

                def send_queries():
                    if progress_cert.has_quorum():
                        return
                    elif not self.leader_election.is_leader(): 
                        # Timed out, let new leader take over
                        return

                    msg_to_send = Message(
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
                and progress_cert.has_quorum()
            ):
                assert self.node_config.is_proposer
                value_to_propose = progress_cert.get_value_to_propose(
                    original_proposal, self.leader_election.get_regency()
                )

                def send_proposals():
                    if len(satisfied_nodes) >= math.ceil(
                        (self.system_config.P + self.system_config.f + 1) / 2
                    ):
                        return

                    msg_to_send = Message(
                        MessageType.PROPOSE,
                        sender_id=self.node_config.node_id,
                        value=value_to_propose,
                        pnumber=self.system_config.leader_id,
                        progress_cert=progress_cert.as_list()
                    )
                    self.multicast_acceptors(msg_to_send)

                    # Re-schedule this later
                    gevent.spawn_later(self.timeout * 2, send_proposals)

                if not started_send_proposals:
                    send_proposals()
                    started_send_proposals = True

            # leader receives REPLY
            if (
                message is not None
                and self.leader_election.is_leader()
                and message.type == MessageType.REPLY
            ):
                progress_cert.add_part(message)
            
            # --------------------PROPOSER---------------------
            # proposer.onLearned()
            if (
                message is not None
                and self.node_config.is_proposer
                and message.type == MessageType.LEARNED
            ):
                learned_nodes_proposer.add(message.sender_id)
                if len(learned_nodes_proposer) >= math.ceil(
                    (self.system_config.L + self.system_config.f + 1) / 2
                ):
                    self.multicast_proposers(
                        Message(
                            MessageType.SATISFIED, sender_id=self.node_config.node_id
                        )
                    )

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
                and message.type == MessageType.SATISFIED
            ):
                satisfied_nodes.add(message.sender_id)
            
            # If the proposer receives a SUSPECT message
            if (
                message is not None
                and self.node_config.is_proposer
                and message.type == MessageType.SUSPECT
            ):
                self.leader_election.on_suspect(message)

            # --------------------ACCEPTOR---------------------
            # acceptor.onPropose()
            if (
                message is not None
                and self.node_config.is_acceptor
                and message.type == MessageType.PROPOSE
            ):
                value, pnumber = message.get_field("value"), message.get_field("pnumber")
                leader_progress_cert = ProgressCertificate.decode(
                    self.system_config, message.get_field("progress_cert")
                )
                
                is_pnumber_match = pnumber == self.leader_election.get_regency()
                has_accepted_for_round = (accepted is not None and accepted[1] >= pnumber)
                is_value_vouched_for = (
                    (accepted is not None and accepted[0] == value) 
                    or leader_progress_cert.vouches_for(value, pnumber)
                ) 

                if (
                    leader_progress_cert.has_quorum()
                    and is_pnumber_match 
                    and not has_accepted_for_round 
                    and is_value_vouched_for
                ):
                    accepted = message.get_field("value"), message.get_field("pnumber")
                    msg_to_send = Message(
                        MessageType.ACCEPTED,
                        sender_id=self.node_config.node_id,
                        value=message.get_field("value"),
                        pnumber=message.get_field("pnumber"),
                    )
                    self.multicast_learners(msg_to_send)
                    
                    msg_to_send.sign(self.node_config.signing_key)
                    self.multicast_acceptors(msg_to_send)

            
            # acceptor.onQuery()
            if (
                message is not None
                and self.node_config.is_acceptor
                and message.type == MessageType.QUERY
            ):
                if (
                    self.leader_election.consider(message.get_field("pnumber"), message.get_field("election_proof"))
                    and self.leader_election.get_regency() == message.get_field("pnumber")
                ):  
                    msg_to_send = Message(
                        MessageType.REPLY,
                        sender_id=self.node_config.node_id,
                        accepted_value=accepted,
                        pnumber=message.get_field("pnumber"),
                        commit_proof=commit_proof.as_list()
                    )
                    msg_to_send.sign(self.node_config.signing_key)
                    # TODO: really only need to send to leader
                    self.multicast_proposers(msg_to_send)


            # acceptor.onAccepted()
            if (
                message is not None
                and self.node_config.is_acceptor
                and message.type == MessageType.ACCEPTED
                and message.verify(self.system_config.all_nodes[message.sender_id].verifying_key)
            ):
                tentative_commit_proof.add_part(message)
                
                if tentative_commit_proof.valid(
                    message.get_field("value"), self.leader_election.get_regency()
                ):
                    commit_proof = tentative_commit_proof
                    msg_to_send = Message(
                        MessageType.COMMITPROOF,
                        sender_id=self.node_config.node_id,
                        commit_proof=commit_proof.as_list()
                    )
                    self.multicast_learners(msg_to_send)


            # --------------------LEARNER---------------------
            # learner.onAccepted()
            if (
                message is not None
                and self.node_config.is_learner
                and message.type == MessageType.ACCEPTED
            ):
                accepted_nodes[message.sender_id] = (
                    message.get_field("value"),
                    message.get_field("pnumber"),
                )

                num_accepts = 0
                for v in accepted_nodes.values():
                    if v == (message.get_field("value"), message.get_field("pnumber")):
                        num_accepts += 1

                if num_accepts >= math.ceil(
                    (self.system_config.A + 3 * self.system_config.f + 1) / 2
                ):
                    learned = (message.get_field("value"), message.get_field("pnumber"))
                    msg_to_send = Message(
                        MessageType.LEARNED,
                        sender_id=self.node_config.node_id,
                        value=message.get_field("value"),
                        pnumber=message.get_field("pnumber"),
                    )
                    self.multicast_proposers(msg_to_send)

            # learner.onStart()
            if self.node_config.is_learner:

                def send_pull():
                    if learned is not None:
                        return

                    msg_to_send = Message(
                        MessageType.PULL, sender_id=self.node_config.node_id
                    )
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
                and message.type == MessageType.PULL
            ):
                if learned is not None:
                    value, pnumber = learned
                    msg_to_send = Message(
                        MessageType.LEARNED,
                        sender_id=self.node_config.node_id,
                        value=value,
                        pnumber=pnumber,
                    )
                    self.send_func(message.sender_id, msg_to_send)

            # learner.onCommitProof()
            if (
                message is not None
                and self.node_config.is_learner
                and message.type == MessageType.COMMITPROOF
            ):
                commit_proof_from_acceptors[message.sender_id] = (
                    CommitProof.decode(self.system_config, message.get_field("commit_proof"))
                )
                
                if message.sender_id in accepted_nodes:
                    value, pnumber = accepted_nodes[message.sender_id]
                    
                    num_supporters = 0
                    for commit_proof in commit_proof_from_acceptors.values():
                        if commit_proof.valid(value, pnumber):
                            num_supporters += 1
                    
                    if num_supporters >= math.ceil(
                        (self.system_config.A + self.system_config.f + 1) / 2
                    ):
                        learned = value, pnumber
                        msg_to_send = Message(
                            MessageType.LEARNED,
                            sender_id=self.node_config.node_id,
                            value=value,
                            pnumber=pnumber,
                        )
                        self.multicast_proposers(msg_to_send)


            # learner.onLearned()
            if (
                message is not None
                and self.node_config.is_learner
                and message.type == MessageType.LEARNED
            ):
                learned_nodes[message.sender_id] = (
                    message.get_field("value"),
                    message.get_field("pnumber"),
                )

                num_learns = 0
                for v in learned_nodes.values():
                    if v == (message.get_field("value"), message.get_field("pnumber")):
                        num_learns += 1

                if num_learns >= self.system_config.f + 1:
                    learned = (message.get_field("value"), message.get_field("pnumber"))
                    # print(f"Learner {self.node_config.node_id} has LEARNED {learned}")

    def start(self):
        self.thread = Greenlet(self.run)
        self.thread.start()

    def wait(self):
        self.thread.join()
        return self.thread.value
