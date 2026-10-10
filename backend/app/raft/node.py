
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class NodeState(str, Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass
class RaftNode:
    node_id: str
    peers: list[str]

    state: NodeState = NodeState.FOLLOWER
    current_term: int = 0
    voted_for: Optional[str] = None
    commit_index: int = -1
    last_applied: int = -1
    log: list[dict] = field(default_factory=list)

    def become_follower(self, term: int) -> None:
        if term < self.current_term:
            raise ValueError("Cannot move to an older Raft term")

        self.current_term = term
        self.state = NodeState.FOLLOWER
        self.voted_for = None

    def become_candidate(self) -> None:
        self.current_term += 1
        self.state = NodeState.CANDIDATE
        self.voted_for = self.node_id

    def become_leader(self) -> None:
        if self.state != NodeState.CANDIDATE:
            raise RuntimeError("Only a candidate can become leader")

        self.state = NodeState.LEADER
    
    def request_vote(self, candidate_id: str, candidate_term: int) -> bool:
        """Grant a vote only if the candidate's term is current or newer
        and this node has not voted for another candidate in that term.
        """
        if candidate_term < self.current_term:
            return False

        if candidate_term > self.current_term:
            self.become_follower(candidate_term)

        if self.voted_for is None or self.voted_for == candidate_id:
            self.voted_for = candidate_id
            self.state = NodeState.FOLLOWER
            return True

        return False
    
    def receive_heartbeat(self, leader_id: str, leader_term: int) -> bool:
        """Accept a valid heartbeat from the current or a newer term."""
        if leader_term < self.current_term:
            return False

        if leader_term > self.current_term:
            self.current_term = leader_term
            self.voted_for = None

        self.state = NodeState.FOLLOWER
        return True



    
    def append_entry(self, command: dict) -> int:
        """Append a command to this node's log if it is the leader."""
        if self.state != NodeState.LEADER:
            raise RuntimeError("Only the leader can append new entries")

        entry = {
            "term": self.current_term,
            "command": command,
        }
        self.log.append(entry)
        return len(self.log) - 1

    def receive_append_entries(
        self,
        leader_term: int,
        entries: list[dict],
    ) -> bool:
        """Accept entries from a leader with a valid term."""
        if leader_term < self.current_term:
            return False

        if leader_term > self.current_term:
            self.current_term = leader_term
            self.voted_for = None

        self.state = NodeState.FOLLOWER
        self.log.extend(entries)
        return True


