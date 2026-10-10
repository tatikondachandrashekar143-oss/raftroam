
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

