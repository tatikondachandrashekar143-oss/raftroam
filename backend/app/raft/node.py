
from backend.app.raft.state_machine import RaftStateMachine
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
    match_index: dict[str, int] = field(default_factory=dict)
    state_machine: RaftStateMachine = field(default_factory=RaftStateMachine)



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
        prev_log_index: int = -1,
        prev_log_term: int = -1,
        leader_commit: int = -1,
    ) -> bool:
        """Validate and append entries received from a leader."""

        # Reject requests from an older term.
        if leader_term < self.current_term:
            return False

        # A newer-term leader causes this node to step down.
        if leader_term > self.current_term:
            self.current_term = leader_term
            self.voted_for = None

        self.state = NodeState.FOLLOWER

        # Verify that the preceding log entry matches.
        if prev_log_index >= 0:
            if prev_log_index >= len(self.log):
                return False

            if self.log[prev_log_index]["term"] != prev_log_term:
                return False

        # Verify and append incoming entries without duplicating matches.
        insert_index = prev_log_index + 1

        for offset, entry in enumerate(entries):
            index = insert_index + offset

            if index < len(self.log):
                if self.log[index] == entry:
                    continue

                # Never overwrite an already committed entry.
                if index <= self.commit_index:
                    return False

                # Remove the conflicting suffix before appending.
                self.log = self.log[:index]

                # Remove replication tracking beyond the retained log.
                self.match_index = {
                    peer: min(match, len(self.log) - 1)
                    for peer, match in self.match_index.items()
                }

            self.log.append(entry)

        # Apply the leader's known commit progress, bounded by our log.
        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, len(self.log) - 1)

        return True

    
    def record_replication(self, follower_id: str, index: int) -> None:
        """Record the highest log index replicated to a follower."""
        if follower_id not in self.peers:
            raise ValueError("Unknown follower")

        if index < -1 or index >= len(self.log):
            raise ValueError("Invalid replicated log index")

        previous = self.match_index.get(follower_id, -1)
        self.match_index[follower_id] = max(previous, index)

    def advance_commit_index(self) -> int:
        """Advance commit index when a current-term entry has a majority."""
        if self.state != NodeState.LEADER:
            raise RuntimeError("Only the leader can advance commit index")

        for index in range(len(self.log) - 1, self.commit_index, -1):
            if self.log[index]["term"] != self.current_term:
                continue

            replicated = 1
            replicated += sum(
                1
                for peer in self.peers
                if self.match_index.get(peer, -1) >= index
            )

            majority = (len(self.peers) + 1) // 2 + 1
            if replicated >= majority:
                self.commit_index = index
                break

        return self.commit_index
    
    def apply_committed_entries(self) -> list[dict]:
        """Apply committed log entries that have not yet been applied."""
        results = []

        while self.last_applied < self.commit_index:
            next_index = self.last_applied + 1
            entry = self.log[next_index]

            result = self.state_machine.apply(
                next_index,
                entry["command"],
            )

            self.last_applied = next_index
            results.append(result)

        return results
    
    def replicate_to_follower(self, follower: "RaftNode") -> bool:
        """Replicate this leader's log to a follower and record its acknowledgement."""
        if self.state != NodeState.LEADER:
            raise RuntimeError("Only the leader can replicate entries")

        if follower.node_id not in self.peers:
            raise ValueError("Unknown follower")

        success = follower.receive_append_entries(
            leader_term=self.current_term,
            entries=self.log.copy(),
            prev_log_index=-1,
            prev_log_term=-1,
            leader_commit=self.commit_index,
        )

        if success:
            self.record_replication(
                follower.node_id,
                len(follower.log) - 1,
            )

        return success


    
    
        
    



