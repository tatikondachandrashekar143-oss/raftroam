
from backend.app.raft.node import NodeState, RaftNode


def run_election(candidate: RaftNode, nodes: list[RaftNode]) -> bool:
    """Ask cluster peers for votes and elect a leader if a majority agrees."""
    candidate.become_candidate()

    votes = 1  # The candidate votes for itself.
    majority = (len(nodes) // 2) + 1

    for node in nodes:
        if node.node_id == candidate.node_id:
            continue

        granted = node.request_vote(candidate.node_id, candidate.current_term)
        if granted:
            votes += 1

    if votes >= majority:
        candidate.become_leader()
        return True

    return False
