
from backend.app.raft.node import NodeState, RaftNode


def run_election(candidate: RaftNode, nodes: list[RaftNode]) -> bool:
    """Elect a leader only when an available candidate wins a majority."""

    if not candidate.available:
        return False

    candidate.become_candidate()

    votes = 1
    majority = (len(nodes) // 2) + 1

    for node in nodes:
        if node.node_id == candidate.node_id:
            continue

        if not node.available:
            continue

        granted = node.request_vote(
            candidate.node_id,
            candidate.current_term,
        )

        if granted:
            votes += 1

    if votes >= majority:
        candidate.become_leader()
        return True

    return False
