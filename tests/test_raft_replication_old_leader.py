
from backend.app.raft.node import RaftNode, NodeState


def test_follower_rejects_replication_from_old_leader_term():
    leader = RaftNode("node-1", ["node-2", "node-3"])
    follower = RaftNode("node-2", ["node-1", "node-3"])

    leader.become_candidate()
    leader.become_leader()

    # The follower has already observed a newer term.
    follower.current_term = leader.current_term + 1

    success = leader.replicate_to_follower(follower)

    assert success is False
    assert follower.current_term > leader.current_term
    assert follower.state == NodeState.FOLLOWER
