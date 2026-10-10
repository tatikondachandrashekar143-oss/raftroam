
from backend.app.raft.node import NodeState, RaftNode


def test_follower_accepts_heartbeat_from_newer_term():
    node = RaftNode("node-2", ["node-1", "node-3"])

    accepted = node.receive_heartbeat("node-1", 1)

    assert accepted is True
    assert node.current_term == 1
    assert node.state == NodeState.FOLLOWER


def test_follower_rejects_old_term_heartbeat():
    node = RaftNode("node-2", ["node-1", "node-3"])
    node.current_term = 3

    accepted = node.receive_heartbeat("node-1", 2)

    assert accepted is False
    assert node.current_term == 3


def test_candidate_steps_down_when_receiving_newer_term_heartbeat():
    node = RaftNode("node-2", ["node-1", "node-3"])
    node.become_candidate()

    accepted = node.receive_heartbeat("node-1", 2)

    assert accepted is True
    assert node.state == NodeState.FOLLOWER
    assert node.current_term == 2
