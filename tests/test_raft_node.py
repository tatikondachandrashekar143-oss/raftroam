
from backend.app.raft.node import NodeState, RaftNode


def test_new_node_starts_as_follower():
    node = RaftNode("node-1", ["node-2", "node-3"])

    assert node.state == NodeState.FOLLOWER
    assert node.current_term == 0
    assert node.voted_for is None


def test_candidate_increments_term_and_votes_for_itself():
    node = RaftNode("node-1", ["node-2", "node-3"])

    node.become_candidate()

    assert node.state == NodeState.CANDIDATE
    assert node.current_term == 1
    assert node.voted_for == "node-1"


def test_candidate_can_become_leader():
    node = RaftNode("node-1", ["node-2", "node-3"])

    node.become_candidate()
    node.become_leader()

    assert node.state == NodeState.LEADER


def test_old_term_is_rejected():
    node = RaftNode("node-1", ["node-2", "node-3"])
    node.current_term = 2

    try:
        node.become_follower(1)
        assert False, "Expected ValueError"
    except ValueError:
        pass

def test_vote_granted_for_new_term():
    node = RaftNode("node-2", ["node-1", "node-3"])

    assert node.request_vote("node-1", 1) is True
    assert node.current_term == 1
    assert node.voted_for == "node-1"


def test_node_rejects_second_candidate_in_same_term():
    node = RaftNode("node-2", ["node-1", "node-3"])

    assert node.request_vote("node-1", 1) is True
    assert node.request_vote("node-3", 1) is False


def test_node_rejects_old_term():
    node = RaftNode("node-2", ["node-1", "node-3"])
    node.current_term = 3

    assert node.request_vote("node-1", 2) is False
    assert node.current_term == 3
