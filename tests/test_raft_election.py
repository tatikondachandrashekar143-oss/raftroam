
from backend.app.raft.node import NodeState, RaftNode
from backend.app.raft.election import run_election


def make_cluster():
    return [
        RaftNode("node-1", ["node-2", "node-3"]),
        RaftNode("node-2", ["node-1", "node-3"]),
        RaftNode("node-3", ["node-1", "node-2"]),
    ]


def test_candidate_wins_with_majority():
    nodes = make_cluster()

    assert run_election(nodes[0], nodes) is True
    assert nodes[0].state == NodeState.LEADER
    assert nodes[0].current_term == 1


def test_only_one_node_is_leader_after_basic_election():
    nodes = make_cluster()

    assert run_election(nodes[0], nodes) is True
    leaders = [node for node in nodes if node.state == NodeState.LEADER]
    assert len(leaders) == 1



def test_candidate_cannot_win_without_majority():
    nodes = make_cluster()

    # These nodes have already voted for node-3 in term 1.
    nodes[1].current_term = 1
    nodes[1].voted_for = "node-3"

    nodes[2].current_term = 1
    nodes[2].voted_for = "node-3"

    assert run_election(nodes[0], nodes) is False
    assert nodes[0].state == NodeState.CANDIDATE

