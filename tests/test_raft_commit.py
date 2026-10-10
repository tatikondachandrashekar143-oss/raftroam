
import pytest

from backend.app.raft.node import NodeState, RaftNode


def make_leader():
    node = RaftNode("node-1", ["node-2", "node-3"])
    node.become_candidate()
    node.become_leader()
    node.append_entry({"tool": "search_buses"})
    return node


def test_leader_commits_entry_after_majority_replication():
    leader = make_leader()

    leader.record_replication("node-2", 0)

    assert leader.advance_commit_index() == 0


def test_leader_does_not_commit_without_majority():
    leader = make_leader()

    assert leader.advance_commit_index() == -1


def test_only_leader_can_advance_commit_index():
    follower = RaftNode("node-2", ["node-1", "node-3"])

    with pytest.raises(RuntimeError):
        follower.advance_commit_index()


def test_unknown_follower_is_rejected():
    leader = make_leader()

    with pytest.raises(ValueError):
        leader.record_replication("node-9", 0)

def test_old_term_entry_is_not_committed_directly():
    leader = make_leader()

    # Simulate an entry from an older term.
    leader.log[0]["term"] = leader.current_term - 1
    leader.record_replication("node-2", 0)

    assert leader.advance_commit_index() == -1
