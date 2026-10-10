
import pytest

from backend.app.raft.node import NodeState, RaftNode


def test_leader_can_append_entry():
    node = RaftNode("node-1", ["node-2", "node-3"])
    node.become_candidate()
    node.become_leader()

    index = node.append_entry({"tool": "search_buses"})

    assert index == 0
    assert len(node.log) == 1
    assert node.log[0]["term"] == 1
    assert node.log[0]["command"]["tool"] == "search_buses"


def test_follower_cannot_append_entry():
    node = RaftNode("node-2", ["node-1", "node-3"])

    with pytest.raises(RuntimeError):
        node.append_entry({"tool": "search_buses"})


def test_follower_accepts_entries_from_newer_term():
    node = RaftNode("node-2", ["node-1", "node-3"])
    entries = [{"term": 1, "command": {"tool": "search_buses"}}]

    accepted = node.receive_append_entries(1, entries)

    assert accepted is True
    assert node.current_term == 1
    assert node.state == NodeState.FOLLOWER
    assert node.log == entries


def test_follower_rejects_entries_from_old_term():
    node = RaftNode("node-2", ["node-1", "node-3"])
    node.current_term = 2

    accepted = node.receive_append_entries(
        1,
        [{"term": 1, "command": {"tool": "search_buses"}}],
    )

    assert accepted is False
    assert node.log == []
