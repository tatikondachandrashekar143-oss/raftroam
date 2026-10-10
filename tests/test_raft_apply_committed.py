
from backend.app.raft.node import NodeState, RaftNode


def make_leader():
    node = RaftNode(node_id="node-1", peers=["node-2", "node-3"])
    node.state = NodeState.LEADER
    node.current_term = 1
    return node


def test_uncommitted_entry_is_not_applied():
    node = make_leader()
    node.append_entry({
        "action": "put",
        "key": "search_flights",
        "value": {"enabled": True},
    })

    assert node.apply_committed_entries() == []
    assert node.state_machine.get("search_flights") is None
    assert node.last_applied == -1


def test_committed_entry_is_applied():
    node = make_leader()
    node.append_entry({
        "action": "put",
        "key": "search_flights",
        "value": {"enabled": True},
    })
    node.commit_index = 0

    results = node.apply_committed_entries()

    assert results == [{"ok": True, "key": "search_flights"}]
    assert node.state_machine.get("search_flights") == {"enabled": True}
    assert node.last_applied == 0


def test_entry_is_not_applied_twice():
    node = make_leader()
    node.append_entry({
        "action": "put",
        "key": "flights",
        "value": {"count": 2},
    })
    node.commit_index = 0

    node.apply_committed_entries()
    results = node.apply_committed_entries()

    assert results == []
    assert node.last_applied == 0


def test_multiple_committed_entries_are_applied_in_order():
    node = make_leader()
    node.append_entry({
        "action": "put",
        "key": "flights",
        "value": {"count": 2},
    })
    node.append_entry({
        "action": "put",
        "key": "hotels",
        "value": {"count": 3},
    })
    node.commit_index = 1

    results = node.apply_committed_entries()

    assert len(results) == 2
    assert node.last_applied == 1
    assert node.state_machine.get("flights") == {"count": 2}
    assert node.state_machine.get("hotels") == {"count": 3}
