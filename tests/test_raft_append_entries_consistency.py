
from backend.app.raft.node import RaftNode, NodeState


def make_follower():
    return RaftNode(
        node_id="node-2",
        peers=["node-1", "node-3"],
    )


def entry(term, key, value):
    return {
        "term": term,
        "command": {
            "action": "put",
            "key": key,
            "value": value,
        },
    }


def test_rejects_missing_previous_log_entry():
    follower = make_follower()

    accepted = follower.receive_append_entries(
        leader_term=1,
        entries=[entry(1, "flights", {"enabled": True})],
        prev_log_index=0,
        prev_log_term=1,
    )

    assert accepted is False
    assert follower.log == []


def test_rejects_mismatched_previous_log_term():
    follower = make_follower()
    follower.log = [entry(1, "flights", {"enabled": True})]

    accepted = follower.receive_append_entries(
        leader_term=2,
        entries=[entry(2, "hotels", {"enabled": True})],
        prev_log_index=0,
        prev_log_term=2,
    )

    assert accepted is False
    assert len(follower.log) == 1


def test_duplicate_replication_does_not_duplicate_entries():
    follower = make_follower()
    replicated_entry = entry(1, "flights", {"enabled": True})

    first = follower.receive_append_entries(
        leader_term=1,
        entries=[replicated_entry],
    )
    second = follower.receive_append_entries(
        leader_term=1,
        entries=[replicated_entry],
    )

    assert first is True
    assert second is True
    assert follower.log == [replicated_entry]


def test_conflicting_uncommitted_entry_is_replaced():
    follower = make_follower()
    follower.log = [entry(1, "old", {"value": 1})]

    accepted = follower.receive_append_entries(
        leader_term=2,
        entries=[entry(2, "new", {"value": 2})],
    )

    assert accepted is True
    assert follower.log == [entry(2, "new", {"value": 2})]


def test_committed_entry_cannot_be_overwritten():
    follower = make_follower()
    follower.log = [entry(1, "flights", {"enabled": True})]
    follower.commit_index = 0

    accepted = follower.receive_append_entries(
        leader_term=2,
        entries=[entry(2, "hotels", {"enabled": True})],
    )

    assert accepted is False
    assert follower.log == [entry(1, "flights", {"enabled": True})]
