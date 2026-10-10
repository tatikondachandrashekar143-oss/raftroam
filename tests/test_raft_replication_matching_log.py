
from backend.app.raft.node import RaftNode


def test_replication_does_not_duplicate_matching_entries():
    leader = RaftNode("node-1", ["node-2", "node-3"])
    follower = RaftNode("node-2", ["node-1", "node-3"])

    leader.become_candidate()
    leader.become_leader()

    leader.append_entry({
        "action": "put",
        "key": "search_flights",
        "value": {"name": "search_flights"},
    })

    # First replication copies the entry to the follower.
    assert leader.replicate_to_follower(follower) is True
    assert len(follower.log) == 1

    # Repeating replication must not duplicate the same entry.
    assert leader.replicate_to_follower(follower) is True

    assert follower.log == leader.log
    assert len(follower.log) == 1
    assert leader.match_index["node-2"] == 0
