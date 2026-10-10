
from backend.app.raft.node import RaftNode


def test_leader_replicates_multiple_entries_to_empty_follower():
    leader = RaftNode("node-1", ["node-2", "node-3"])
    follower = RaftNode("node-2", ["node-1", "node-3"])

    leader.become_candidate()
    leader.become_leader()

    leader.append_entry({
        "action": "put",
        "key": "search_flights",
        "value": {"name": "search_flights"},
    })
    leader.append_entry({
        "action": "put",
        "key": "search_hotels",
        "value": {"name": "search_hotels"},
    })

    success = leader.replicate_to_follower(follower)

    assert success is True
    assert follower.log == leader.log
    assert len(follower.log) == 2
    assert leader.match_index["node-2"] == 1
