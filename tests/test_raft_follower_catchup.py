
from backend.app.raft.node import RaftNode


def test_follower_catches_up_when_its_log_is_behind():
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

    # Simulate the follower having only the first entry.
    follower.log.append(leader.log[0].copy())

    assert len(follower.log) == 1
    assert len(leader.log) == 2

    success = leader.replicate_to_follower(follower)

    assert success is True
    assert follower.log == leader.log
    assert len(follower.log) == 2
    assert leader.match_index["node-2"] == 1

