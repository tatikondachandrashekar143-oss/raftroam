
from backend.app.raft.node import RaftNode


def test_leader_replication_replaces_divergent_uncommitted_suffix():
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

    # Give the follower a conflicting, uncommitted second entry.
    follower.log = [
        leader.log[0].copy(),
        {
            "term": 0,
            "command": {
                "action": "put",
                "key": "old_tool",
                "value": {"name": "old_tool"},
            },
        },
    ]

    success = leader.replicate_to_follower(follower)

    assert success is True
    assert follower.log == leader.log
    assert follower.log[1]["command"]["key"] == "search_hotels"
