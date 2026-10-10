
from backend.app.raft.node import RaftNode


def test_leader_replication_replaces_conflicting_uncommitted_entry():
    leader = RaftNode("node-1", ["node-2", "node-3"])
    follower = RaftNode("node-2", ["node-1", "node-3"])

    leader.become_candidate()
    leader.become_leader()

    leader.append_entry({
        "action": "put",
        "key": "search_flights",
        "value": {"source": "leader"},
    })

    # Simulate a conflicting, uncommitted entry on the follower.
    follower.log.append({
        "term": 1,
        "command": {
            "action": "put",
            "key": "search_flights",
            "value": {"source": "follower"},
        },
    })

    success = leader.replicate_to_follower(follower)

    assert success is True
    assert follower.log == leader.log
    assert follower.log[0]["command"]["value"]["source"] == "leader"
