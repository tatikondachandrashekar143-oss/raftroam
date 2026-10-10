
from backend.app.raft.node import RaftNode


def test_replication_does_not_overwrite_committed_entry():
    leader = RaftNode("node-1", ["node-2", "node-3"])
    follower = RaftNode("node-2", ["node-1", "node-3"])

    leader.become_candidate()
    leader.become_leader()

    leader.append_entry({
        "action": "put",
        "key": "search_flights",
        "value": {"source": "leader"},
    })

    # Simulate an already-committed, conflicting follower entry.
    follower.log.append({
        "term": 1,
        "command": {
            "action": "put",
            "key": "search_flights",
            "value": {"source": "follower"},
        },
    })
    follower.commit_index = 0

    success = leader.replicate_to_follower(follower)

    assert success is False
    assert follower.log[0]["command"]["value"]["source"] == "follower"

