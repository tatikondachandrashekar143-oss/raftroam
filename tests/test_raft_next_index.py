
from backend.app.raft.node import RaftNode


def test_leader_initializes_replication_indexes_for_each_follower():
    leader = RaftNode("node-1", ["node-2", "node-3"])

    leader.become_candidate()
    leader.become_leader()

    leader.append_entry({
        "action": "put",
        "key": "search_flights",
        "value": {"name": "search_flights"},
    })

    # Reset tracking to simulate the initialization done at election.
    leader.become_candidate()
    leader.become_leader()

    assert leader.next_index == {
        "node-2": 1,
        "node-3": 1,
    }

    assert leader.match_index == {
        "node-2": -1,
        "node-3": -1,
    }

