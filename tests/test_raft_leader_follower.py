
from backend.app.raft.node import RaftNode, NodeState


def test_leader_replicates_entry_to_follower_and_records_ack():
    leader = RaftNode("node-1", ["node-2", "node-3"])
    follower = RaftNode("node-2", ["node-1", "node-3"])

    leader.become_candidate()
    leader.become_leader()

    leader.append_entry({
        "action": "put",
        "key": "search_flights",
        "value": {"name": "search_flights"},
    })

    # This method will connect the leader's existing
    # replication logic to the follower.
    success = leader.replicate_to_follower(follower)

    assert success is True
    assert follower.log == leader.log
    assert leader.match_index["node-2"] == 0
    assert follower.state == NodeState.FOLLOWER