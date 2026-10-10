
from backend.app.raft.node import RaftNode, NodeState


def test_leader_commits_entry_after_follower_replication():
    leader = RaftNode("node-1", ["node-2", "node-3"])
    follower = RaftNode("node-2", ["node-1", "node-3"])

    leader.become_candidate()
    leader.become_leader()

    leader.append_entry({
        "action": "put",
        "key": "search_flights",
        "value": {"name": "search_flights"},
    })

    success = leader.replicate_to_follower(follower)

    assert success is True
    assert leader.match_index["node-2"] == 0

    committed_index = leader.advance_commit_index()

    assert committed_index == 0
    assert leader.commit_index == 0
    assert follower.commit_index == -1
