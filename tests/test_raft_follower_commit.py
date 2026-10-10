
from backend.app.raft.node import RaftNode


def test_follower_applies_entry_after_receiving_leader_commit():
    leader = RaftNode("node-1", ["node-2", "node-3"])
    follower = RaftNode("node-2", ["node-1", "node-3"])

    leader.become_candidate()
    leader.become_leader()

    leader.append_entry({
        "action": "put",
        "key": "search_flights",
        "value": {"name": "search_flights"},
    })

    assert leader.replicate_to_follower(follower) is True

    leader.advance_commit_index()
    assert leader.commit_index == 0

    # Replicate again so the follower receives the leader's new commit index.
    assert leader.replicate_to_follower(follower) is True

    assert follower.commit_index == 0

    results = follower.apply_committed_entries()

    assert follower.last_applied == 0
    assert len(results) == 1
