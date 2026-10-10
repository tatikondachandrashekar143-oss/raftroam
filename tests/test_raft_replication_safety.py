
import pytest

from backend.app.raft.node import RaftNode, NodeState


def test_follower_cannot_replicate_entries():
    follower = RaftNode("node-2", ["node-1", "node-3"])
    other_follower = RaftNode("node-3", ["node-1", "node-2"])

    assert follower.state == NodeState.FOLLOWER

    with pytest.raises(RuntimeError, match="Only the leader"):
        follower.replicate_to_follower(other_follower)
