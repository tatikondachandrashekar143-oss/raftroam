
import pytest

from backend.app.raft.node import RaftNode


def test_leader_rejects_unknown_follower():
    leader = RaftNode("node-1", ["node-2", "node-3"])
    unknown_node = RaftNode("node-4", ["node-1"])

    leader.become_candidate()
    leader.become_leader()

    with pytest.raises(ValueError, match="Unknown follower"):
        leader.replicate_to_follower(unknown_node)
