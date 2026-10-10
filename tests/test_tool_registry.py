
import pytest

from backend.app.raft.node import NodeState, RaftNode
from backend.app.storage.registry import ToolRegistry


@pytest.fixture
def registry():
    node = RaftNode(
        node_id="node-1",
        peers=["node-2", "node-3"],
    )
    node.state = NodeState.LEADER
    node.current_term = 1
    return ToolRegistry(node)


def test_register_tool_appends_uncommitted_entry(registry):
    tool = {
        "name": "search_flights",
        "description": "Search available travel options",
        "input_schema": {
            "origin": "string",
            "destination": "string",
        },
    }

    result = registry.register_tool(tool)

    assert result["accepted"] is True
    assert result["tool"] == "search_flights"
    assert result["log_index"] == 0
    assert result["committed"] is False

    # Appending does not make the tool visible in the state machine.
    assert registry.get_tool("search_flights") is None


def test_tool_becomes_visible_after_commit_and_application(registry):
    tool = {
        "name": "search_hotels",
        "description": "Search hotels",
    }

    registry.register_tool(tool)
    registry.node.commit_index = 0
    registry.node.apply_committed_entries()

    assert registry.get_tool("search_hotels") == tool
    assert registry.list_tools() == [tool]


def test_register_tool_rejects_invalid_definition(registry):
    with pytest.raises(ValueError):
        registry.register_tool("not a dictionary")

    with pytest.raises(ValueError):
        registry.register_tool({"name": ""})

    assert registry.node.log == []


def test_non_leader_cannot_register_or_delete(registry):
    registry.node.state = NodeState.FOLLOWER

    with pytest.raises(RuntimeError):
        registry.register_tool({"name": "search_flights"})

    with pytest.raises(RuntimeError):
        registry.delete_tool("search_flights")

    assert registry.node.log == []


def test_delete_tool_only_takes_effect_after_commit(registry):
    tool = {
        "name": "search_flights",
        "description": "Search flights",
    }

    registry.register_tool(tool)
    registry.node.commit_index = 0
    registry.node.apply_committed_entries()

    result = registry.delete_tool("search_flights")

    assert result["committed"] is False
    assert registry.get_tool("search_flights") == tool

    registry.node.commit_index = 1
    registry.node.apply_committed_entries()

    assert registry.get_tool("search_flights") is None
    assert registry.list_tools() == []

