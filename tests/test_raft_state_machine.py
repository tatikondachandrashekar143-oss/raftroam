
import pytest

from backend.app.raft.state_machine import RaftStateMachine


def test_put_stores_tool_definition():
    machine = RaftStateMachine()

    result = machine.apply(
        0,
        {
            "action": "put",
            "key": "search_buses",
            "value": {"description": "Search available buses"},
        },
    )

    assert result["ok"] is True
    assert machine.get("search_buses") == {
        "description": "Search available buses"
    }
    assert machine.last_applied == 0


def test_delete_removes_existing_key():
    machine = RaftStateMachine()
    machine.apply(0, {"action": "put", "key": "search_buses", "value": {}})

    result = machine.apply(1, {"action": "delete", "key": "search_buses"})

    assert result["deleted"] is True
    assert machine.get("search_buses") is None


def test_commands_must_be_applied_in_order():
    machine = RaftStateMachine()

    with pytest.raises(ValueError):
        machine.apply(1, {"action": "put", "key": "tool", "value": {}})


def test_invalid_command_does_not_advance_last_applied():
    machine = RaftStateMachine()

    with pytest.raises(ValueError):
        machine.apply(0, {"action": "unknown", "key": "tool"})

    assert machine.last_applied == -1
