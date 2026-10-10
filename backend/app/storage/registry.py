
from backend.app.raft.node import NodeState, RaftNode


class ToolRegistry:
    """Store tool definitions through the Raft-backed state machine."""

    def __init__(self, node: RaftNode):
        self.node = node

    def register_tool(self, tool: dict) -> dict:
        if not isinstance(tool, dict):
            raise ValueError("Tool definition must be a dictionary")

        name = tool.get("name")

        if not isinstance(name, str) or not name.strip():
            raise ValueError("Tool requires a non-empty name")

        if self.node.state != NodeState.LEADER:
            raise RuntimeError("Only the Raft leader can register tools")

        index = self.node.append_entry({
            "action": "put",
            "key": name,
            "value": tool,
        })

        return {
            "accepted": True,
            "tool": name,
            "log_index": index,
            "committed": False,
        }

    def get_tool(self, name: str) -> dict | None:
        return self.node.state_machine.get(name)

    def list_tools(self) -> list[dict]:
        return [
            self.node.state_machine.get(key)
            for key in self.node.state_machine.list_keys()
        ]

    def delete_tool(self, name: str) -> dict:
        if not isinstance(name, str) or not name.strip():
            raise ValueError("Tool name must be a non-empty string")

        if self.node.state != NodeState.LEADER:
            raise RuntimeError("Only the Raft leader can delete tools")

        index = self.node.append_entry({
            "action": "delete",
            "key": name,
        })

        return {
            "accepted": True,
            "tool": name,
            "log_index": index,
            "committed": False,
        }

