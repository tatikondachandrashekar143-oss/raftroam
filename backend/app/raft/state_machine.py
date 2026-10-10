
class RaftStateMachine:
    """Apply committed Raft commands to an in-memory key-value store."""

    def __init__(self):
        self.data: dict[str, dict] = {}
        self.last_applied = -1

    def apply(self, index: int, command: dict) -> dict:
        """Apply the next committed command exactly once."""
        if index != self.last_applied + 1:
            raise ValueError("Commands must be applied in log order")

        action = command.get("action")
        key = command.get("key")

        if not isinstance(key, str) or not key:
            raise ValueError("Command requires a non-empty key")

        if action == "put":
            value = command.get("value")
            if not isinstance(value, dict):
                raise ValueError("Put command requires a dictionary value")
            self.data[key] = value
            result = {"ok": True, "key": key}

        elif action == "delete":
            existed = key in self.data
            self.data.pop(key, None)
            result = {"ok": True, "key": key, "deleted": existed}

        else:
            raise ValueError("Unsupported state-machine action")

        self.last_applied = index
        return result

    def get(self, key: str) -> dict | None:
        return self.data.get(key)

    def list_keys(self) -> list[str]:
        return sorted(self.data.keys())
