
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from backend.app.raft.election import run_election
from backend.app.raft.node import NodeState, RaftNode
from backend.app.storage.registry import ToolRegistry


app = FastAPI(
    title="RaftRoam API",
    description="Fault-tolerant agentic travel planning backend",
    version="0.2.0",
)


# --------------------------------------------------
# 1. Initialize the three-node cluster
# --------------------------------------------------

node_ids = ["node-1", "node-2", "node-3"]

nodes = {
    node_id: RaftNode(
        node_id=node_id,
        peers=[peer for peer in node_ids if peer != node_id],
    )
    for node_id in node_ids
}


# Elect the initial leader.
run_election(nodes["node-1"], list(nodes.values()))


# --------------------------------------------------
# 2. Leader management
# --------------------------------------------------

def get_current_leader():
    """Return the current available leader, if one exists."""
    for node in nodes.values():
        if node.available and node.state == NodeState.LEADER:
            return node

    return None


def trigger_failover():
    """Elect a replacement leader when necessary."""
    current_leader = get_current_leader()

    if current_leader is not None:
        return current_leader

    for node in nodes.values():
        if not node.available:
            continue

        if run_election(node, list(nodes.values())):
            return node

    return None


def get_active_registry():
    """Create a registry connected to the current leader."""
    leader = get_current_leader()

    if leader is None:
        raise HTTPException(
            status_code=503,
            detail="No available Raft leader",
        )

    return leader, ToolRegistry(leader)


# --------------------------------------------------
# 3. Health endpoint
# --------------------------------------------------

@app.get("/health")
def health_check():
    return {
        "status": "healthy",
        "service": "RaftRoam API",
    }


# --------------------------------------------------
# 4. Simulate node failure
# --------------------------------------------------

class NodeFailureRequest(BaseModel):
    node_id: str


@app.post("/cluster/fail")
def simulate_node_failure(request: NodeFailureRequest):
    node = nodes.get(request.node_id)

    if node is None:
        raise HTTPException(
            status_code=404,
            detail="Node not found",
        )

    if not node.available:
        raise HTTPException(
            status_code=409,
            detail="Node is already unavailable",
        )

    node.available = False

    if node.state == NodeState.LEADER:
        node.state = NodeState.FOLLOWER

    new_leader = trigger_failover()

    return {
        "failed_node": request.node_id,
        "new_leader": (
            new_leader.node_id if new_leader else None
        ),
        "failover_successful": new_leader is not None,
    }


# --------------------------------------------------
# 5. Cluster status
# --------------------------------------------------

@app.get("/cluster/status")
def cluster_status():
    current_leader = get_current_leader()

    return {
        "nodes": [
            {
                "node_id": node.node_id,
                "state": node.state.value,
                "term": node.current_term,
                "commit_index": node.commit_index,
                "available": node.available,
            }
            for node in nodes.values()
        ],
        "leader_id": (
            current_leader.node_id
            if current_leader is not None
            else None
        ),
    }


# --------------------------------------------------
# 6. List registered tools
# --------------------------------------------------

@app.get("/registry/tools")
def list_tools():
    _, registry = get_active_registry()
    return {"tools": registry.list_tools()}


# --------------------------------------------------
# 7. Register and replicate a tool
# --------------------------------------------------

@app.post("/registry/tools")
def register_tool(tool: dict):
    leader, registry = get_active_registry()

    try:
        # Append the command to the current leader's log.
        result = registry.register_tool(tool)
        log_index = result["log_index"]

        # Replicate the entry to available followers.
        for node in nodes.values():
            if (
                node.node_id != leader.node_id
                and node.available
            ):
                leader.replicate_to_follower(node)

        # Advance the commit index only when a majority
        # has replicated the entry.
        leader.advance_commit_index()
        committed = leader.commit_index >= log_index

        if not committed:
            return {
                "accepted": True,
                "tool": tool["name"],
                "log_index": log_index,
                "committed": False,
                "message": (
                    "Entry appended but not yet committed"
                ),
            }

        # Apply committed entries on the leader.
        leader.apply_committed_entries()

        # Propagate the updated commit index to available
        # followers and apply their committed entries.
        for node in nodes.values():
            if (
                node.node_id != leader.node_id
                and node.available
            ):
                leader.replicate_to_follower(node)
                node.apply_committed_entries()

        return {
            "accepted": True,
            "tool": tool["name"],
            "log_index": log_index,
            "committed": True,
        }

    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=str(exc),
        ) from exc

    except RuntimeError as exc:
        raise HTTPException(
            status_code=409,
            detail=str(exc),
        ) from exc


# --------------------------------------------------
# 8. Check cluster consistency
# --------------------------------------------------

@app.get("/cluster/consistency")
def cluster_consistency():
    node_results = []

    for node in nodes.values():
        committed_tools = {
            key: node.state_machine.get(key)
            for key in node.state_machine.list_keys()
        }

        node_results.append({
            "node_id": node.node_id,
            "available": node.available,
            "commit_index": node.commit_index,
            "last_applied": node.last_applied,
            "tools": committed_tools,
        })

    # Compare available nodes. An unavailable node may be
    # behind until it recovers and catches up.
    active_results = [
        result
        for result in node_results
        if result["available"]
    ]

    if not active_results:
        consistent = False
    else:
        first = active_results[0]

        consistent = all(
            result["commit_index"] == first["commit_index"]
            and result["last_applied"] == first["last_applied"]
            and result["tools"] == first["tools"]
            for result in active_results
        )

    return {
        "status": (
            "CONSISTENT" if consistent else "INCONSISTENT"
        ),
        "nodes": len(node_results),
        "available_nodes": len(active_results),
        "details": node_results,
    }

# --------------------------------------------------
# 9. Travel request demo endpoint
# --------------------------------------------------

class TravelRequest(BaseModel):
    action: str
    origin: str | None = None
    destination: str | None = None
    flight_number: str | None = None


@app.post("/demo/travel-request")
def demo_travel_request(request: TravelRequest):
    # Route the request through the current leader's registry.
    _, active_registry = get_active_registry()

    if request.action not in {
        "search_flights",
        "check_flight_status",
    }:
        raise HTTPException(
            status_code=400,
            detail="Unsupported travel action",
        )

    # Confirm that the requested tool is registered.
    tool = active_registry.get_tool(request.action)

    if tool is None:
        raise HTTPException(
            status_code=404,
            detail=f"Tool '{request.action}' is not registered",
        )

    # Validate the required input for the selected tool.
    if request.action == "search_flights":
        if not request.origin or not request.destination:
            raise HTTPException(
                status_code=400,
                detail="origin and destination are required",
            )

        # Demo data only; no real airline API is called.
        result = {
            "origin": request.origin.upper(),
            "destination": request.destination.upper(),
            "flights": [
                {
                    "flight_number": "AI202",
                    "departure": "06:30",
                    "arrival": "09:45",
                    "price_inr": 5200,
                    "status": "ON TIME",
                },
                {
                    "flight_number": "AI405",
                    "departure": "09:15",
                    "arrival": "12:40",
                    "price_inr": 5850,
                    "status": "ON TIME",
                },
            ],
        }

    else:
        if not request.flight_number:
            raise HTTPException(
                status_code=400,
                detail="flight_number is required",
            )

        # Demo status only; no live flight API is called.
        result = {
            "flight_number": request.flight_number.upper(),
            "status": "ON TIME",
            "departure": "06:30",
            "gate": "B12",
            "source": "demo simulator",
        }

    return {
        "success": True,
        "action": request.action,
        "tool": tool["name"],
        "leader_id": get_current_leader().node_id,
        "result": result,
    }




