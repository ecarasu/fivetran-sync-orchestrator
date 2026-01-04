import json
from pathlib import Path

def load_connections(config_path="connections.json"):
    path = Path(config_path)

    if not path.exists():
        raise FileNotFoundError(f"Connections file not found: {path}")

    with path.open() as f:
        config = json.load(f)

    connections = config.get("connections", [])
    if not connections:
        raise ValueError("No connections defined in connections.json")

    active_connections = []

    for c in connections:
        active_flag = c.get("Active", "N")

        if active_flag != "Y":
            continue

        active_connections.append(
            (c["id"], c["poll_interval_seconds"])
        )

    return active_connections
