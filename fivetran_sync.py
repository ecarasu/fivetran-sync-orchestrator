import os
import requests
import time
from datetime import datetime, timedelta
from config.connections_loader import load_connections
from slack_notifier import notify_sync_status

API_KEY = os.getenv("FIVETRAN_API_KEY")
if not API_KEY:
    raise RuntimeError("FIVETRAN_API_KEY environment variable not set")

CONNECTIONS = load_connections("connections.json")
BASE_URL = "https://api.fivetran.com/v1/"
HEADERS = {
    "Accept": "application/json;version=2",
    "Authorization": f"Basic {API_KEY}",
}

# How long we’re willing to wait for a single connection (in minutes)
MAX_WAIT_MINUTES = 120


def get_account_name():
    url = f"{BASE_URL}account/info"
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    return resp.json()["data"]["account_name"]


def trigger_sync(connection_id):
    url = f"{BASE_URL}connections/{connection_id}/sync"
    payload = {"force": True}
    resp = requests.post(url, headers=HEADERS, json=payload)
    if not resp.ok:
        raise RuntimeError(
            f"Failed to trigger sync for {connection_id}. "
            f"Status {resp.status_code}: {resp.text}"
        )
    return resp.json()


def get_connection_state(connection_id):
    url = f"{BASE_URL}connections/{connection_id}"
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    data = resp.json()["data"]

    status = data["status"]["sync_state"]
    succeeded_at = data.get("succeeded_at")
    schema = data.get("schema")

    return status, succeeded_at, schema


def wait_for_sync(connection_id, poll_interval_seconds):
    start = datetime.now()
    deadline = start + timedelta(minutes=MAX_WAIT_MINUTES)

    while True:
        status, succeeded_at, schema = get_connection_state(connection_id)

        if status != "syncing":
            print(
                f"[{datetime.now().strftime('%H:%M:%S')}] "
                f"Connection '{connection_id}' (schema: {schema}) finished with state '{status}'. "
                f"succeeded_at={succeeded_at}"
            )
            notify_sync_status(
                connection_id=connection_id,
                status=status,
                schema=schema,
                succeeded_at=succeeded_at
            )
            return status

        if datetime.now() > deadline:
            print(
                f"[{datetime.now().strftime('%H:%M:%S')}] "
                f"Timed out waiting for connection '{connection_id}' (still '{status}')"
            )
            return "timeout"

        print(
            f"[{datetime.now().strftime('%H:%M:%S')}] "
            f"Connection '{connection_id}' still syncing..."
        )
        time.sleep(poll_interval_seconds)


def main():
    try:
        account_name = get_account_name()
        print(f"Current account: {account_name}")
    except Exception as e:
        print(f"Error getting account info: {e}")
        return

    for conn_id, poll_interval in CONNECTIONS:
        print(f"\n=== Triggering sync for connection '{conn_id}' ===")
        try:
            notify_sync_status(
                message = {
                    "text": (
                        f"\nTriggering sync for connection '{conn_id}'\n"
                    )
    }

            )
            trigger_sync(conn_id)
            
        except Exception as e:
            print(f"Error triggering sync for {conn_id}: {e}")
            continue

        # Give Fivetran a moment to register the sync
        time.sleep(15)

        try:
            final_status = wait_for_sync(conn_id, poll_interval)
            print(f"Final status for {conn_id}: {final_status}")
        except Exception as e:
            print(f"Error while polling {conn_id}: {e}")


if __name__ == "__main__":
    main()
