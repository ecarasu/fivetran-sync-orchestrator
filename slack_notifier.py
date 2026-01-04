import os
import requests
from typing import Optional, Dict, Any


SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")


# Slack Incoming Webhook URL
# Pulled from environment variables to avoid hardcoding secrets

def notify_sync_status(
    connection_id: Optional[str] = None,
    status: Optional[str] = None,
    schema: Optional[str] = None,
    succeeded_at: Optional[str] = None,
    message: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Sends a Slack notification when a Fivetran sync reaches a terminal state.

    This function is intentionally designed to be:
    - Safe: never raises exceptions to the caller
    - Optional: becomes a no-op if Slack webhook is not configured
    - Flexible: allows a fully custom Slack payload via `message`
    """

    # Exit early if Slack webhook is not configured
    # This ensures the data pipeline never fails due to missing Slack setup
    if not SLACK_WEBHOOK_URL:
        return

    # Normalize status value to avoid runtime errors (e.g., None.upper())
    status_text = status.upper() if status else "UNKNOWN"

    # If a custom Slack payload is not provided,
    # construct a default, human-readable message
    if message is None:
        message = {
            "text": (
                "📦 *Fivetran Sync Completed*\n"
                f"*Connection:* `{connection_id or 'N/A'}`\n"
                f"*Status:* `{status_text}`\n"
                f"*Schema:* `{schema or 'N/A'}`\n"
                f"*Completed At:* {succeeded_at or 'N/A'} UTC"
            )
        }

    try:
        # Send the message to Slack using the Incoming Webhook
        # A timeout is used to prevent blocking the pipeline
        response = requests.post(
            SLACK_WEBHOOK_URL,
            json=message,
            timeout=10,
        )

        # Raise an exception for non-2xx responses
        response.raise_for_status()

    except Exception as exc:
        # Slack failures should never break the data pipeline
        # Errors are logged instead of being raised
        print(f"Slack notification failed for {connection_id}: {exc}")
