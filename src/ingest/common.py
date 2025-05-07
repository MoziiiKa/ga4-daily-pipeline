from datetime import datetime, timezone
from google.cloud import logging as gcp_logging

# Initialize Cloud Logging client and logger
log_client = gcp_logging.Client()
log_handler = log_client.get_default_handler()
logger = log_client.logger("ga4-ingest")  # structured logger


def _log(msg: str, severity: str = "INFO"):
    """
    Log a structured message to Cloud Logging with a component label and current UTC day.

    Args:
        msg: The log message.
        severity: Log severity level (e.g., "INFO", "ERROR").
    """
    logger.log_struct(
        {"message": msg},
        severity=severity,
        labels={
            "component": "ingest",
            "day": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        },
    )
