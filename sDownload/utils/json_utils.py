import json
from datetime import datetime, timezone
from typing import Any, Dict


def json_serializer(obj: Any) -> Any:
    """
    Custom JSON serializer for objects not supported by default json module.
    Handles datetime objects by converting them to ISO 8601 strings with Zulu suffix.
    """
    if isinstance(obj, datetime):
        # Ensure we have a timezone-aware object for consistency, default to UTC
        if obj.tzinfo is None:
            obj = obj.replace(tzinfo=timezone.utc)
        # Convert to ISO format and replace +00:00 with Z
        return obj.isoformat().replace("+00:00", "Z")
    raise TypeError(f"Type {type(obj)} not serializable")


def json_dumps(obj: Any, indent: int = 4) -> str:
    """
    Dump an object to a JSON string with standardized date formatting.
    """
    return json.dumps(obj, indent=indent, default=json_serializer)


def parse_json_date(date_str: str) -> datetime:
    """
    Parse an ISO 8601 date string, handling the 'Z' suffix as UTC.
    """
    if date_str.endswith("Z"):
        date_str = date_str.replace("Z", "+00:00")
    return datetime.fromisoformat(date_str)
