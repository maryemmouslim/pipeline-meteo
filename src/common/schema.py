"""Canonical event schema shared by the producer and streaming job."""

from __future__ import annotations

from datetime import datetime
from typing import Any

FIELDS = (
    "event_id", "timestamp", "station_id", "latitude", "longitude",
    "temperature_c", "humidity_pct", "pressure_hpa", "wind_speed_kmh",
    "precipitation_mm",
)


def validate_shape(event: dict[str, Any]) -> list[str]:
    """Return explicit validation errors for one event."""
    errors = [f"missing field: {field}" for field in FIELDS if field not in event]
    if errors:
        return errors
    try:
        datetime.fromisoformat(str(event["timestamp"]).replace("Z", "+00:00"))
    except ValueError:
        errors.append("timestamp is not ISO-8601")
    bounds = {
        "temperature_c": (-80, 60),
        "humidity_pct": (0, 100),
        "pressure_hpa": (800, 1200),
        "wind_speed_kmh": (0, 300),
        "precipitation_mm": (0, 500),
        "latitude": (-90, 90),
        "longitude": (-180, 180),
    }
    for field, (lower, upper) in bounds.items():
        try:
            value = float(event[field])
        except (TypeError, ValueError):
            errors.append(f"{field} is not numeric")
            continue
        if not lower <= value <= upper:
            errors.append(f"{field} outside [{lower}, {upper}]")
    return errors

