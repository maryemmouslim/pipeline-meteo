import pandas as pd

from src.quality.expectations import validate_batch


def test_quality_splits_invalid_temperature():
    frame = pd.DataFrame([
        {
            "event_id": "ok", "timestamp": "2026-01-01T00:00:00+00:00",
            "station_id": "s1", "latitude": 48.8, "longitude": 2.3,
            "temperature_c": 20, "humidity_pct": 50, "pressure_hpa": 1013,
            "wind_speed_kmh": 4, "precipitation_mm": 0,
        },
        {
            "event_id": "bad", "timestamp": "2026-01-01T00:00:00+00:00",
            "station_id": "s1", "latitude": 48.8, "longitude": 2.3,
            "temperature_c": 100, "humidity_pct": 50, "pressure_hpa": 1013,
            "wind_speed_kmh": 4, "precipitation_mm": 0,
        },
    ])
    valid, invalid = validate_batch(frame)
    assert list(valid["event_id"]) == ["ok"]
    assert list(invalid["event_id"]) == ["bad"]
    assert "temperature_c outside" in invalid.iloc[0]["quality_errors"]

