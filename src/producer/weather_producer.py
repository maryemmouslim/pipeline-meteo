from __future__ import annotations

import argparse
import json
import math
import os
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer
from prometheus_client import Counter, Gauge, start_http_server

from src.common.schema import validate_shape

EVENTS_PRODUCED = Counter("weather_events_produced_total", "Number of weather events produced")
LAST_TEMPERATURE = Gauge("weather_last_temperature_c", "Last generated temperature in Celsius")


def build_event(index: int, station_id: str = "paris-01") -> dict:
    phase = index / 12
    return {
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "station_id": station_id,
        "latitude": 48.8566,
        "longitude": 2.3522,
        "temperature_c": round(15 + 8 * math.sin(phase) + random.gauss(0, 0.4), 2),
        "humidity_pct": round(max(0, min(100, 65 - 15 * math.sin(phase) + random.gauss(0, 2))), 2),
        "pressure_hpa": round(1013 + random.gauss(0, 5), 2),
        "wind_speed_kmh": round(max(0, 18 + random.gauss(0, 5)), 2),
        "precipitation_mm": round(max(0, random.gauss(0.7, 1.2)), 2),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--interval", type=float, default=2)
    parser.add_argument("--count", type=int, default=0, help="0 means run continuously")
    args = parser.parse_args()
    start_http_server(8000)
    producer = Producer({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")})
    index = 0
    try:
        while args.count == 0 or index < args.count:
            event = build_event(index)
            errors = validate_shape(event)
            if errors:
                raise ValueError(f"generated invalid event: {errors}")
            producer.produce(
                "weather.raw",
                key=event["station_id"],
                value=json.dumps(event).encode(),
            )
            producer.poll(0)
            EVENTS_PRODUCED.inc()
            LAST_TEMPERATURE.set(event["temperature_c"])
            index += 1
            time.sleep(args.interval)
    finally:
        producer.flush(10)


if __name__ == "__main__":
    main()
