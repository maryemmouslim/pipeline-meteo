from __future__ import annotations

import json
import os
from datetime import datetime

import pandas as pd
from minio import Minio
from pyspark.sql import SparkSession

from src.quality.expectations import validate_batch


def client() -> Minio:
    return Minio(
        os.getenv("MINIO_ENDPOINT", "localhost:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        secure=False,
    )


def write_parquet(frame: pd.DataFrame, prefix: str) -> None:
    if frame.empty:
        return
    path = f"/tmp/{prefix}-{datetime.utcnow().strftime('%Y%m%d%H%M%S%f')}.parquet"
    frame.to_parquet(path, index=False)
    object_name = f"{prefix}/{os.path.basename(path)}"
    minio = client()
    if not minio.bucket_exists("weather"):
        minio.make_bucket("weather")
    minio.fput_object("weather", object_name, path, content_type="application/octet-stream")
    os.remove(path)


def process_batch(rows) -> None:
    values = [json.loads(row.value.decode("utf-8")) for row in rows]
    if not values:
        return
    valid, invalid = validate_batch(pd.DataFrame(values))
    write_parquet(valid, "clean")
    write_parquet(invalid, "quarantine")
    print(f"processed={len(values)} valid={len(valid)} invalid={len(invalid)}", flush=True)


def main() -> None:
    spark = (
        SparkSession.builder.appName("weather-streaming")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
        .option("subscribe", "weather.raw")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )
    query = stream.writeStream.foreachBatch(
        lambda dataframe, _: process_batch(dataframe.select("value").toLocalIterator())
    ).option("checkpointLocation", "/tmp/weather-checkpoints").start()
    query.awaitTermination()


if __name__ == "__main__":
    main()
