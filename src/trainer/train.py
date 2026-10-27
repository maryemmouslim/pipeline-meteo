from __future__ import annotations

import argparse
import io
import os
import time
from datetime import datetime

import mlflow
import numpy as np
import pandas as pd
from minio import Minio
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
from xgboost import XGBRegressor

FEATURES = ["temperature_c", "humidity_pct", "pressure_hpa", "wind_speed_kmh", "precipitation_mm"]


def load_data() -> pd.DataFrame:
    mc = Minio(os.getenv("MINIO_ENDPOINT", "localhost:9000"),
               access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
               secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"), secure=False)
    objects = list(mc.list_objects("weather", prefix="clean/", recursive=True))
    frames = []
    for obj in objects:
        data = mc.get_object("weather", obj.object_name).read()
        frames.append(pd.read_parquet(io.BytesIO(data)))
    if not frames:
        return pd.DataFrame()
    frame = pd.concat(frames, ignore_index=True).sort_values("timestamp")
    frame["target"] = frame.groupby("station_id")["temperature_c"].shift(-1)
    return frame.dropna(subset=FEATURES + ["target"])


def train_once() -> dict[str, float] | None:
    data = load_data()
    if len(data) < 20:
        print(f"waiting for data ({len(data)}/20)", flush=True)
        return None
    x_train, x_test, y_train, y_test = train_test_split(
        data[FEATURES], data["target"], test_size=0.2, random_state=42
    )
    models = {
        "random_forest": RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1),
        "xgboost": XGBRegressor(n_estimators=100, max_depth=4, learning_rate=0.05, objective="reg:squarederror"),
    }
    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000"))
    mlflow.set_experiment("weather-temperature-forecast")
    best: tuple[float, str] | None = None
    results: dict[str, float] = {}
    for name, model in models.items():
        with mlflow.start_run(run_name=name):
            model.fit(x_train, y_train)
            prediction = model.predict(x_test)
            rmse = float(np.sqrt(mean_squared_error(y_test, prediction)))
            metrics = {"rmse": rmse, "mae": float(mean_absolute_error(y_test, prediction)),
                       "r2": float(r2_score(y_test, prediction))}
            mlflow.log_params({"model": name, "rows": len(data), "features": len(FEATURES)})
            mlflow.log_metrics(metrics)
            mlflow.sklearn.log_model(model, "model")
            results[f"{name}_rmse"] = rmse
            if best is None or rmse < best[0]:
                best = (rmse, name)
    _train_lstm_if_available(data)
    if best:
        print(f"champion={best[1]} rmse={best[0]:.4f}", flush=True)
    return results


def _train_lstm_if_available(data: pd.DataFrame) -> None:
    """Train a small sequence model without making TensorFlow mandatory for local use."""
    try:
        import tensorflow as tf
    except ImportError:
        print("tensorflow unavailable; skipping lstm", flush=True)
        return
    ordered = data.sort_values("timestamp")
    values = ordered[FEATURES].to_numpy(dtype=np.float32)
    targets = ordered["target"].to_numpy(dtype=np.float32)
    if len(values) < 12:
        return
    window = 3
    x = np.array([values[i - window:i] for i in range(window, len(values))])
    y = targets[window:]
    split = max(1, int(len(x) * 0.8))
    model = tf.keras.Sequential([
        tf.keras.layers.Input(shape=(window, len(FEATURES))),
        tf.keras.layers.LSTM(24),
        tf.keras.layers.Dense(1),
    ])
    model.compile(optimizer="adam", loss="mse")
    with mlflow.start_run(run_name="lstm"):
        model.fit(x[:split], y[:split], epochs=5, batch_size=16, verbose=0)
        prediction = model.predict(x[split:], verbose=0).reshape(-1)
        actual = y[split:]
        if len(actual):
            mlflow.log_params({"model": "lstm", "window": window, "rows": len(data)})
            mlflow.log_metrics({
                "rmse": float(np.sqrt(mean_squared_error(actual, prediction))),
                "mae": float(mean_absolute_error(actual, prediction)),
                "r2": float(r2_score(actual, prediction)),
            })
            mlflow.tensorflow.log_model(model, "model")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--loop", action="store_true")
    args = parser.parse_args()
    while True:
        train_once()
        if not args.loop:
            return
        time.sleep(300)


if __name__ == "__main__":
    main()
