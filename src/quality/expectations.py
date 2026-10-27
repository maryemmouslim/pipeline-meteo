from __future__ import annotations

import pandas as pd

from src.common.schema import FIELDS


def validate_batch(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Validate a batch and return (valid_rows, invalid_rows)."""
    invalid: list[dict] = []
    invalid_indexes: list[int] = []
    for row_index, row in frame.iterrows():
        event = row.to_dict()
        errors = []
        missing = [field for field in FIELDS if pd.isna(event.get(field))]
        errors.extend(f"missing field: {field}" for field in missing)
        if not errors:
            from src.common.schema import validate_shape
            errors = validate_shape(event)
        if errors:
            item = dict(event)
            item["quality_errors"] = "; ".join(errors)
            invalid.append(item)
            invalid_indexes.append(row_index)
    invalid_frame = pd.DataFrame(invalid)
    if invalid_frame.empty:
        invalid_frame = pd.DataFrame(columns=[*frame.columns, "quality_errors"])
    valid = frame.drop(index=invalid_indexes, errors="ignore") if invalid else frame.copy()
    valid = valid.drop_duplicates(subset=["event_id"], keep="last")
    return valid, invalid_frame
