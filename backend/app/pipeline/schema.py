from __future__ import annotations

from typing import Any, Dict

import pandas as pd
from genson import SchemaBuilder


def extract_schema_metadata(table_rows: dict[str, list[dict[str, Any]]]) -> dict[str, dict[str, Any]]:
    schemas: dict[str, dict[str, Any]] = {}

    for table_name, rows in table_rows.items():
        df = pd.DataFrame(rows)

        columns = []
        total_rows = max(len(df), 1)
        for col in df.columns:
            null_ratio = float(df[col].isnull().sum()) / total_rows
            columns.append(
                {
                    "name": str(col),
                    "dtype": str(df[col].dtype),
                    "null_ratio": round(null_ratio, 4),
                }
            )

        builder = SchemaBuilder()
        for row in rows:
            builder.add_object(row)
        json_schema = builder.to_schema()

        schemas[table_name] = {
            "row_count": len(rows),
            "columns": columns,
            "json_schema": json_schema,
        }

    return schemas


def detect_key_candidates(table_rows: dict[str, list[dict[str, Any]]]) -> dict[str, dict[str, list[dict[str, Any]]]]:
    key_meta: dict[str, dict[str, list[dict[str, Any]]]] = {}

    for table_name, rows in table_rows.items():
        df = pd.DataFrame(rows)
        primary_candidates: list[dict[str, Any]] = []
        foreign_candidates: list[dict[str, Any]] = []

        for col in df.columns:
            series = df[col].dropna()
            if len(series) == 0:
                continue

            uniqueness_ratio = float(series.nunique()) / max(len(series), 1)
            name_bonus = 0.1 if "_id" in str(col).lower() or str(col).lower().endswith("id") else 0.0
            score = min(1.0, uniqueness_ratio + name_bonus)

            candidate = {
                "column": str(col),
                "uniqueness_ratio": round(uniqueness_ratio, 4),
                "score": round(score, 4),
            }

            if score > 0.9:
                primary_candidates.append(candidate)
            elif "_id" in str(col).lower() or str(col).lower().endswith("id"):
                foreign_candidates.append(candidate)

        key_meta[table_name] = {
            "primary_key_candidates": primary_candidates,
            "foreign_key_candidates": foreign_candidates,
        }

    return key_meta
