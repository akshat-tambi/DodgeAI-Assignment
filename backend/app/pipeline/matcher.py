from __future__ import annotations

from itertools import combinations
import re
from typing import Any

from rapidfuzz import fuzz


def _dtype_family(dtype_name: str) -> str:
    d = dtype_name.lower()
    if any(k in d for k in ["int", "float", "double", "decimal"]):
        return "numeric"
    if any(k in d for k in ["date", "time"]):
        return "datetime"
    if "bool" in d:
        return "boolean"
    return "string"


def _name_similarity(a: str, b: str) -> float:
    ratio = fuzz.ratio(a, b)
    token_ratio = fuzz.token_sort_ratio(a, b)
    return max(ratio, token_ratio) / 100.0


def _normalize_column_name(name: str) -> str:
    s = name.lower().strip()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


def _is_attribute_match(name_a: str, name_b: str, min_name_similarity: float) -> tuple[bool, float]:
    raw_sim = _name_similarity(name_a, name_b)
    norm_a = _normalize_column_name(name_a)
    norm_b = _normalize_column_name(name_b)

    # Exact normalized attribute match is a strong signal.
    if norm_a == norm_b:
        return True, max(raw_sim, 1.0)

    # Attribute variants like billingDocument vs cancelledBillingDocument.
    if norm_a in norm_b or norm_b in norm_a:
        return True, max(raw_sim, 0.75)

    return raw_sim >= min_name_similarity, raw_sim


def find_candidate_column_matches(
    schemas: dict[str, dict[str, Any]],
    min_name_similarity: float = 0.65,
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []

    for table_a, table_b in combinations(sorted(schemas.keys()), 2):
        cols_a = schemas[table_a].get("columns", [])
        cols_b = schemas[table_b].get("columns", [])

        for col_a in cols_a:
            for col_b in cols_b:
                matched, name_sim = _is_attribute_match(col_a["name"], col_b["name"], min_name_similarity)
                if not matched:
                    continue

                family_a = _dtype_family(col_a["dtype"])
                family_b = _dtype_family(col_b["dtype"])
                type_match = 1 if family_a == family_b else 0

                candidates.append(
                    {
                        "table_a": table_a,
                        "column_a": col_a["name"],
                        "table_b": table_b,
                        "column_b": col_b["name"],
                        "name_similarity": round(name_sim, 4),
                        "type_match": type_match,
                    }
                )

    return candidates
