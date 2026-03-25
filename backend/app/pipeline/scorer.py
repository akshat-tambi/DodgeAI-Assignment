from __future__ import annotations

from typing import Any


def _normalized_values(rows: list[dict[str, Any]], column: str) -> set[str]:
    values: set[str] = set()
    for row in rows:
        val = row.get(column)
        if val is None:
            continue
        values.add(str(val).strip())
    return values


def _uniqueness_ratio(rows: list[dict[str, Any]], column: str) -> float:
    non_null = [row.get(column) for row in rows if row.get(column) is not None]
    if not non_null:
        return 0.0
    return len({str(v).strip() for v in non_null}) / len(non_null)


def _looks_like_id(name: str) -> bool:
    n = name.lower()
    return any(token in n for token in ["id", "document", "order", "customer", "partner", "item", "delivery", "billing"])


def _key_lookup(key_meta: dict[str, dict[str, list[dict[str, Any]]]]) -> tuple[dict[str, set[str]], dict[str, set[str]]]:
    pk_map: dict[str, set[str]] = {}
    fk_map: dict[str, set[str]] = {}
    for table, meta in key_meta.items():
        pk_map[table] = {c["column"] for c in meta.get("primary_key_candidates", [])}
        fk_map[table] = {c["column"] for c in meta.get("foreign_key_candidates", [])}
    return pk_map, fk_map


def _infer_direction_and_type(
    candidate: dict[str, Any],
    uniq_a: float,
    uniq_b: float,
    pk_map: dict[str, set[str]],
    fk_map: dict[str, set[str]],
) -> tuple[str, str, str, str, str, float]:
    table_a = candidate["table_a"]
    col_a = candidate["column_a"]
    table_b = candidate["table_b"]
    col_b = candidate["column_b"]

    a_is_pk = col_a in pk_map.get(table_a, set()) or uniq_a >= 0.95
    b_is_pk = col_b in pk_map.get(table_b, set()) or uniq_b >= 0.95
    a_is_fk = col_a in fk_map.get(table_a, set()) or _looks_like_id(col_a)
    b_is_fk = col_b in fk_map.get(table_b, set()) or _looks_like_id(col_b)

    pkfk_bonus = 1.0 if ((a_is_fk and b_is_pk) or (b_is_fk and a_is_pk)) else 0.0

    if a_is_fk and b_is_pk and not (b_is_fk and a_is_pk):
        source_table, source_column, source_uniq = table_a, col_a, uniq_a
        target_table, target_column, target_uniq = table_b, col_b, uniq_b
    elif b_is_fk and a_is_pk and not (a_is_fk and b_is_pk):
        source_table, source_column, source_uniq = table_b, col_b, uniq_b
        target_table, target_column, target_uniq = table_a, col_a, uniq_a
    elif a_is_pk and not b_is_pk:
        source_table, source_column, source_uniq = table_b, col_b, uniq_b
        target_table, target_column, target_uniq = table_a, col_a, uniq_a
    elif b_is_pk and not a_is_pk:
        source_table, source_column, source_uniq = table_a, col_a, uniq_a
        target_table, target_column, target_uniq = table_b, col_b, uniq_b
    elif uniq_a < uniq_b:
        source_table, source_column, source_uniq = table_a, col_a, uniq_a
        target_table, target_column, target_uniq = table_b, col_b, uniq_b
    else:
        source_table, source_column, source_uniq = table_b, col_b, uniq_b
        target_table, target_column, target_uniq = table_a, col_a, uniq_a

    if source_uniq >= 0.95 and target_uniq >= 0.95:
        relationship_type = "1-1"
    elif source_uniq < 0.95 and target_uniq >= 0.95:
        relationship_type = "many-1"
    elif source_uniq >= 0.95 and target_uniq < 0.95:
        relationship_type = "1-many"
    else:
        relationship_type = "many-many"

    return source_table, source_column, target_table, target_column, relationship_type, pkfk_bonus


def score_relationships(
    candidate_pairs: list[dict[str, Any]],
    table_rows: dict[str, list[dict[str, Any]]],
    key_meta: dict[str, dict[str, list[dict[str, Any]]]],
    overlap_threshold: float,
    confidence_threshold: float,
    borderline_low: float,
    borderline_high: float,
    min_intersection_size: int,
    min_distinct_values: int,
    max_relationships_per_table_pair: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    accepted: list[dict[str, Any]] = []
    borderline: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    pk_map, fk_map = _key_lookup(key_meta)

    for candidate in candidate_pairs:
        values_a = _normalized_values(table_rows[candidate["table_a"]], candidate["column_a"])
        values_b = _normalized_values(table_rows[candidate["table_b"]], candidate["column_b"])

        min_size = min(len(values_a), len(values_b))
        if min_size == 0:
            continue

        if min_size < min_distinct_values:
            rejected.append(
                {
                    **candidate,
                    "intersection_size": 0,
                    "overlap_ratio": 0.0,
                    "score": 0.0,
                    "decision": "rejected",
                    "reason": "low_distinct_value_count",
                }
            )
            continue

        intersection_size = len(values_a.intersection(values_b))
        if intersection_size < min_intersection_size:
            rejected.append(
                {
                    **candidate,
                    "intersection_size": intersection_size,
                    "overlap_ratio": 0.0,
                    "score": 0.0,
                    "decision": "rejected",
                    "reason": "low_intersection_size",
                }
            )
            continue

        overlap_ratio = intersection_size / min_size

        if overlap_ratio < overlap_threshold:
            candidate_with_stats = {
                **candidate,
                "intersection_size": intersection_size,
                "overlap_ratio": round(overlap_ratio, 4),
                "score": 0.0,
                "decision": "rejected",
            }
            rejected.append(candidate_with_stats)
            continue

        uniq_a = _uniqueness_ratio(table_rows[candidate["table_a"]], candidate["column_a"])
        uniq_b = _uniqueness_ratio(table_rows[candidate["table_b"]], candidate["column_b"])
        source_table, source_column, target_table, target_column, relationship_type, pkfk_bonus = _infer_direction_and_type(
            candidate,
            uniq_a,
            uniq_b,
            pk_map,
            fk_map,
        )

        score = (
            0.25 * float(candidate["name_similarity"])
            + 0.15 * float(candidate["type_match"])
            + 0.45 * overlap_ratio
            + 0.15 * pkfk_bonus
        )

        result = {
            **candidate,
            "intersection_size": intersection_size,
            "overlap_ratio": round(overlap_ratio, 4),
            "score": round(score, 4),
            "source_table": source_table,
            "source_column": source_column,
            "target_table": target_table,
            "target_column": target_column,
            "source_uniqueness": round(uniq_a if source_table == candidate["table_a"] else uniq_b, 4),
            "target_uniqueness": round(uniq_b if target_table == candidate["table_b"] else uniq_a, 4),
            "relationship_type": relationship_type,
            "pkfk_bonus": pkfk_bonus,
        }

        if score >= confidence_threshold:
            result["decision"] = "accepted"
            accepted.append(result)
        elif borderline_low <= score < borderline_high:
            result["decision"] = "borderline"
            borderline.append(result)
        else:
            result["decision"] = "rejected"
            rejected.append(result)

    if accepted and max_relationships_per_table_pair > 0:
        accepted = _cap_relationships_per_table_pair(accepted, max_relationships_per_table_pair)

    return accepted, borderline, rejected


def _cap_relationships_per_table_pair(
    relationships: list[dict[str, Any]],
    cap: int,
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for rel in relationships:
        key = (rel["source_table"], rel["target_table"])
        grouped.setdefault(key, []).append(rel)

    capped: list[dict[str, Any]] = []
    for rels in grouped.values():
        rels_sorted = sorted(
            rels,
            key=lambda r: (float(r.get("score", 0.0)), float(r.get("overlap_ratio", 0.0)), int(r.get("intersection_size", 0))),
            reverse=True,
        )
        capped.extend(rels_sorted[:cap])

    return capped
