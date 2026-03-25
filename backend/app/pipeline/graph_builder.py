from __future__ import annotations

from typing import Any


def _normalize_value(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return str(value).lower()
    text = str(value).strip()
    if not text:
        return None
    return text


def _row_node_id(table_name: str, row: dict[str, Any], row_index: int) -> str:
    row_id = row.get("__row_id") or f"{table_name}:{row_index}"
    return f"row::{table_name}::{row_id}"


def build_granular_graph(
    table_rows: dict[str, list[dict[str, Any]]],
    accepted_relationships: list[dict[str, Any]],
    parent_child_links: list[tuple[str, str, str, str]],
) -> dict[str, Any]:
    nodes: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []
    edge_keys: set[tuple[str, str, str, str]] = set()
    row_lookup: dict[str, dict[str, Any]] = {}

    for table_name, rows in table_rows.items():
        for row_index, row in enumerate(rows):
            node_id = _row_node_id(table_name, row, row_index)
            row_lookup[node_id] = {
                "table": table_name,
                "row": row,
            }
            title = row.get("__row_id") or f"{table_name}:{row_index}"
            nodes.append(
                {
                    "id": node_id,
                    "label": str(title),
                    "data": {
                        "type": "row",
                        "entity": table_name,
                        "table": table_name,
                        "row_id": str(title),
                        "fields": row,
                    },
                }
            )

    for rel in accepted_relationships:
        source_table = rel["source_table"]
        target_table = rel["target_table"]
        source_column = rel["source_column"]
        target_column = rel["target_column"]
        relationship_type = rel.get("relationship_type", "unknown")

        source_rows = table_rows.get(source_table, [])
        target_rows = table_rows.get(target_table, [])
        if not source_rows or not target_rows:
            continue

        target_index: dict[str, list[str]] = {}
        for target_idx, target_row in enumerate(target_rows):
            value = _normalize_value(target_row.get(target_column))
            if value is None:
                continue
            target_id = _row_node_id(target_table, target_row, target_idx)
            target_index.setdefault(value, []).append(target_id)

        for source_idx, source_row in enumerate(source_rows):
            value = _normalize_value(source_row.get(source_column))
            if value is None:
                continue

            source_id = _row_node_id(source_table, source_row, source_idx)
            for target_id in target_index.get(value, []):
                edge_key = (source_id, target_id, source_column, target_column)
                if edge_key in edge_keys:
                    continue
                edge_keys.add(edge_key)

                edge_id = f"link::{len(edges)}"
                edges.append(
                    {
                        "id": edge_id,
                        "source": source_id,
                        "target": target_id,
                        "label": f"{source_column}={target_column}",
                        "data": {
                            "edge_type": "DATA_LINK",
                            "relationship_type": relationship_type,
                            "source_column": source_column,
                            "target_column": target_column,
                            "match_value": value,
                            "score": rel.get("score", 0.0),
                            "overlap_ratio": rel.get("overlap_ratio", 0.0),
                        },
                    }
                )

    for parent_table, parent_key, child_table, child_parent_key in parent_child_links:
        parent_rows = table_rows.get(parent_table, [])
        child_rows = table_rows.get(child_table, [])
        if not parent_rows or not child_rows:
            continue

        child_index: dict[str, list[str]] = {}
        for child_idx, child_row in enumerate(child_rows):
            value = _normalize_value(child_row.get(child_parent_key))
            if value is None:
                continue
            child_id = _row_node_id(child_table, child_row, child_idx)
            child_index.setdefault(value, []).append(child_id)

        for parent_idx, parent_row in enumerate(parent_rows):
            value = _normalize_value(parent_row.get(parent_key))
            if value is None:
                continue
            parent_id = _row_node_id(parent_table, parent_row, parent_idx)
            for child_id in child_index.get(value, []):
                edge_key = (parent_id, child_id, parent_key, child_parent_key)
                if edge_key in edge_keys:
                    continue
                edge_keys.add(edge_key)

                edge_id = f"nested::{len(edges)}"
                edges.append(
                    {
                        "id": edge_id,
                        "source": parent_id,
                        "target": child_id,
                        "label": f"{parent_key}={child_parent_key}",
                        "data": {
                            "edge_type": "NESTED_PARENT_CHILD",
                            "relationship_type": "1-many",
                            "source_column": parent_key,
                            "target_column": child_parent_key,
                            "match_value": value,
                            "score": 1.0,
                            "overlap_ratio": 1.0,
                        },
                    }
                )

    return {"nodes": nodes, "edges": edges}


def build_table_graph(
    table_rows: dict[str, list[dict[str, Any]]],
    accepted_relationships: list[dict[str, Any]],
    parent_child_links: list[tuple[str, str, str, str]],
) -> dict[str, Any]:
    nodes: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []
    edge_map: dict[tuple[str, str], dict[str, Any]] = {}

    for table_name, rows in table_rows.items():
        nodes.append(
            {
                "id": table_name,
                "label": table_name,
                "data": {
                    "entity": table_name,
                    "row_count": len(rows),
                },
            }
        )

    for rel in accepted_relationships:
        source = rel["source_table"]
        target = rel["target_table"]
        key = (source, target)
        mapping = {
            "source_column": rel["source_column"],
            "target_column": rel["target_column"],
            "score": rel["score"],
            "overlap_ratio": rel["overlap_ratio"],
            "intersection_size": rel.get("intersection_size", 0),
        }

        if key not in edge_map:
            edge_map[key] = {
                "source": source,
                "target": target,
                "relationship_type": rel.get("relationship_type", "unknown"),
                "mappings": [mapping],
                "score": rel["score"],
            }
        else:
            edge_map[key]["mappings"].append(mapping)
            edge_map[key]["score"] = max(edge_map[key]["score"], rel["score"])

    for parent_table, parent_key, child_table, child_parent_key in parent_child_links:
        key = (parent_table, child_table)
        mapping = {
            "source_column": parent_key,
            "target_column": child_parent_key,
            "score": 1.0,
            "overlap_ratio": 1.0,
            "intersection_size": len(table_rows.get(child_table, [])),
        }
        if key not in edge_map:
            edge_map[key] = {
                "source": parent_table,
                "target": child_table,
                "relationship_type": "1-many",
                "mappings": [mapping],
                "score": 1.0,
                "edge_type": "NESTED_PARENT_CHILD",
            }
        else:
            edge_map[key]["mappings"].append(mapping)
            edge_map[key]["score"] = max(edge_map[key]["score"], 1.0)

    for (source, target), edge_info in edge_map.items():
        mappings = sorted(edge_info["mappings"], key=lambda m: (m["score"], m["overlap_ratio"]), reverse=True)
        top_mappings = mappings[:3]
        label = " | ".join([f"{m['source_column']}->{m['target_column']}" for m in top_mappings])
        edge_id = f"{source}->{target}"
        edges.append(
            {
                "id": edge_id,
                "source": source,
                "target": target,
                "label": label,
                "data": {
                    "edge_type": edge_info.get("edge_type", "FK_PK_INFERRED"),
                    "relationship_type": edge_info["relationship_type"],
                    "score": edge_info["score"],
                    "mappings": top_mappings,
                    "mapping_count": len(mappings),
                    "column_a": top_mappings[0]["source_column"],
                    "column_b": top_mappings[0]["target_column"],
                    "overlap_ratio": top_mappings[0]["overlap_ratio"],
                },
            }
        )

    return {"nodes": nodes, "edges": edges}


def graph_to_payload(graph_payload: dict[str, Any]) -> dict[str, Any]:
    return graph_payload
