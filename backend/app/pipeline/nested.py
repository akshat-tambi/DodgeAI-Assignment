from __future__ import annotations

from typing import Any, Dict, List, Tuple


def _is_primitive(value: Any) -> bool:
    return value is None or isinstance(value, (str, int, float, bool))


def relationalize_tables(
    table_rows: dict[str, list[dict[str, Any]]],
) -> tuple[dict[str, list[dict[str, Any]]], list[tuple[str, str, str, str]]]:
    """
    Returns:
    - flattened tables
    - parent-child relationship mapping tuples:
      (parent_table, parent_key, child_table, child_parent_key)
    """
    flat_tables: dict[str, list[dict[str, Any]]] = {k: [] for k in table_rows}
    parent_child_links: list[tuple[str, str, str, str]] = []

    for table_name, rows in table_rows.items():
        for idx, row in enumerate(rows):
            row_id = row.get("id") or row.get(f"{table_name}_id") or f"{table_name}_{idx}"
            base_row: Dict[str, Any] = {"__row_id": row_id}

            for key, value in row.items():
                if _is_primitive(value):
                    base_row[key] = value
                    continue

                child_table = f"{table_name}__{key}"
                flat_tables.setdefault(child_table, [])
                parent_key = "__row_id"
                child_parent_key = f"{table_name}_parent_id"

                if isinstance(value, dict):
                    child_row = {
                        "__row_id": f"{row_id}:{key}:0",
                        child_parent_key: row_id,
                    }
                    for k2, v2 in value.items():
                        if _is_primitive(v2):
                            child_row[k2] = v2
                        else:
                            child_row[k2] = str(v2)
                    flat_tables[child_table].append(child_row)
                elif isinstance(value, list):
                    for item_idx, item in enumerate(value):
                        if isinstance(item, dict):
                            child_row = {
                                "__row_id": f"{row_id}:{key}:{item_idx}",
                                child_parent_key: row_id,
                                "_index": item_idx,
                            }
                            for k2, v2 in item.items():
                                child_row[k2] = v2 if _is_primitive(v2) else str(v2)
                            flat_tables[child_table].append(child_row)
                        else:
                            flat_tables[child_table].append(
                                {
                                    "__row_id": f"{row_id}:{key}:{item_idx}",
                                    child_parent_key: row_id,
                                    "_index": item_idx,
                                    "value": item if _is_primitive(item) else str(item),
                                }
                            )
                else:
                    base_row[key] = str(value)

                link = (table_name, parent_key, child_table, child_parent_key)
                if link not in parent_child_links:
                    parent_child_links.append(link)

            flat_tables[table_name].append(base_row)

    return flat_tables, parent_child_links
