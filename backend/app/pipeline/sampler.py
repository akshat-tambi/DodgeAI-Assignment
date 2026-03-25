from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


def read_jsonl_rows(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def sample_tables_from_root(root_dir: Path, sample_size: int = 100) -> dict[str, list[dict[str, Any]]]:
    logger.info("sampling start root=%s sample_size=%s", root_dir, sample_size)
    sampled: dict[str, list[dict[str, Any]]] = {}

    for table_dir in sorted([p for p in root_dir.iterdir() if p.is_dir()]):
        table_rows: list[dict[str, Any]] = []
        for jsonl_file in sorted(table_dir.glob("*.jsonl")):
            file_rows = read_jsonl_rows(jsonl_file)
            logger.info(
                "sampling table=%s file=%s rows_read=%s",
                table_dir.name,
                jsonl_file.name,
                len(file_rows),
            )
            table_rows.extend(file_rows)
            if len(table_rows) >= sample_size:
                break
        sampled[table_dir.name] = table_rows[:sample_size]
        logger.info(
            "sampling table_complete table=%s sampled_rows=%s",
            table_dir.name,
            len(sampled[table_dir.name]),
        )

    logger.info("sampling done tables=%s", len(sampled))

    return sampled
