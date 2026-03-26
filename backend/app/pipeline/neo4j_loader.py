from __future__ import annotations

import json
import logging
from typing import Any

from neo4j import GraphDatabase

logger = logging.getLogger(__name__)


class Neo4jGraphLoader:
    def __init__(self, uri: str, username: str, password: str) -> None:
        self.enabled = bool(uri and username and password)
        self._driver = GraphDatabase.driver(uri, auth=(username, password)) if self.enabled else None
        if self.enabled:
            logger.info("neo4j loader enabled uri=%s user=%s", uri, username)
        else:
            logger.warning("neo4j loader disabled due to missing credentials")

    def close(self) -> None:
        if self._driver is not None:
            self._driver.close()

    def wipe_graph(self) -> None:
        if not self.enabled:
            logger.info("neo4j wipe skipped (loader disabled)")
            return
        logger.info("neo4j wipe started")
        with self._driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
        logger.info("neo4j wipe completed")

    def load_graph(self, payload: dict[str, Any], *, job_id: str | None = None) -> None:
        if not self.enabled:
            logger.info("neo4j load skipped (loader disabled)")
            return

        nodes = payload.get("nodes", [])
        edges = payload.get("edges", [])
        logger.info("neo4j load started nodes=%s edges=%s", len(nodes), len(edges))

        def _chunks(items: list[dict[str, Any]], size: int) -> list[list[dict[str, Any]]]:
            return [items[i : i + size] for i in range(0, len(items), size)]

        node_rows = [
            {
                "id": node["id"],
                "label": node.get("label", node["id"]),
                "entity": node.get("data", {}).get("entity", "unknown"),
                "data": json.dumps(node.get("data", {})),
            }
            for node in nodes
            if node.get("id")
        ]

        edge_rows = [
            {
                "id": edge["id"],
                "source": edge["source"],
                "target": edge["target"],
                "label": edge.get("label", ""),
                "score": float(edge.get("data", {}).get("score", 0.0)),
                "columns": json.dumps(
                    edge.get("data", {}).get(
                        "mappings",
                        [
                            {
                                "source_column": edge.get("data", {}).get("column_a"),
                                "target_column": edge.get("data", {}).get("column_b"),
                            }
                        ],
                    )
                ),
                "relationship_type": edge.get("data", {}).get("relationship_type", "unknown"),
                "overlap_ratio": float(edge.get("data", {}).get("overlap_ratio", 0.0)),
                "edge_type": edge.get("data", {}).get("edge_type", "UNKNOWN"),
            }
            for edge in edges
            if edge.get("id") and edge.get("source") and edge.get("target")
        ]

        with self._driver.session() as session:
            node_batches = _chunks(node_rows, 500)
            for idx, batch in enumerate(node_batches, start=1):
                session.run(
                    """
                    UNWIND $rows AS row
                    MERGE (n:GraphNode {id: row.id})
                    SET n.label = row.label,
                        n.entity = row.entity,
                        n.data = row.data
                    """,
                    rows=batch,
                )
                logger.info("neo4j load nodes batch=%s/%s size=%s", idx, len(node_batches), len(batch))

            edge_batches = _chunks(edge_rows, 500)
            for idx, batch in enumerate(edge_batches, start=1):
                session.run(
                    """
                    UNWIND $rows AS row
                    MATCH (a:GraphNode {id: row.source})
                    MATCH (b:GraphNode {id: row.target})
                    MERGE (a)-[r:GRAPH_EDGE {id: row.id}]->(b)
                    SET r.label = row.label,
                        r.score = row.score,
                        r.columns = row.columns,
                        r.relationship_type = row.relationship_type,
                        r.overlap_ratio = row.overlap_ratio,
                        r.edge_type = row.edge_type
                    """,
                    rows=batch,
                )
                logger.info("neo4j load edges batch=%s/%s size=%s", idx, len(edge_batches), len(batch))

            if job_id:
                session.run(
                    """
                    MERGE (m:GraphMeta {key: 'active'})
                    SET m.job_id = $job_id,
                        m.updated_at = timestamp()
                    """,
                    job_id=job_id,
                )
                logger.info("neo4j active graph marker set job_id=%s", job_id)

        logger.info("neo4j load completed")

    def get_active_job_id(self) -> str | None:
        if not self.enabled:
            return None

        with self._driver.session() as session:
            has_label = session.run(
                "CALL db.labels() YIELD label WHERE label = 'GraphMeta' RETURN label LIMIT 1"
            ).single()
            if not has_label:
                return None

            record = session.run(
                "MATCH (m:GraphMeta {key: 'active'}) RETURN m.job_id AS job_id LIMIT 1"
            ).single()
            if not record:
                return None
            value = record.get("job_id")
            return str(value) if value else None

    def fetch_graph(self) -> dict[str, Any]:
        if not self.enabled:
            logger.info("neo4j fetch skipped (loader disabled)")
            return {"nodes": [], "edges": []}

        with self._driver.session() as session:
            node_result = session.run(
                "MATCH (n:GraphNode) RETURN n.id AS id, n.label AS label, n.entity AS entity, n.data AS data"
            )
            edge_result = session.run(
                """
                MATCH (a:GraphNode)-[r:GRAPH_EDGE]->(b:GraphNode)
                RETURN r.id AS id, a.id AS source, b.id AS target, r.label AS label,
                       r.score AS score, r.columns AS columns, r.relationship_type AS relationship_type,
                       r.overlap_ratio AS overlap_ratio, r.edge_type AS edge_type
                """
            )

            nodes = []
            edges = []

            for rec in node_result:
                data = {}
                raw_data = rec.get("data")
                if raw_data:
                    try:
                        data = json.loads(raw_data)
                    except Exception:
                        data = {}
                data["entity"] = rec.get("entity", "unknown")
                nodes.append(
                    {
                        "id": rec["id"],
                        "label": rec.get("label") or rec["id"],
                        "data": data,
                    }
                )

            for rec in edge_result:
                extra: dict[str, Any] = {}
                raw_cols = rec.get("columns")
                if raw_cols:
                    try:
                        parsed_cols = json.loads(raw_cols)
                        # `r.columns` can be either a list of mappings (new format)
                        # or a dict-like payload (legacy/defensive handling).
                        if isinstance(parsed_cols, list):
                            extra["mappings"] = parsed_cols
                        elif isinstance(parsed_cols, dict):
                            extra.update(parsed_cols)
                    except Exception:
                        extra = {}
                extra["score"] = rec.get("score", 0.0)
                extra["relationship_type"] = rec.get("relationship_type", "unknown")
                extra["overlap_ratio"] = rec.get("overlap_ratio", 0.0)
                extra["edge_type"] = rec.get("edge_type", "UNKNOWN")
                edges.append(
                    {
                        "id": rec["id"],
                        "source": rec["source"],
                        "target": rec["target"],
                        "label": rec.get("label", ""),
                        "data": extra,
                    }
                )

            logger.info("neo4j fetch completed nodes=%s edges=%s", len(nodes), len(edges))
            return {"nodes": nodes, "edges": edges}

    def run_read_query(self, cypher: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        if not self.enabled:
            logger.info("neo4j read query skipped (loader disabled)")
            return []

        def _normalize(value: Any) -> Any:
            if isinstance(value, (str, int, float, bool)) or value is None:
                return value
            if isinstance(value, list):
                return [_normalize(v) for v in value]
            if isinstance(value, dict):
                return {str(k): _normalize(v) for k, v in value.items()}

            # neo4j graph objects expose mapping-like properties and optional ids.
            properties = getattr(value, "_properties", None)
            if isinstance(properties, dict):
                normalized: dict[str, Any] = {str(k): _normalize(v) for k, v in properties.items()}
                element_id = getattr(value, "element_id", None)
                if element_id is not None:
                    normalized.setdefault("element_id", str(element_id))
                return normalized

            try:
                return str(value)
            except Exception:
                return None

        with self._driver.session() as session:
            result = session.run(cypher, params or {})
            rows: list[dict[str, Any]] = []
            for record in result:
                rows.append({str(k): _normalize(v) for k, v in record.data().items()})
            return rows
