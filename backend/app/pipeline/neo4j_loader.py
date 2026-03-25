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

    def load_graph(self, payload: dict[str, Any]) -> None:
        if not self.enabled:
            logger.info("neo4j load skipped (loader disabled)")
            return

        nodes = payload.get("nodes", [])
        edges = payload.get("edges", [])
        logger.info("neo4j load started nodes=%s edges=%s", len(nodes), len(edges))

        with self._driver.session() as session:
            for node in nodes:
                session.run(
                    """
                    MERGE (n:GraphNode {id: $id})
                    SET n.label = $label,
                        n.entity = $entity,
                        n.data = $data
                    """,
                    id=node["id"],
                    label=node.get("label", node["id"]),
                    entity=node.get("data", {}).get("entity", "unknown"),
                    data=json.dumps(node.get("data", {})),
                )

            for edge in edges:
                session.run(
                    """
                    MATCH (a:GraphNode {id: $source})
                    MATCH (b:GraphNode {id: $target})
                    MERGE (a)-[r:GRAPH_EDGE {id: $id}]->(b)
                    SET r.label = $label,
                        r.score = $score,
                        r.columns = $columns,
                        r.relationship_type = $relationship_type,
                        r.overlap_ratio = $overlap_ratio,
                        r.edge_type = $edge_type
                    """,
                    id=edge["id"],
                    source=edge["source"],
                    target=edge["target"],
                    label=edge.get("label", ""),
                    score=float(edge.get("data", {}).get("score", 0.0)),
                    columns=json.dumps(
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
                    relationship_type=edge.get("data", {}).get("relationship_type", "unknown"),
                    overlap_ratio=float(edge.get("data", {}).get("overlap_ratio", 0.0)),
                    edge_type=edge.get("data", {}).get("edge_type", "UNKNOWN"),
                )

        logger.info("neo4j load completed")

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
