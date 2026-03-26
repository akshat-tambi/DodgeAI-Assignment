from __future__ import annotations

import logging
import shutil
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.config import Settings
from app.pipeline.graph_builder import build_granular_graph, build_table_graph, graph_to_payload
from app.pipeline.matcher import find_candidate_column_matches
from app.pipeline.nested import relationalize_tables
from app.pipeline.sampler import sample_tables_from_root
from app.pipeline.schema import detect_key_candidates, extract_schema_metadata
from app.pipeline.scorer import score_relationships
from app.pipeline.neo4j_loader import Neo4jGraphLoader
from app.services.groq_refiner import GroqRefiner
from app.services.job_store import JobStore

logger = logging.getLogger(__name__)


def _validate_extracted_structure(root: Path) -> None:
    folders = [p for p in root.iterdir() if p.is_dir()]
    if not folders:
        raise ValueError("ZIP must contain at least one top-level folder.")

    for folder in folders:
        jsonl_files = list(folder.glob("*.jsonl"))
        if not jsonl_files:
            raise ValueError(f"Folder '{folder.name}' must contain at least one .jsonl file.")


async def process_upload_job(
    job_store: JobStore,
    job_id: str,
    zip_path: Path,
    settings: Settings,
    neo4j_loader: Neo4jGraphLoader,
    groq_refiner: GroqRefiner,
) -> None:
    temp_extract_dir = Path(tempfile.mkdtemp(prefix=f"extract_{job_id}_"))
    events: list[dict[str, Any]] = []

    async def update_stage(
        stage: str,
        message: str,
        *,
        status: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        event: dict[str, Any] = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "stage": stage,
            "message": message,
        }
        if details:
            event["details"] = details
        events.append(event)
        await job_store.update(
            job_id,
            status=status,
            stage=stage,
            message=message,
            metadata_patch={"events": events[-300:]},
        )
        logger.info(
            "pipeline job_id=%s stage=%s message=%s details=%s",
            job_id,
            stage,
            message,
            details or {},
        )

    try:
        logger.info("pipeline start job_id=%s zip_path=%s", job_id, zip_path)
        await update_stage("wipe", "Wiping previous Neo4j graph", status="running")
        neo4j_loader.wipe_graph()

        await update_stage("unzip", "Extracting upload", details={"extract_dir": str(temp_extract_dir)})
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(temp_extract_dir)

        candidate_root = temp_extract_dir
        nested_root_dirs = [p for p in temp_extract_dir.iterdir() if p.is_dir()]
        if len(nested_root_dirs) == 1 and not list(nested_root_dirs[0].glob("*.jsonl")):
            candidate_root = nested_root_dirs[0]

        logger.info("pipeline candidate_root job_id=%s root=%s", job_id, candidate_root)
        _validate_extracted_structure(candidate_root)

        await update_stage("sampling", "Sampling table rows")
        sampled_tables = sample_tables_from_root(candidate_root, sample_size=100)
        sampled_counts = {name: len(rows) for name, rows in sampled_tables.items()}
        logger.info("pipeline sampled job_id=%s table_counts=%s", job_id, sampled_counts)

        await update_stage("nested", "Relationalizing nested structures")
        relationalized_tables, parent_child_links = relationalize_tables(sampled_tables)
        logger.info(
            "pipeline relationalized job_id=%s table_count=%s parent_child_links=%s",
            job_id,
            len(relationalized_tables),
            len(parent_child_links),
        )

        await update_stage("schema", "Extracting schema metadata")
        schemas = extract_schema_metadata(relationalized_tables)
        key_candidates = detect_key_candidates(relationalized_tables)
        logger.info(
            "pipeline schema job_id=%s schema_tables=%s",
            job_id,
            len(schemas),
        )

        await update_stage("matching", "Matching candidate columns")
        column_candidates = find_candidate_column_matches(
            schemas,
            min_name_similarity=settings.min_name_similarity,
        )
        logger.info("pipeline matching job_id=%s candidates=%s", job_id, len(column_candidates))

        await update_stage("scoring", "Scoring relationships")
        accepted, borderline, rejected = score_relationships(
            candidate_pairs=column_candidates,
            table_rows=relationalized_tables,
            key_meta=key_candidates,
            overlap_threshold=settings.overlap_threshold,
            confidence_threshold=settings.confidence_threshold,
            borderline_low=settings.borderline_low,
            borderline_high=settings.borderline_high,
            min_intersection_size=settings.min_intersection_size,
            min_distinct_values=settings.min_distinct_values,
            max_relationships_per_table_pair=settings.max_relationships_per_table_pair,
        )
        logger.info(
            "pipeline scoring job_id=%s accepted=%s borderline=%s rejected=%s min_name_similarity=%s min_intersection=%s min_distinct=%s cap_per_pair=%s",
            job_id,
            len(accepted),
            len(borderline),
            len(rejected),
            settings.min_name_similarity,
            settings.min_intersection_size,
            settings.min_distinct_values,
            settings.max_relationships_per_table_pair,
        )

        await update_stage("llm_refinement", "Verifying inferred relationships with Groq")
        inferred_for_verification = [*accepted, *borderline]
        verified_inferred = groq_refiner.verify_all(
            inferred_for_verification,
            relationalized_tables,
            schemas,
        )
        accepted = [r for r in verified_inferred if r.get("decision") == "accepted"]
        llm_rejected = [r for r in verified_inferred if r.get("decision") == "rejected"]
        rejected.extend(llm_rejected)
        borderline = [r for r in verified_inferred if r.get("decision") == "borderline"]
        logger.info(
            "pipeline llm_refinement job_id=%s verified_total=%s accepted_after_verify=%s rejected_after_verify=%s",
            job_id,
            len(verified_inferred),
            len(accepted),
            len(llm_rejected),
        )

        await update_stage("graph_build", "Building graph payload")
        table_graph_payload = build_table_graph(relationalized_tables, accepted, parent_child_links)
        table_graph_payload = graph_to_payload(table_graph_payload)
        granular_graph_payload = build_granular_graph(relationalized_tables, accepted, parent_child_links)
        logger.info(
            "pipeline graph_build job_id=%s table_nodes=%s table_edges=%s granular_nodes=%s granular_edges=%s",
            job_id,
            len(table_graph_payload.get("nodes", [])),
            len(table_graph_payload.get("edges", [])),
            len(granular_graph_payload.get("nodes", [])),
            len(granular_graph_payload.get("edges", [])),
        )

        await update_stage("neo4j_load", "Loading graph into Neo4j")
        neo4j_loader.load_graph(table_graph_payload)
        neo4j_graph = neo4j_loader.fetch_graph()
        logger.info(
            "pipeline neo4j_load job_id=%s persisted_nodes=%s persisted_edges=%s",
            job_id,
            len(neo4j_graph.get("nodes", [])),
            len(neo4j_graph.get("edges", [])),
        )

        metadata: dict[str, Any] = {
            "tables": list(relationalized_tables.keys()),
            "schemas": schemas,
            "key_candidates": key_candidates,
            "relationships": {
                "accepted": accepted,
                "borderline": borderline,
                "rejected": rejected,
                "parent_child": [
                    {
                        "parent_table": p,
                        "parent_key": pk,
                        "child_table": c,
                        "child_parent_key": ck,
                    }
                    for p, pk, c, ck in parent_child_links
                ],
            },
            "graph_table": neo4j_graph if neo4j_graph["nodes"] else table_graph_payload,
            "graph_granular": granular_graph_payload,
            "graph": granular_graph_payload,
            "events": events,
        }

        await update_stage(
            "done",
            "Processing completed",
            status="completed",
            details={
                "tables": len(relationalized_tables),
                "accepted_relationships": len(accepted),
                "graph_nodes": len(granular_graph_payload.get("nodes", [])),
                "graph_edges": len(granular_graph_payload.get("edges", [])),
            },
        )

        await job_store.update(
            job_id,
            metadata_patch=metadata,
        )
        logger.info("pipeline complete job_id=%s", job_id)
    except Exception as exc:
        logger.exception("pipeline failed job_id=%s", job_id)
        await job_store.update(
            job_id,
            status="failed",
            stage="failed",
            message="Processing failed",
            error=str(exc),
            metadata_patch={"events": events[-300:]},
        )
    finally:
        if zip_path.exists():
            logger.info("pipeline cleanup deleting_zip job_id=%s path=%s", job_id, zip_path)
            zip_path.unlink(missing_ok=True)
        if temp_extract_dir.exists():
            logger.info("pipeline cleanup deleting_extract_dir job_id=%s path=%s", job_id, temp_extract_dir)
            shutil.rmtree(temp_extract_dir, ignore_errors=True)
