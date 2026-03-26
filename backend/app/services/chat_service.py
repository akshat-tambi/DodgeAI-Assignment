from __future__ import annotations

import json
import re
import uuid
from typing import Any

from groq import Groq

from app.pipeline.neo4j_loader import Neo4jGraphLoader
from app.services.job_store import JobStore

_OFF_TOPIC_MESSAGE = "This system is designed to answer questions related to the provided dataset only."
_BASE_DOMAIN_TERMS = {
    "order",
    "orders",
    "sales",
    "delivery",
    "deliveries",
    "billing",
    "invoice",
    "invoices",
    "payment",
    "payments",
    "customer",
    "customers",
    "product",
    "products",
    "material",
    "journal",
    "document",
    "flow",
    "graph",
    "table",
    "entity",
}
_BLOCKED_CYPHER = {
    "create",
    "merge",
    "delete",
    "detach",
    "set",
    "remove",
    "drop",
    "call",
    "apoc",
    "load csv",
    "dbms",
}


class ChatService:
    def __init__(
        self,
        *,
        groq_api_key: str,
        groq_model: str,
        neo4j_loader: Neo4jGraphLoader,
        job_store: JobStore,
    ) -> None:
        self.client = Groq(api_key=groq_api_key)
        self.model = groq_model
        self.neo4j_loader = neo4j_loader
        self.job_store = job_store
        self._active_job_id: str | None = None

    async def answer(
        self,
        *,
        job_id: str,
        question: str,
        conversation_id: str | None,
        selected_node_id: str | None,
    ) -> dict[str, Any]:
        conv_id = conversation_id or str(uuid.uuid4())
        state = await self.job_store.get(job_id)
        if not state:
            return {
                "job_id": job_id,
                "conversation_id": conv_id,
                "answer": "No job was found for the provided job_id.",
                "domain_allowed": True,
                "evidence": {
                    "cypher": "",
                    "row_count": 0,
                    "reasoning": "Unknown job_id",
                },
                "highlights": {"node_ids": [], "edge_ids": []},
            }

        if state.status != "completed":
            return {
                "job_id": job_id,
                "conversation_id": conv_id,
                "answer": "This upload is still processing. Wait for completion before using chat.",
                "domain_allowed": True,
                "evidence": {
                    "cypher": "",
                    "row_count": 0,
                    "reasoning": "Job not completed",
                },
                "highlights": {"node_ids": [], "edge_ids": []},
            }

        dataset_context = state.metadata.get("dataset_context", {})

        if not self._is_domain_question(question, dataset_context=dataset_context, metadata=state.metadata):
            return {
                "job_id": job_id,
                "conversation_id": conv_id,
                "answer": _OFF_TOPIC_MESSAGE,
                "domain_allowed": False,
                "evidence": {
                    "cypher": "",
                    "row_count": 0,
                    "reasoning": "Off-topic guardrail rejection",
                },
                "highlights": {"node_ids": [], "edge_ids": []},
            }

        await self._ensure_job_graph_loaded(job_id=job_id, metadata=state.metadata)

        history = await self.job_store.get_conversation(conv_id, job_id=job_id, max_turns=8)
        planner = self._plan_query(
            question=question,
            selected_node_id=selected_node_id,
            metadata=state.metadata,
            history=history,
            dataset_context=dataset_context,
        )

        planned_cypher = str(planner.get("cypher", ""))
        cypher = self._sanitize_read_only_cypher(planned_cypher)
        query_trace = self._build_query_trace(planned=planned_cypher, executed=cypher)
        if not cypher:
            return {
                "job_id": job_id,
                "conversation_id": conv_id,
                "answer": "I could not produce a safe graph query for that request. Please rephrase with dataset entities.",
                "domain_allowed": True,
                "evidence": {
                    "cypher": "",
                    "row_count": 0,
                    "reasoning": "Planner failed to produce a safe read-only query",
                    "queries": query_trace,
                },
                "highlights": {"node_ids": [], "edge_ids": []},
            }

        rows = self.neo4j_loader.run_read_query(cypher)
        query_contract = planner.get("_query_contract") if isinstance(planner.get("_query_contract"), dict) else {}
        if not query_contract:
            query_contract = self._derive_query_contract(
                question=question,
                tables=state.metadata.get("tables", []),
                table_columns={
                    str(table): [str(c.get("name")) for c in (spec.get("columns", [])[:20])]
                    for table, spec in (state.metadata.get("schemas", {}) or {}).items()
                    if isinstance(spec, dict)
                },
                relationships=[
                    {
                        "source_table": r.get("source_table"),
                        "source_column": r.get("source_column"),
                        "target_table": r.get("target_table"),
                        "target_column": r.get("target_column"),
                        "relationship_type": r.get("relationship_type"),
                        "score": r.get("score"),
                    }
                    for r in (state.metadata.get("relationships", {}).get("accepted", [])[:40])
                ],
                schema_profile=self._derive_schema_profile(state.metadata),
            )

        if self._is_degenerate_aggregation_result(query_contract=query_contract, rows=rows):
            repaired_cypher = self._repair_query_from_results(
                question=question,
                current_cypher=cypher,
                rows=rows,
                metadata=state.metadata,
                query_contract=query_contract,
            )
            repaired_safe = self._sanitize_read_only_cypher(repaired_cypher)
            if repaired_safe and repaired_safe != cypher:
                repaired_rows = self.neo4j_loader.run_read_query(repaired_safe)
                if repaired_rows and not self._is_degenerate_aggregation_result(query_contract=query_contract, rows=repaired_rows):
                    cypher = repaired_safe
                    rows = repaired_rows
                    query_trace.append({"stage": "repaired_execution", "cypher": repaired_safe})

        analysis_rows = rows

        if self._should_enrich_with_links(question=question, cypher=cypher, rows=rows):
            enrichment_cypher = self._build_link_enrichment_query(cypher)
            if enrichment_cypher:
                linked_rows = self.neo4j_loader.run_read_query(enrichment_cypher)
                if linked_rows:
                    analysis_rows = linked_rows
                    query_trace.append({"stage": "enrichment", "cypher": enrichment_cypher})

        answer = self._synthesize_answer(
            question=question,
            rows=analysis_rows,
            planner_reasoning=str(planner.get("reasoning", ""))[:500],
        )
        highlights = self._extract_highlights(analysis_rows)

        await self.job_store.append_conversation_turn(
            conv_id,
            job_id=job_id,
            user_message=question,
            assistant_message=answer,
        )

        return {
            "job_id": job_id,
            "conversation_id": conv_id,
            "answer": answer,
            "domain_allowed": True,
            "evidence": {
                "cypher": cypher,
                "row_count": len(analysis_rows),
                "reasoning": str(planner.get("reasoning", ""))[:500],
                "queries": query_trace,
            },
            "highlights": highlights,
        }

    def _build_query_trace(self, *, planned: str, executed: str) -> list[dict[str, str]]:
        trace: list[dict[str, str]] = []
        planned_text = (planned or "").strip()
        executed_text = (executed or "").strip()

        if planned_text:
            trace.append({"stage": "planned", "cypher": planned_text})

        if executed_text:
            stage = "executed" if planned_text and executed_text != planned_text else "planned"
            if not trace or trace[-1]["cypher"] != executed_text:
                trace.append({"stage": stage, "cypher": executed_text})

        return trace

    def _should_enrich_with_links(self, *, question: str, cypher: str, rows: list[dict[str, Any]]) -> bool:
        if not rows:
            return False

        lowered_cypher = (cypher or "").lower()
        if "graph_edge" in lowered_cypher:
            return False

        if not self._rows_are_node_only(rows):
            return False

        q = (question or "").lower()
        return any(token in q for token in ("analysis", "analyze", "analyse", "link", "relationship", "related"))

    def _rows_are_node_only(self, rows: list[dict[str, Any]]) -> bool:
        allowed = {"id", "label", "entity", "data"}
        checked = 0
        for row in rows[:5]:
            if not isinstance(row, dict):
                continue
            keys = {str(k).lower() for k in row.keys()}
            if not keys or not keys.issubset(allowed):
                return False
            checked += 1
        return checked > 0

    def _build_link_enrichment_query(self, cypher: str) -> str:
        text = (cypher or "").strip()
        pattern = re.compile(
            r"^MATCH\s*\(\s*(?P<var>[A-Za-z_]\w*)\s*:\s*GraphNode\s*\)\s*"
            r"WHERE\s*(?P<where>.+?)\s*"
            r"RETURN\s+.+?(?:\s+LIMIT\s+(?P<limit>\d+))?\s*$",
            re.IGNORECASE | re.DOTALL,
        )
        match = pattern.match(text)
        if not match:
            return ""

        var_name = match.group("var")
        where_clause = (match.group("where") or "").strip()
        if not where_clause:
            return ""

        raw_limit = match.group("limit")
        limit = 50
        if raw_limit:
            try:
                limit = max(1, min(int(raw_limit), 50))
            except Exception:
                limit = 50

        return (
            f"MATCH ({var_name}:GraphNode) "
            f"WHERE {where_clause} "
            f"OPTIONAL MATCH ({var_name})-[r:GRAPH_EDGE]-(m:GraphNode) "
            "RETURN DISTINCT "
            f"{var_name}.id AS source_id, {var_name}.label AS source_label, {var_name}.entity AS source_entity, {var_name}.data AS source_data, "
            "r.id AS edge_id, r.label AS edge_label, r.relationship_type AS edge_type, r.score AS edge_score, r.columns AS edge_columns, "
            "m.id AS target_id, m.label AS target_label, m.entity AS target_entity, m.data AS target_data "
            f"LIMIT {limit}"
        )

    async def _ensure_job_graph_loaded(self, *, job_id: str, metadata: dict[str, Any]) -> None:
        if self._active_job_id == job_id:
            return

        active_job_id = self.neo4j_loader.get_active_job_id()
        if active_job_id and active_job_id == job_id:
            self._active_job_id = job_id
            return

        payload = metadata.get("graph_granular") or metadata.get("graph")
        if not isinstance(payload, dict):
            self._active_job_id = None
            return

        self.neo4j_loader.wipe_graph()
        self.neo4j_loader.load_graph(payload, job_id=job_id)
        self._active_job_id = job_id

    def _is_domain_question(self, question: str, *, dataset_context: dict[str, Any], metadata: dict[str, Any]) -> bool:
        q = question.lower()
        dynamic_terms = self._collect_domain_terms(dataset_context=dataset_context, metadata=metadata)
        return any(term in q for term in dynamic_terms)

    def _plan_query(
        self,
        *,
        question: str,
        selected_node_id: str | None,
        metadata: dict[str, Any],
        history: list[dict[str, Any]],
        dataset_context: dict[str, Any],
    ) -> dict[str, Any]:
        tables = metadata.get("tables", [])[:40]
        schemas = metadata.get("schemas", {})
        table_columns = {
            str(table): [str(c.get("name")) for c in (spec.get("columns", [])[:20])]
            for table, spec in schemas.items()
            if isinstance(spec, dict)
        }
        known_columns: list[str] = []
        for cols in table_columns.values():
            for col in cols:
                if col and col not in known_columns:
                    known_columns.append(col)
                if len(known_columns) >= 80:
                    break
            if len(known_columns) >= 80:
                break
        accepted = metadata.get("relationships", {}).get("accepted", [])[:40]
        compact_relationships = [
            {
                "source_table": r.get("source_table"),
                "source_column": r.get("source_column"),
                "target_table": r.get("target_table"),
                "target_column": r.get("target_column"),
                "relationship_type": r.get("relationship_type"),
                "score": r.get("score"),
            }
            for r in accepted
        ]

        schema_profile = self._derive_schema_profile(metadata)
        query_contract = self._derive_query_contract(
            question=question,
            tables=tables,
            table_columns=table_columns,
            relationships=compact_relationships,
            schema_profile=schema_profile,
        )

        prompt = {
            "task": "Generate one safe read-only Cypher query for the business question.",
            "constraints": [
                "Domain: uploaded Order-to-Cash dataset only.",
                "Graph model uses (n:GraphNode) and [r:GRAPH_EDGE].",
                "Node fields: id, label, entity, data (JSON string).",
                "Never use dynamic table labels; always use (:GraphNode).",
                "Filter table/entity via n.entity = '<table_name>'.",
                "Row-level columns live inside n.data JSON string.",
                "For column equality lookups, filter using a regex on n.data that allows optional spaces and optional quotes around the value.",
                "Edge fields: id, label, relationship_type, score, columns, edge_type.",
                "Only read-only query allowed.",
                "Always include LIMIT <= 50.",
                "No APOC, no CALL, no write clauses.",
                "Prefer RETURN projections: n.id AS id, n.label AS label, n.entity AS entity, n.data AS data.",
                "Output JSON only.",
            ],
            "intent_guidance": [
                "If the user asks details for a field-value lookup, build regex on n.data for that exact field and value.",
                "If user asks for links/relationships around a field-value lookup, include OPTIONAL MATCH (n)-[r:GRAPH_EDGE]-(m:GraphNode) and return linked node/edge columns.",
                "Prefer fields from known_columns and tables from tables when possible.",
                "For regex on n.data, ensure full-string match works by including leading and trailing .* around the key/value pattern.",
                "If asked for highest/top entities, use aggregation with COUNT or SUM, ORDER BY descending, and return ranked rows.",
                "If asked to trace a document flow across lifecycle stages, use chained OPTIONAL MATCH across related nodes and return stage-wise entities/ids.",
                "If asked to find broken or incomplete flows, use OPTIONAL MATCH and filter missing hops with IS NULL conditions.",
                "When links are requested, include edge metadata (relationship_type, columns, score) to ground the answer.",
                "Infer lifecycle stages from schema_profile.table_name_signals and relationships rather than relying on fixed business labels.",
            ],
            "selected_node_id": selected_node_id,
            "question": question,
            "conversation_history": history,
            "dataset_context": dataset_context,
            "tables": tables,
            "table_columns": table_columns,
            "known_columns": known_columns,
            "schema_profile": schema_profile,
            "query_contract": query_contract,
            "relationships": compact_relationships,
            "response_schema": {
                "cypher": "string",
                "reasoning": "string_max_120_words",
            },
        }

        try:
            completion = self.client.chat.completions.create(
                model=self.model,
                temperature=0,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a strict Cypher planner. Return JSON only.",
                    },
                    {
                        "role": "user",
                        "content": json.dumps(prompt, separators=(",", ":")),
                    },
                ],
            )
            content = completion.choices[0].message.content or "{}"
            parsed = self._parse_json(content)
            if isinstance(parsed, dict):
                candidate_cypher = str(parsed.get("cypher", "")).strip()
                if candidate_cypher and not self._cypher_satisfies_intent(candidate_cypher, query_contract):
                    repaired = self._repair_query_plan(
                        question=question,
                        candidate_plan=parsed,
                        query_contract=query_contract,
                        tables=tables,
                        table_columns=table_columns,
                        relationships=compact_relationships,
                        schema_profile=schema_profile,
                    )
                    if isinstance(repaired, dict) and str(repaired.get("cypher", "")).strip():
                        repaired_cypher = str(repaired.get("cypher", "")).strip()
                        if self._cypher_satisfies_intent(repaired_cypher, query_contract):
                            repaired["_query_contract"] = query_contract
                            return repaired
                parsed["_query_contract"] = query_contract
                return parsed
            return {}
        except Exception:
            return {}

    def _derive_query_contract(
        self,
        *,
        question: str,
        tables: list[Any],
        table_columns: dict[str, list[str]],
        relationships: list[dict[str, Any]],
        schema_profile: dict[str, Any],
    ) -> dict[str, Any]:
        prompt = {
            "task": "Infer query intent contract from user question and dataset schema context.",
            "question": question,
            "schema_profile": schema_profile,
            "tables": tables,
            "table_columns": table_columns,
            "relationships": relationships,
            "rules": [
                "Do not rely on fixed business keywords.",
                "Use schema and relationship context to infer whether aggregation/traversal/missingness logic is required.",
                "Return JSON only.",
            ],
            "response_schema": {
                "needs_aggregation": "boolean",
                "needs_relationship_traversal": "boolean",
                "needs_missingness_logic": "boolean",
                "expects_ranked_output": "boolean",
            },
        }

        try:
            completion = self.client.chat.completions.create(
                model=self.model,
                temperature=0,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a strict intent contract generator. Return JSON only.",
                    },
                    {
                        "role": "user",
                        "content": json.dumps(prompt, separators=(",", ":")),
                    },
                ],
            )
            parsed = self._parse_json(completion.choices[0].message.content or "{}")
            if isinstance(parsed, dict):
                return {
                    "needs_aggregation": bool(parsed.get("needs_aggregation", False)),
                    "needs_relationship_traversal": bool(parsed.get("needs_relationship_traversal", False)),
                    "needs_missingness_logic": bool(parsed.get("needs_missingness_logic", False)),
                    "expects_ranked_output": bool(parsed.get("expects_ranked_output", False)),
                }
        except Exception:
            pass

        return {
            "needs_aggregation": False,
            "needs_relationship_traversal": False,
            "needs_missingness_logic": False,
            "expects_ranked_output": False,
        }

    def _cypher_satisfies_intent(self, cypher: str, query_contract: dict[str, Any]) -> bool:
        text = (cypher or "").lower()

        if query_contract.get("needs_aggregation"):
            has_agg = any(token in text for token in ("count(", "sum(", "avg(", "min(", "max("))
            has_rank = "order by" in text if query_contract.get("expects_ranked_output") else True
            if not (has_agg and has_rank):
                return False

        if query_contract.get("needs_relationship_traversal"):
            has_rel = "graph_edge" in text or "-[" in text
            if not has_rel:
                return False

        if query_contract.get("needs_missingness_logic"):
            has_missing_logic = any(token in text for token in (" is null", " is not null", "coalesce(", "case "))
            if not has_missing_logic:
                return False

        return True

    def _repair_query_plan(
        self,
        *,
        question: str,
        candidate_plan: dict[str, Any],
        query_contract: dict[str, Any],
        tables: list[Any],
        table_columns: dict[str, list[str]],
        relationships: list[dict[str, Any]],
        schema_profile: dict[str, Any],
    ) -> dict[str, Any]:
        prompt = {
            "task": "Revise the Cypher so it satisfies query intent while remaining safe/read-only.",
            "question": question,
            "query_contract": query_contract,
            "current_plan": {
                "cypher": str(candidate_plan.get("cypher", "")),
                "reasoning": str(candidate_plan.get("reasoning", "")),
            },
            "constraints": [
                "Graph model uses (n:GraphNode) and [r:GRAPH_EDGE].",
                "Use n.entity for table filtering; row fields live in n.data JSON.",
                "Read-only query only. No APOC/CALL/write clauses.",
                "LIMIT <= 50.",
                "Return JSON only with keys cypher and reasoning.",
            ],
            "schema_profile": schema_profile,
            "tables": tables,
            "table_columns": table_columns,
            "relationships": relationships,
            "response_schema": {
                "cypher": "string",
                "reasoning": "string_max_120_words",
            },
        }

        try:
            completion = self.client.chat.completions.create(
                model=self.model,
                temperature=0,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a strict Cypher planner repairer. Return JSON only.",
                    },
                    {
                        "role": "user",
                        "content": json.dumps(prompt, separators=(",", ":")),
                    },
                ],
            )
            content = completion.choices[0].message.content or "{}"
            parsed = self._parse_json(content)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}

    def _is_degenerate_aggregation_result(self, *, query_contract: dict[str, Any], rows: list[dict[str, Any]]) -> bool:
        if not query_contract.get("needs_aggregation"):
            return False
        if not rows:
            return False

        numeric_keys: set[str] = set()
        sample = [row for row in rows[:20] if isinstance(row, dict)]
        if not sample:
            return False

        for key in sample[0].keys():
            values = [r.get(key) for r in sample]
            numeric_values = [v for v in values if isinstance(v, (int, float)) and not isinstance(v, bool)]
            if len(numeric_values) >= max(2, int(0.6 * len(sample))):
                key_l = str(key).lower()
                if key_l not in {"id"} and not key_l.endswith("_id"):
                    numeric_keys.add(str(key))

        if not numeric_keys:
            return False

        spreads = []
        for key in numeric_keys:
            vals = [row.get(key) for row in sample if isinstance(row.get(key), (int, float))]
            if vals:
                spreads.append(max(vals) - min(vals))
        return bool(spreads) and all(spread == 0 for spread in spreads)

    def _repair_query_from_results(
        self,
        *,
        question: str,
        current_cypher: str,
        rows: list[dict[str, Any]],
        metadata: dict[str, Any],
        query_contract: dict[str, Any],
    ) -> str:
        tables = metadata.get("tables", [])[:60]
        schemas = metadata.get("schemas", {})
        table_columns = {
            str(table): [str(c.get("name")) for c in (spec.get("columns", [])[:30])]
            for table, spec in schemas.items()
            if isinstance(spec, dict)
        }
        accepted = metadata.get("relationships", {}).get("accepted", [])[:80]
        relationships = [
            {
                "source_table": r.get("source_table"),
                "source_column": r.get("source_column"),
                "target_table": r.get("target_table"),
                "target_column": r.get("target_column"),
                "relationship_type": r.get("relationship_type"),
                "score": r.get("score"),
            }
            for r in accepted
        ]
        schema_profile = self._derive_schema_profile(metadata)

        prompt = {
            "task": "Repair Cypher when aggregation results are degenerate while staying fully dataset-grounded.",
            "question": question,
            "query_contract": query_contract,
            "current_cypher": current_cypher,
            "result_signal": {
                "row_count": len(rows),
                "sample_rows": rows[:5],
                "issue": "Aggregation metric appears degenerate (all zeros).",
            },
            "constraints": [
                "Use only (n:GraphNode) and [r:GRAPH_EDGE] patterns.",
                "Use n.entity for table/entity filtering and n.data for row-level fields.",
                "For ranking questions, ensure ORDER BY on a meaningful aggregate metric.",
                "Do not hardcode business constants; infer joins from relationships and table_columns.",
                "Read-only Cypher only. LIMIT <= 50.",
                "Return JSON only: {cypher, reasoning}.",
            ],
            "tables": tables,
            "table_columns": table_columns,
            "relationships": relationships,
            "schema_profile": schema_profile,
            "response_schema": {
                "cypher": "string",
                "reasoning": "string_max_120_words",
            },
        }

        try:
            completion = self.client.chat.completions.create(
                model=self.model,
                temperature=0,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a strict Cypher planner repairer. Return JSON only.",
                    },
                    {
                        "role": "user",
                        "content": json.dumps(prompt, separators=(",", ":")),
                    },
                ],
            )
            parsed = self._parse_json(completion.choices[0].message.content or "{}")
            if isinstance(parsed, dict):
                return str(parsed.get("cypher", ""))
            return ""
        except Exception:
            return ""

    def _derive_schema_profile(self, metadata: dict[str, Any]) -> dict[str, Any]:
        tables = [str(t).strip().lower() for t in metadata.get("tables", []) if str(t).strip()]
        accepted = metadata.get("relationships", {}).get("accepted", [])

        degree: dict[str, int] = {}
        role_signals: dict[str, list[str]] = {}

        for table in tables:
            degree[table] = 0
            parts = [part for part in re.split(r"[^a-z0-9]+", table) if part]
            # Keep only informative substrings and avoid one-off noise.
            signals = [p for p in parts if len(p) >= 3]
            if signals:
                role_signals[table] = signals[:8]

        for rel in accepted[:200]:
            source = str(rel.get("source_table") or "").strip().lower()
            target = str(rel.get("target_table") or "").strip().lower()
            if source:
                degree[source] = degree.get(source, 0) + 1
            if target:
                degree[target] = degree.get(target, 0) + 1

        top_tables = sorted(degree.items(), key=lambda x: x[1], reverse=True)[:15]
        return {
            "table_count": len(tables),
            "top_connected_tables": [name for name, _ in top_tables],
            "table_name_signals": role_signals,
        }

    def _collect_domain_terms(self, *, dataset_context: dict[str, Any], metadata: dict[str, Any]) -> set[str]:
        terms = set(_BASE_DOMAIN_TERMS)
        for table in metadata.get("tables", [])[:120]:
            terms.add(str(table).strip().lower())

        for key in ("domain_terms", "entity_terms", "process_terms"):
            values = dataset_context.get(key, [])
            if not isinstance(values, list):
                continue
            for item in values:
                term = str(item or "").strip().lower()
                if term:
                    terms.add(term)

        return terms

    def _synthesize_answer(self, *, question: str, rows: list[dict[str, Any]], planner_reasoning: str) -> str:
        compact_rows = rows[:30]
        prompt = {
            "task": "Answer the user question using query results only.",
            "rules": [
                "Do not invent facts.",
                "If result rows are empty, say no matching records were found.",
                "Keep answer concise and data-backed.",
            ],
            "question": question,
            "planner_reasoning": planner_reasoning,
            "row_count": len(rows),
            "rows": compact_rows,
        }
        try:
            completion = self.client.chat.completions.create(
                model=self.model,
                temperature=0,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a data analyst. Use only provided rows.",
                    },
                    {
                        "role": "user",
                        "content": json.dumps(prompt, separators=(",", ":")),
                    },
                ],
            )
            answer = (completion.choices[0].message.content or "").strip()
            if answer:
                low = answer.lower()
                has_no_match_claim = any(
                    token in low
                    for token in (
                        "no matching",
                        "no records",
                        "not found",
                        "no result",
                    )
                )
                if rows and has_no_match_claim:
                    return self._deterministic_rows_summary(rows)
                if not rows and not has_no_match_claim:
                    return "No matching records were found for this question in the current dataset graph."
                return answer
        except Exception:
            pass

        if not rows:
            return "No matching records were found for this question in the current dataset graph."
        return self._deterministic_rows_summary(rows)

    def _deterministic_rows_summary(self, rows: list[dict[str, Any]]) -> str:
        preview = rows[:3]
        return f"Found {len(rows)} matching records. Sample: {json.dumps(preview, ensure_ascii=True)}"

    def _sanitize_read_only_cypher(self, cypher: str) -> str:
        query = (cypher or "").strip()
        if not query:
            return ""

        rewritten = self._rewrite_legacy_node_lookup_query(query)
        if rewritten:
            query = rewritten

        query = self._normalize_data_regex_predicates(query)

        lowered = re.sub(r"\s+", " ", query.lower())
        if ";" in query:
            return ""
        if not ("match " in lowered or lowered.startswith("match")):
            return ""
        if " return " not in lowered and not lowered.endswith(" return"):
            return ""
        if any(token in lowered for token in _BLOCKED_CYPHER):
            return ""

        if " limit " not in lowered:
            query = f"{query} LIMIT 50"
        return query

    def _normalize_data_regex_predicates(self, query: str) -> str:
        def repl(match: re.Match[str]) -> str:
            raw = match.group("pattern")
            if not raw:
                return match.group(0)

            canonical = self._canonicalize_json_key_value_regex(raw)
            if canonical:
                return f"n.data =~ '{canonical}'"

            pattern = raw
            prefix = ""
            if pattern.startswith("(?i)"):
                prefix = "(?i)"
                pattern = pattern[4:]

            normalized = f"{prefix}.*{pattern}.*"
            return f"n.data =~ '{normalized}'"

        return re.sub(r"n\.data\s*=~\s*'(?P<pattern>[^']*)'", repl, query, flags=re.IGNORECASE)

    def _canonicalize_json_key_value_regex(self, raw_pattern: str) -> str | None:
        raw = (raw_pattern or "").strip()
        if not raw:
            return None

        # Drop common regex prefixes/suffixes to inspect the JSON key:value intent.
        core = raw
        core = re.sub(r"^\(\?[isx-]+\)", "", core, flags=re.IGNORECASE)
        core = re.sub(r"^\.\*", "", core)
        core = re.sub(r"\.\*$", "", core)
        core = core.replace(r'\"', '"')

        m = re.search(r'"(?P<key>[A-Za-z_][A-Za-z0-9_]*)"\s*:\s*"?(?P<value>[^"\s][^"]*?)"?$', core)
        if not m:
            return None

        key = m.group("key").strip()
        value = m.group("value").strip()
        if not key or not value:
            return None

        # Only canonicalize literal value lookups, not explicit regex-heavy patterns.
        if re.search(r"[\[\]{}()|+*?]", value):
            return None

        key_re = re.escape(key)
        val_re = re.escape(value)
        return f"(?is).*\\\"{key_re}\\\"\\s*:\\s*\\\"?{val_re}\\\"?.*"

    def _rewrite_legacy_node_lookup_query(self, query: str) -> str | None:
        text = (query or "").strip()
        pattern = re.compile(
            r"^MATCH\s*\(\s*(?P<var>[A-Za-z_]\w*)\s*:\s*(?P<label>[A-Za-z_]\w*)\s*\)\s*"
            r"WHERE\s*(?P=var)\.(?P<prop>[A-Za-z_]\w*)\s*=\s*(?P<value>'[^']*'|\"[^\"]*\"|[-+]?\d+(?:\.\d+)?)\s*"
            r"RETURN\s+(?P=var)\b(?:\s+LIMIT\s+(?P<limit>\d+))?\s*$",
            re.IGNORECASE,
        )
        match = pattern.match(text)
        if not match:
            return None

        table_label = match.group("label")
        prop = match.group("prop")
        raw_value = match.group("value")
        raw_limit = match.group("limit")

        if raw_value.startswith(("'", '"')) and raw_value.endswith(("'", '"')):
            value = raw_value[1:-1]
        else:
            value = raw_value

        limit = 50
        if raw_limit:
            try:
                limit = max(1, min(int(raw_limit), 50))
            except Exception:
                limit = 50

        def _esc_single(value_text: str) -> str:
            return value_text.replace("'", "''")

        regex_value = re.escape(str(value))
        regex = f"(?is).*\\\"{re.escape(prop)}\\\"\\s*:\\s*\\\"?{regex_value}\\\"?.*"

        return (
            "MATCH (n:GraphNode) "
            f"WHERE n.entity = '{_esc_single(table_label)}' "
            f"AND n.data =~ '{_esc_single(regex)}' "
            "RETURN n.id AS id, n.label AS label, n.entity AS entity, n.data AS data "
            f"LIMIT {limit}"
        )

    def _extract_highlights(self, rows: list[dict[str, Any]]) -> dict[str, list[str]]:
        node_ids: list[str] = []
        edge_ids: list[str] = []

        def add_unique(target: list[str], value: str, max_items: int = 40) -> None:
            if value and value not in target and len(target) < max_items:
                target.append(value)

        for row in rows[:120]:
            if not isinstance(row, dict):
                continue
            for key, value in row.items():
                key_l = key.lower()
                if isinstance(value, dict):
                    maybe_id = value.get("id")
                    if isinstance(maybe_id, str):
                        if "edge" in key_l:
                            add_unique(edge_ids, maybe_id)
                        else:
                            add_unique(node_ids, maybe_id)
                elif isinstance(value, str):
                    if key_l in {"id", "node_id", "source", "target", "from", "to", "entity"}:
                        add_unique(node_ids, value)
                    if key_l in {"edge_id", "relationship_id"}:
                        add_unique(edge_ids, value)

        return {"node_ids": node_ids, "edge_ids": edge_ids}

    @staticmethod
    def _parse_json(raw: str) -> dict[str, Any]:
        text = (raw or "").strip()
        if text.startswith("```"):
            text = text.strip("`")
            if text.lower().startswith("json"):
                text = text[4:]
        text = text.strip()
        try:
            return json.loads(text)
        except Exception:
            start = text.find("{")
            end = text.rfind("}")
            if start >= 0 and end > start:
                try:
                    return json.loads(text[start : end + 1])
                except Exception:
                    return {}
            return {}
