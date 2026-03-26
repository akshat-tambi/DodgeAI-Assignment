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

    async def answer(
        self,
        *,
        question: str,
        conversation_id: str | None,
        selected_node_id: str | None,
    ) -> dict[str, Any]:
        conv_id = conversation_id or str(uuid.uuid4())
        latest = await self.job_store.get_latest()
        if not latest or latest.status != "completed":
            return {
                "conversation_id": conv_id,
                "answer": "No completed graph is available yet. Upload data and wait for processing to complete.",
                "domain_allowed": True,
                "evidence": {
                    "cypher": "",
                    "row_count": 0,
                    "reasoning": "No completed graph context",
                },
                "highlights": {"node_ids": [], "edge_ids": []},
            }

        dataset_context = latest.metadata.get("dataset_context", {})

        if not self._is_domain_question(question, dataset_context=dataset_context, metadata=latest.metadata):
            return {
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

        history = await self.job_store.get_conversation(conv_id, max_turns=8)
        planner = self._plan_query(
            question=question,
            selected_node_id=selected_node_id,
            metadata=latest.metadata,
            history=history,
            dataset_context=dataset_context,
        )

        cypher = self._sanitize_read_only_cypher(str(planner.get("cypher", "")))
        if not cypher:
            return {
                "conversation_id": conv_id,
                "answer": "I could not produce a safe graph query for that request. Please rephrase with dataset entities.",
                "domain_allowed": True,
                "evidence": {
                    "cypher": "",
                    "row_count": 0,
                    "reasoning": "Planner failed to produce a safe read-only query",
                },
                "highlights": {"node_ids": [], "edge_ids": []},
            }

        rows = self.neo4j_loader.run_read_query(cypher)
        answer = self._synthesize_answer(
            question=question,
            rows=rows,
            planner_reasoning=str(planner.get("reasoning", ""))[:500],
        )
        highlights = self._extract_highlights(rows)

        await self.job_store.append_conversation_turn(
            conv_id,
            user_message=question,
            assistant_message=answer,
        )

        return {
            "conversation_id": conv_id,
            "answer": answer,
            "domain_allowed": True,
            "evidence": {
                "cypher": cypher,
                "row_count": len(rows),
                "reasoning": str(planner.get("reasoning", ""))[:500],
            },
            "highlights": highlights,
        }

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

        prompt = {
            "task": "Generate one safe read-only Cypher query for the business question.",
            "constraints": [
                "Domain: uploaded Order-to-Cash dataset only.",
                "Graph model uses (n:GraphNode) and [r:GRAPH_EDGE].",
                "Node fields: id, label, entity, data (JSON string).",
                "Never use dynamic labels such as (:billing_document_headers); always use (:GraphNode).",
                "Filter table/entity via n.entity = '<table_name>'.",
                "Row-level columns live inside n.data JSON string.",
                "For column equality (example billingDocument=90504289), filter using a regex on n.data that allows optional spaces and optional quotes around the value.",
                "Edge fields: id, label, relationship_type, score, columns, edge_type.",
                "Only read-only query allowed.",
                "Always include LIMIT <= 50.",
                "No APOC, no CALL, no write clauses.",
                "Prefer RETURN projections: n.id AS id, n.label AS label, n.entity AS entity, n.data AS data.",
                "Output JSON only.",
            ],
            "selected_node_id": selected_node_id,
            "question": question,
            "conversation_history": history,
            "dataset_context": dataset_context,
            "tables": tables,
            "table_columns": table_columns,
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
                return parsed
            return {}
        except Exception:
            return {}

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
                return answer
        except Exception:
            pass

        if not rows:
            return "No matching records were found for this question in the current dataset graph."
        preview = rows[:3]
        return f"Found {len(rows)} matching records. Sample: {json.dumps(preview, ensure_ascii=True)}"

    def _sanitize_read_only_cypher(self, cypher: str) -> str:
        query = (cypher or "").strip()
        if not query:
            return ""

        rewritten = self._rewrite_legacy_node_lookup_query(query)
        if rewritten:
            query = rewritten

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
