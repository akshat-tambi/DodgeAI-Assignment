from __future__ import annotations

import json
from typing import Any

from groq import Groq


class GroqRefiner:
    def __init__(self, api_key: str, model: str) -> None:
        if not api_key:
            raise ValueError("GROQ_API_KEY is required because LLM verification is mandatory")
        self.model = model
        self.client = Groq(api_key=api_key)

    def verify_all(
        self,
        inferred_relationships: list[dict[str, Any]],
        table_rows: dict[str, list[dict[str, Any]]],
        schemas: dict[str, Any],
        *,
        max_context_samples: int = 3,
    ) -> list[dict[str, Any]]:
        if not inferred_relationships:
            return inferred_relationships

        verified: list[dict[str, Any]] = [dict(rel) for rel in inferred_relationships]
        sample_limit = max(1, min(max_context_samples, 5))
        candidates: list[dict[str, Any]] = []

        for idx, relation in enumerate(verified):
            try:
                evidence = self._build_evidence(relation, table_rows, sample_limit)
                compact_schema = self._compact_schema(relation, schemas)
                candidates.append(
                    {
                        "candidate_id": idx,
                        "candidate": {
                            "source_table": relation.get("source_table", relation.get("table_a")),
                            "source_column": relation.get("source_column", relation.get("column_a")),
                            "target_table": relation.get("target_table", relation.get("table_b")),
                            "target_column": relation.get("target_column", relation.get("column_b")),
                            "relationship_type": relation.get("relationship_type", "unknown"),
                            "score": relation.get("score", 0.0),
                            "overlap_ratio": relation.get("overlap_ratio", 0.0),
                            "intersection_size": relation.get("intersection_size", 0),
                        },
                        "schema": compact_schema,
                        "evidence": evidence,
                    }
                )
            except Exception as exc:
                relation["llm_confirmed"] = False
                relation["llm_confidence"] = 0.0
                relation["llm_reason"] = f"evidence_build_failed: {exc}"
                relation["decision"] = "rejected"

        if not candidates:
            return verified

        prompt = {
            "instruction": (
                "Verify ALL inferred FK/PK candidates in one pass. "
                "Evaluate each candidate independently with a strict false-positive-avoidance posture. "
                "Use semantic reasoning first, then use evidence to validate or disprove. "
                "If uncertain, reject."
            ),
            "decision_policy": {
                "accept_when": [
                    "Identifier-like columns align semantically (e.g., document/order/customer/product/account IDs).",
                    "Entity intent and cardinality are plausible before considering overlap.",
                    "Row evidence supports the semantic hypothesis and does not contradict it."
                ],
                "reject_when": [
                    "Columns are primarily metric/measure fields (amount, total, net, tax, rate, quantity, currency, price).",
                    "Relationship appears driven by repeated numeric values, rounding, or small domains.",
                    "Semantic mismatch between column meanings even if overlap is high.",
                    "Direction/cardinality is unsupported by evidence."
                ],
                "priority": "semantic_meaning_over_statistical_overlap",
                "semantic_priors": [
                    "Metric/amount/price/tax/rate/currency fields are usually measures, not foreign keys.",
                    "High overlap on measures is common and not sufficient for FK/PK confirmation.",
                    "Treat measure-to-measure joins as suspicious unless clear entity/key semantics are present.",
                    "Use row evidence to refine or overturn borderline semantic judgments.",
                    "Numeric overlap is supporting evidence only, never the primary reason to accept."
                ],
                "reasoning_steps": [
                    "Step 1: infer semantic role of source and target columns.",
                    "Step 2: decide if an FK/PK hypothesis is semantically plausible.",
                    "Step 3: use overlap and examples only to validate/disprove that hypothesis."
                ]
            },
            "candidates": candidates,
            "response_format": {
                "decisions": [
                    {
                        "candidate_id": "integer",
                        "confirm": "boolean",
                        "relationship_type": "one_of: ['1-1', '1-many', 'many-1', 'many-many', 'unknown']",
                        "confidence": "number_between_0_and_1",
                        "metric_only": "boolean",
                        "suspicious_tags": "array_of_short_strings",
                        "reason": "max_20_words"
                    },
                ]
            },
            "output_rules": [
                "Output valid JSON object only, no markdown.",
                "Never add extra keys.",
                "Return one decision for every candidate_id.",
                "Decide confirm primarily from semantic validity, then use evidence to refine confidence.",
                "Use suspicious_tags to identify issues such as metric_only, semantic_mismatch, weak_evidence, value_coincidence, or wrong_direction.",
                "You may reject using domain semantics even if row evidence is sparse.",
                "Do not accept based on overlap alone.",
                "When in doubt between accept and reject, reject."
            ],
        }

        try:
            completion = self.client.chat.completions.create(
                model=self.model,
                temperature=0,
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "You are a strict data relationship verifier. "
                            "Never invent evidence. "
                            "Be conservative and reject ambiguous links, especially metric-only joins. "
                            "Use domain semantics and data modeling priors, not overlap alone. "
                            "False positives are more harmful than false negatives."
                        ),
                    },
                    {
                        "role": "user",
                        "content": json.dumps(prompt, separators=(",", ":")),
                    },
                ],
            )
            content = completion.choices[0].message.content or "{}"
            parsed = self._parse_json(content)
            raw_decisions = parsed.get("decisions", [])
            decisions_by_id: dict[int, dict[str, Any]] = {}

            if isinstance(raw_decisions, list):
                for item in raw_decisions:
                    if not isinstance(item, dict):
                        continue
                    cid = item.get("candidate_id")
                    if isinstance(cid, int):
                        decisions_by_id[cid] = item

            for idx, relation in enumerate(verified):
                decision = decisions_by_id.get(idx)
                if not decision:
                    relation["llm_confirmed"] = False
                    relation["llm_confidence"] = 0.0
                    relation["llm_metric_only"] = False
                    relation["llm_suspicious_tags"] = ["missing_llm_decision"]
                    relation["llm_reason"] = "llm_missing_decision_for_candidate"
                    relation["decision"] = "rejected"
                    continue

                confirm = bool(decision.get("confirm", False))
                relation["llm_confirmed"] = confirm
                relation["llm_confidence"] = float(decision.get("confidence", 0.0) or 0.0)
                relation["llm_metric_only"] = bool(decision.get("metric_only", False))

                tags = decision.get("suspicious_tags", [])
                if isinstance(tags, list):
                    relation["llm_suspicious_tags"] = [str(tag)[:40] for tag in tags[:8]]
                else:
                    relation["llm_suspicious_tags"] = []

                relation["llm_reason"] = str(decision.get("reason", ""))[:200]
                relation["relationship_type"] = str(
                    decision.get("relationship_type", relation.get("relationship_type", "unknown"))
                )
                relation["decision"] = "accepted" if confirm else "rejected"
        except Exception as exc:
            for relation in verified:
                relation["llm_confirmed"] = False
                relation["llm_confidence"] = 0.0
                relation["llm_metric_only"] = False
                relation["llm_suspicious_tags"] = ["verification_failed"]
                relation["llm_reason"] = f"verification_failed: {exc}"
                # Preserve previous decision on transient failures.
                relation["decision"] = relation.get("decision", "rejected")

        return verified

    @staticmethod
    def _normalize(value: Any) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        return text

    def _build_evidence(
        self,
        relation: dict[str, Any],
        table_rows: dict[str, list[dict[str, Any]]],
        sample_limit: int,
    ) -> dict[str, Any]:
        source_table = relation.get("source_table", relation.get("table_a"))
        target_table = relation.get("target_table", relation.get("table_b"))
        source_column = relation.get("source_column", relation.get("column_a"))
        target_column = relation.get("target_column", relation.get("column_b"))

        source_rows = table_rows.get(source_table, [])
        target_rows = table_rows.get(target_table, [])

        target_index: dict[str, list[dict[str, Any]]] = {}
        for row in target_rows:
            v = self._normalize(row.get(target_column))
            if v is None:
                continue
            target_index.setdefault(v, []).append(row)

        matched_examples: list[dict[str, Any]] = []
        unmatched_source_values: list[str] = []
        source_value_samples: list[str] = []
        target_value_samples: list[str] = []

        for row in source_rows:
            value = self._normalize(row.get(source_column))
            if value is None:
                continue

            if len(source_value_samples) < sample_limit and value not in source_value_samples:
                source_value_samples.append(value)

            matches = target_index.get(value, [])
            if matches:
                if len(matched_examples) < sample_limit:
                    matched_examples.append(
                        {
                            "value": value,
                            "source_row_id": str(row.get("__row_id", "")),
                            "target_row_id": str(matches[0].get("__row_id", "")),
                        }
                    )
            else:
                if len(unmatched_source_values) < sample_limit:
                    unmatched_source_values.append(value)

            if len(matched_examples) >= sample_limit and len(unmatched_source_values) >= sample_limit:
                break

        for row in target_rows:
            value = self._normalize(row.get(target_column))
            if value is None:
                continue
            if len(target_value_samples) < sample_limit and value not in target_value_samples:
                target_value_samples.append(value)
            if len(target_value_samples) >= sample_limit:
                break

        return {
            "source_row_count": len(source_rows),
            "target_row_count": len(target_rows),
            "matched_examples": matched_examples,
            "unmatched_source_values": unmatched_source_values,
            "source_value_samples": source_value_samples,
            "target_value_samples": target_value_samples,
        }

    @staticmethod
    def _compact_schema(relation: dict[str, Any], schemas: dict[str, Any]) -> dict[str, Any]:
        source_table = relation.get("source_table", relation.get("table_a"))
        target_table = relation.get("target_table", relation.get("table_b"))
        source_column = relation.get("source_column", relation.get("column_a"))
        target_column = relation.get("target_column", relation.get("column_b"))

        def column_meta(table: str, column: str) -> dict[str, Any]:
            meta = schemas.get(table, {})
            columns = meta.get("columns", [])
            for c in columns:
                if c.get("name") == column:
                    return {
                        "name": c.get("name"),
                        "dtype": c.get("dtype"),
                        "null_ratio": c.get("null_ratio"),
                    }
            return {"name": column}

        return {
            "source": {
                "table": source_table,
                "row_count": schemas.get(source_table, {}).get("row_count", 0),
                "column": column_meta(source_table, source_column),
            },
            "target": {
                "table": target_table,
                "row_count": schemas.get(target_table, {}).get("row_count", 0),
                "column": column_meta(target_table, target_column),
            },
        }

    @staticmethod
    def _parse_json(raw: str) -> dict[str, Any]:
        raw = raw.strip()
        if raw.startswith("```"):
            raw = raw.strip("`")
            if raw.lower().startswith("json"):
                raw = raw[4:]
        raw = raw.strip()
        try:
            return json.loads(raw)
        except Exception:
            start = raw.find("{")
            end = raw.rfind("}")
            if start >= 0 and end > start:
                return json.loads(raw[start : end + 1])
            return {}
