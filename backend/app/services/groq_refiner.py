from __future__ import annotations

import json
from typing import Any

from groq import Groq


class GroqRefiner:
    def __init__(self, api_key: str, model: str) -> None:
        self.enabled = bool(api_key)
        self.model = model
        self.client = Groq(api_key=api_key) if self.enabled else None

    def refine(self, borderline_relationships: list[dict[str, Any]], schemas: dict[str, Any]) -> list[dict[str, Any]]:
        if not self.enabled or not borderline_relationships:
            return borderline_relationships

        refined: list[dict[str, Any]] = []

        for candidate in borderline_relationships:
            prompt = {
                "instruction": "Confirm or reject this candidate relationship. Do not invent new relationships.",
                "table_a": candidate["table_a"],
                "table_b": candidate["table_b"],
                "column_a": candidate["column_a"],
                "column_b": candidate["column_b"],
                "stats": {
                    "score": candidate["score"],
                    "overlap_ratio": candidate["overlap_ratio"],
                    "intersection_size": candidate["intersection_size"],
                },
                "schema_a": schemas.get(candidate["table_a"], {}),
                "schema_b": schemas.get(candidate["table_b"], {}),
                "response_format": {
                    "confirm": "boolean",
                    "relationship_type": "one_of: ['1-1', '1-many', 'many-1', 'many-many', 'unknown']",
                    "reason": "short string",
                },
            }

            try:
                completion = self.client.chat.completions.create(
                    model=self.model,
                    temperature=0,
                    messages=[
                        {
                            "role": "system",
                            "content": "You are a strict data modeling assistant. Only evaluate the provided candidate.",
                        },
                        {"role": "user", "content": json.dumps(prompt)},
                    ],
                )
                content = completion.choices[0].message.content or "{}"
                parsed = self._parse_json(content)
                confirm = bool(parsed.get("confirm", False))
                candidate["llm_reason"] = str(parsed.get("reason", ""))
                candidate["relationship_type"] = str(parsed.get("relationship_type", "unknown"))
                candidate["llm_confirmed"] = confirm
                candidate["decision"] = "accepted" if confirm else "rejected"
            except Exception as exc:
                candidate["llm_confirmed"] = False
                candidate["llm_reason"] = f"refinement_failed: {exc}"
                candidate["decision"] = "rejected"

            refined.append(candidate)

        return refined

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
