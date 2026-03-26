# Assignment

Graph-based data modeling and dataset-grounded chat system.

## Overview

This project ingests a ZIP of relational-style files, infers cross-table links, builds a graph, and enables natural-language analysis over that graph.

The system has two major surfaces:
- Frontend: upload, graph exploration, node inspection, and chat UX.
- Backend: ingestion pipeline, graph persistence, chat planning/execution, and response synthesis.

## Architecture Decisions

### 1) Split responsibilities by interaction pattern

Decision:
- Keep ingestion and graph logic in FastAPI backend.
- Keep rendering and user interactions in React frontend.

Why:
- Ingestion, scoring, graph persistence, and LLM orchestration are compute/data-heavy server concerns.
- Visualization, selection, and chat interaction loops are UX concerns that fit client-side state.

Tradeoff:
- Requires clear API contracts (`/upload`, `/jobs/{id}`, `/graph`, `/chat`) and job polling.

### 2) Async job model for uploads

Decision:
- Upload returns immediately with a `job_id`.
- Client polls status and fetches graph when processing completes.

Why:
- ZIP processing and graph writes can be long-running.
- Avoids request timeouts and gives user-visible progress/stages.

Tradeoff:
- Slightly more frontend complexity (polling, terminal-state handling).

### 3) Graph-first analytics

Decision:
- Materialize inferred relationships as graph nodes/edges and query with Cypher.

Why:
- Flow tracing, ranking by relationship fanout, and multi-hop analysis are easier and more expressive on graph structures than repeated SQL joins generated on demand.

Tradeoff:
- Requires consistency between pipeline output and chat query planner assumptions.

### 4) Evidence-first chat responses

Decision:
- Every chat answer includes evidence metadata and query traces.

Why:
- Improves transparency and debuggability for generated Cypher.
- Makes model behavior auditable for assignment requirements.

Tradeoff:
- Slightly more payload size and UI complexity.

## Database Choices

### Neo4j (graph store)

Used for:
- Persisting granular graph entities and inferred links.
- Running read-only Cypher queries for chat and graph exploration.

Why Neo4j:
- Native graph model and traversal performance.
- Cypher is concise for relationship-centric questions (pathing, fanout, neighbor analysis).

### MySQL (operational state)

Used for:
- Job state persistence.
- Conversation turn history.

Why MySQL:
- Simple, durable transactional store for operational metadata.
- Great fit for keyed lookups (`job_id`, `conversation_id`) and ordered turn retrieval.

### Why not one database only?

Decision:
- Keep graph analytics and operational metadata in separate systems.

Reason:
- Each workload aligns with a different storage/query model.
- Reduces complexity versus forcing graph problems into a relational schema or vice versa.

## LLM Prompting Strategy

### Goals

- Generate valid, read-only Cypher queries grounded in available schema/context.
- Avoid hallucinated entities/fields.
- Produce answers tied to actual query results.

### Prompting approach

1. Context injection
- Provide job-scoped graph/schema context and known columns/tables.
- Include selected node context when user focuses on specific graph entities.

2. Planner output as structured artifact
- LLM produces a query plan with candidate Cypher.
- Backend validates plan shape against expected query contract.

3. Contract-based validation and repair
- Infer contract for the user intent (for example ranking/aggregation vs lookup).
- If generated query is inconsistent, trigger repair pass.

4. Execution feedback loop
- Detect degenerate/weak results (for example uninformative aggregations).
- Ask LLM for revised query using execution feedback.

5. Evidence trace surfaced to UI
- Return planned/executed/repaired queries in evidence for visibility.

### Why this strategy

- A single prompt pass is often brittle for NL-to-Cypher.
- Contract + repair loop improves correctness without hardcoding fixed question templates.

## Guardrails

### Domain guardrails

- Chat is restricted to uploaded dataset graph context.
- Out-of-domain or unsupported requests are constrained to dataset-relevant handling.

### Query guardrails

- Read-only query intent (no write/DDL operations).
- Sanitization and validation before execution.
- Job scoping so users only query relevant active/selected graph state.

### Result guardrails

- Evidence object includes row counts and executed query traces.
- Low-confidence/degenerate outcomes can trigger repair logic instead of silent failure.

### Operational guardrails

- Upload size limit and upload rate limiting.
- Clear terminal job states (`completed`, `failed`) and error messaging.

## Frontend UX Notes

- Graph uses granular point-node rendering with highlighting and neighborhood expansion.
- Node inspector shows row fields and linked-table summary.
- Chat panel integrates evidence trace and highlight feedback into the graph.
- Sidebar cards support internal scrolling for large node payloads/evidence.

## Deployment Model

Current target deployment:
- Frontend on Vercel.
- Backend as Docker service on Render.

Render free-tier adjustments:
- No persistent disk support.
- MySQL data and uploads use `/tmp` paths (ephemeral across restarts).

For persistent state on Render:
- Paid tier with mounted disk and corresponding `UPLOAD_DIR`/`MYSQL_DATA_DIR`.

## Configuration

Key backend environment groups:
- App/CORS/network: `APP_HOST`, `APP_PORT`, `CORS_ORIGINS`.
- Ingestion thresholds: overlap/confidence/borderline and matching constraints.
- Graph/LLM: Neo4j + Groq credentials/model.
- Ops: upload limits, rate limit, MySQL connection.

Use:
- `backend/.env.example` as local template.
- `render.yaml` for deployment environment mapping.
