from __future__ import annotations

import asyncio
import logging
import uuid
from pathlib import Path

from fastapi import APIRouter, File, HTTPException, Query, Request, UploadFile

from app.config import get_settings
from app.models.schemas import ChatRequest, ChatResponse, GraphResponse, JobStatusResponse, UploadResponse
from app.pipeline.neo4j_loader import Neo4jGraphLoader
from app.pipeline.orchestrator import process_upload_job
from app.services.chat_service import ChatService
from app.services.groq_refiner import GroqRefiner
from app.services.job_store import JobStore, UploadRateLimiter

router = APIRouter()
logger = logging.getLogger(__name__)
settings = get_settings()

UPLOAD_DIR = Path(settings.upload_dir)
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

job_store = JobStore(
    mysql_host=settings.mysql_host,
    mysql_port=settings.mysql_port,
    mysql_user=settings.mysql_user,
    mysql_password=settings.mysql_password,
    mysql_database=settings.mysql_database,
)
rate_limiter = UploadRateLimiter(settings.rate_limit_uploads_per_minute)
neo4j_loader = Neo4jGraphLoader(settings.neo4j_uri, settings.neo4j_user, settings.neo4j_password)
groq_refiner = GroqRefiner(settings.groq_api_key, settings.groq_model)
chat_service = ChatService(
    groq_api_key=settings.groq_api_key,
    groq_model=settings.groq_model,
    neo4j_loader=neo4j_loader,
    job_store=job_store,
)


@router.post("/upload", response_model=UploadResponse)
async def upload_zip(request: Request, file: UploadFile = File(...)) -> UploadResponse:
    client_ip = request.client.host if request.client else "unknown"
    logger.info("upload request received ip=%s filename=%s", client_ip, file.filename)
    allowed = await rate_limiter.allow(client_ip)
    if not allowed:
        logger.warning("upload rejected due to rate limit ip=%s", client_ip)
        raise HTTPException(status_code=429, detail="Rate limit exceeded. Try again in a minute.")

    if not file.filename.lower().endswith(".zip"):
        logger.warning("upload rejected invalid_extension ip=%s filename=%s", client_ip, file.filename)
        raise HTTPException(status_code=400, detail="Only ZIP uploads are supported.")

    max_bytes = settings.max_upload_mb * 1024 * 1024
    job_id = str(uuid.uuid4())
    zip_path = UPLOAD_DIR / f"{job_id}.zip"

    total = 0
    with zip_path.open("wb") as f:
        while True:
            chunk = await file.read(1024 * 1024)
            if not chunk:
                break
            total += len(chunk)
            if total > max_bytes:
                zip_path.unlink(missing_ok=True)
                logger.warning("upload rejected too_large ip=%s size_bytes=%s", client_ip, total)
                raise HTTPException(status_code=413, detail=f"File exceeds {settings.max_upload_mb} MB limit.")
            f.write(chunk)

    await job_store.create(job_id)
    logger.info("upload accepted ip=%s job_id=%s stored_zip=%s bytes=%s", client_ip, job_id, zip_path, total)

    asyncio.create_task(
        process_upload_job(
            job_store=job_store,
            job_id=job_id,
            zip_path=zip_path,
            settings=settings,
            neo4j_loader=neo4j_loader,
            groq_refiner=groq_refiner,
        )
    )

    return UploadResponse(job_id=job_id, status="queued")


@router.get("/jobs/{job_id}", response_model=JobStatusResponse)
async def get_job(job_id: str) -> JobStatusResponse:
    state = await job_store.get(job_id)
    if not state:
        raise HTTPException(status_code=404, detail="Job not found")
    logger.info("job status requested job_id=%s status=%s stage=%s", job_id, state.status, state.stage)
    return JobStatusResponse(
        job_id=state.job_id,
        status=state.status,
        stage=state.stage,
        message=state.message,
        error=state.error,
        metadata=state.metadata,
    )


@router.get("/graph", response_model=GraphResponse)
async def get_latest_graph(view: str = Query("granular", pattern="^(granular|table)$")) -> GraphResponse:
    latest = await job_store.get_latest()
    if not latest:
        raise HTTPException(status_code=404, detail="No jobs yet")

    graph_key = "graph_granular" if view == "granular" else "graph_table"
    graph = latest.metadata.get(graph_key) or latest.metadata.get("graph", {"nodes": [], "edges": []})
    logger.info(
        "graph fetch latest_job_id=%s view=%s status=%s nodes=%s edges=%s",
        latest.job_id,
        view,
        latest.status,
        len(graph.get("nodes", [])),
        len(graph.get("edges", [])),
    )
    return GraphResponse(
        job_id=latest.job_id,
        nodes=graph.get("nodes", []),
        edges=graph.get("edges", []),
        metadata={
            "status": latest.status,
            "stage": latest.stage,
            "message": latest.message,
            "view": view,
        },
    )


@router.post("/chat", response_model=ChatResponse)
async def chat_with_graph(payload: ChatRequest) -> ChatResponse:
    question = (payload.question or "").strip()
    if not question:
        raise HTTPException(status_code=400, detail="Question is required")

    result = await chat_service.answer(
        question=question,
        conversation_id=payload.conversation_id,
        selected_node_id=payload.selected_node_id,
    )
    return ChatResponse(**result)
