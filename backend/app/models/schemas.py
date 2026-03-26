from typing import Any, Dict, List, Optional

from pydantic import BaseModel


class UploadResponse(BaseModel):
    job_id: str
    status: str


class JobStatusResponse(BaseModel):
    job_id: str
    status: str
    stage: str
    message: str
    error: Optional[str] = None
    metadata: Dict[str, Any] = {}


class GraphNode(BaseModel):
    id: str
    label: str
    data: Dict[str, Any] = {}


class GraphEdge(BaseModel):
    id: str
    source: str
    target: str
    label: str
    data: Dict[str, Any] = {}


class GraphResponse(BaseModel):
    job_id: str
    nodes: List[GraphNode]
    edges: List[GraphEdge]
    metadata: Dict[str, Any]


class ChatRequest(BaseModel):
    question: str
    conversation_id: Optional[str] = None
    selected_node_id: Optional[str] = None


class ChatEvidence(BaseModel):
    cypher: str
    row_count: int
    reasoning: str


class ChatHighlights(BaseModel):
    node_ids: List[str] = []
    edge_ids: List[str] = []


class ChatResponse(BaseModel):
    conversation_id: str
    answer: str
    domain_allowed: bool
    evidence: ChatEvidence
    highlights: ChatHighlights
