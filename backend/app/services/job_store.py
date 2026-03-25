import asyncio
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class JobState:
    job_id: str
    status: str = "queued"
    stage: str = "queued"
    message: str = "Job queued"
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class UploadRateLimiter:
    def __init__(self, uploads_per_minute: int) -> None:
        self.uploads_per_minute = uploads_per_minute
        self._access: dict[str, deque[float]] = defaultdict(deque)
        self._lock = asyncio.Lock()

    async def allow(self, key: str) -> bool:
        now = time.time()
        cutoff = now - 60.0
        async with self._lock:
            q = self._access[key]
            while q and q[0] < cutoff:
                q.popleft()
            if len(q) >= self.uploads_per_minute:
                return False
            q.append(now)
            return True


class JobStore:
    def __init__(self) -> None:
        self._jobs: Dict[str, JobState] = {}
        self._lock = asyncio.Lock()
        self._latest_job_id: Optional[str] = None

    async def create(self, job_id: str) -> JobState:
        async with self._lock:
            state = JobState(job_id=job_id)
            self._jobs[job_id] = state
            self._latest_job_id = job_id
            return state

    async def update(
        self,
        job_id: str,
        *,
        status: Optional[str] = None,
        stage: Optional[str] = None,
        message: Optional[str] = None,
        error: Optional[str] = None,
        metadata_patch: Optional[Dict[str, Any]] = None,
    ) -> Optional[JobState]:
        async with self._lock:
            state = self._jobs.get(job_id)
            if not state:
                return None
            if status is not None:
                state.status = status
            if stage is not None:
                state.stage = stage
            if message is not None:
                state.message = message
            if error is not None:
                state.error = error
            if metadata_patch is not None:
                state.metadata.update(metadata_patch)
            return state

    async def get(self, job_id: str) -> Optional[JobState]:
        async with self._lock:
            return self._jobs.get(job_id)

    async def get_latest(self) -> Optional[JobState]:
        async with self._lock:
            if not self._latest_job_id:
                return None
            return self._jobs.get(self._latest_job_id)
