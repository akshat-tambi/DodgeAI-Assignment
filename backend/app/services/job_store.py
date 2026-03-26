import asyncio
import json
import logging
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

try:
    import pymysql
except Exception:  # pragma: no cover
    pymysql = None

logger = logging.getLogger(__name__)


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
    def __init__(
        self,
        *,
        mysql_host: str = "",
        mysql_port: int = 3306,
        mysql_user: str = "",
        mysql_password: str = "",
        mysql_database: str = "",
    ) -> None:
        self._lock = asyncio.Lock()
        self._conn = None

        if pymysql is None:
            raise RuntimeError("PyMySQL is required for MySQL-backed JobStore.")
        if not (mysql_host and mysql_user and mysql_database):
            raise ValueError("MYSQL_HOST, MYSQL_USER, and MYSQL_DATABASE are required.")

        try:
            self._conn = pymysql.connect(
                host=mysql_host,
                port=int(mysql_port),
                user=mysql_user,
                password=mysql_password,
                database=mysql_database,
                charset="utf8mb4",
                autocommit=True,
                cursorclass=pymysql.cursors.DictCursor,
            )
            self._ensure_mysql_tables()
            logger.info("job store persistence enabled via mysql host=%s db=%s", mysql_host, mysql_database)
        except Exception as exc:  # pragma: no cover
            raise RuntimeError(f"Failed to initialize MySQL JobStore: {exc}") from exc

    @property
    def using_mysql(self) -> bool:
        return self._conn is not None

    def _ensure_mysql_tables(self) -> None:
        if not self._conn:
            return
        with self._conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                    job_id VARCHAR(64) PRIMARY KEY,
                    status VARCHAR(32) NOT NULL,
                    stage VARCHAR(64) NOT NULL,
                    message TEXT NOT NULL,
                    error TEXT NULL,
                    metadata_json LONGTEXT NULL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS conversations (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    job_id VARCHAR(64) NOT NULL,
                    conversation_id VARCHAR(64) NOT NULL,
                    turn_index INT NOT NULL,
                    user_message TEXT NOT NULL,
                    assistant_message TEXT NOT NULL,
                    ts DOUBLE NOT NULL,
                    INDEX idx_job_conversation_turn (job_id, conversation_id, turn_index)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """
            )
            try:
                cur.execute("ALTER TABLE conversations ADD COLUMN job_id VARCHAR(64) NOT NULL DEFAULT ''")
            except Exception:
                pass
            try:
                cur.execute(
                    "CREATE INDEX idx_job_conversation_turn ON conversations (job_id, conversation_id, turn_index)"
                )
            except Exception:
                pass

    @staticmethod
    def _loads_metadata(raw: Any) -> Dict[str, Any]:
        if not raw:
            return {}
        try:
            obj = json.loads(raw)
            if isinstance(obj, dict):
                return obj
            return {}
        except Exception:
            return {}

    @staticmethod
    def _to_state(row: Dict[str, Any]) -> JobState:
        return JobState(
            job_id=row["job_id"],
            status=row.get("status", "queued"),
            stage=row.get("stage", "queued"),
            message=row.get("message", "Job queued"),
            error=row.get("error"),
            metadata=JobStore._loads_metadata(row.get("metadata_json")),
        )

    async def create(self, job_id: str) -> JobState:
        async with self._lock:
            state = JobState(job_id=job_id)
            with self._conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO jobs (job_id, status, stage, message, error, metadata_json)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        status=VALUES(status),
                        stage=VALUES(stage),
                        message=VALUES(message),
                        error=VALUES(error),
                        metadata_json=VALUES(metadata_json)
                    """,
                    (
                        state.job_id,
                        state.status,
                        state.stage,
                        state.message,
                        state.error,
                        json.dumps(state.metadata),
                    ),
                )
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
            with self._conn.cursor() as cur:
                cur.execute("SELECT * FROM jobs WHERE job_id=%s", (job_id,))
                row = cur.fetchone()
                state = self._to_state(row) if row else None
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

            with self._conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE jobs
                    SET status=%s, stage=%s, message=%s, error=%s, metadata_json=%s
                    WHERE job_id=%s
                    """,
                    (
                        state.status,
                        state.stage,
                        state.message,
                        state.error,
                        json.dumps(state.metadata),
                        state.job_id,
                    ),
                )
            return state

    async def get(self, job_id: str) -> Optional[JobState]:
        async with self._lock:
            with self._conn.cursor() as cur:
                cur.execute("SELECT * FROM jobs WHERE job_id=%s", (job_id,))
                row = cur.fetchone()
                return self._to_state(row) if row else None

    async def get_latest(self) -> Optional[JobState]:
        async with self._lock:
            with self._conn.cursor() as cur:
                cur.execute("SELECT * FROM jobs ORDER BY updated_at DESC LIMIT 1")
                row = cur.fetchone()
                return self._to_state(row) if row else None

    async def get_conversation(self, conversation_id: str, *, job_id: str, max_turns: int = 10) -> list[dict[str, Any]]:
        async with self._lock:
            lim = max(1, int(max_turns))
            with self._conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT job_id, conversation_id, turn_index, user_message, assistant_message, ts
                    FROM conversations
                    WHERE conversation_id=%s AND job_id=%s
                    ORDER BY turn_index DESC
                    LIMIT %s
                    """,
                    (conversation_id, job_id, lim),
                )
                rows = list(reversed(cur.fetchall() or []))
                return [
                    {
                        "user": r.get("user_message", ""),
                        "assistant": r.get("assistant_message", ""),
                        "ts": r.get("ts"),
                    }
                    for r in rows
                ]

    async def append_conversation_turn(
        self,
        conversation_id: str,
        *,
        job_id: str,
        user_message: str,
        assistant_message: str,
        max_turns: int = 12,
    ) -> None:
        async with self._lock:
            with self._conn.cursor() as cur:
                cur.execute(
                    "SELECT COALESCE(MAX(turn_index), 0) AS max_idx FROM conversations WHERE conversation_id=%s AND job_id=%s",
                    (conversation_id, job_id),
                )
                row = cur.fetchone() or {"max_idx": 0}
                next_index = int(row.get("max_idx", 0)) + 1
                ts = time.time()
                cur.execute(
                    """
                    INSERT INTO conversations (job_id, conversation_id, turn_index, user_message, assistant_message, ts)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (job_id, conversation_id, next_index, user_message, assistant_message, ts),
                )
                if max_turns > 0:
                    cur.execute(
                        """
                        DELETE FROM conversations
                        WHERE conversation_id=%s
                          AND job_id=%s
                          AND turn_index <= %s
                        """,
                        (conversation_id, job_id, max(0, next_index - max_turns)),
                    )
