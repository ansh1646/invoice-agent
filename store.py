
import hashlib
import json
import os
import threading
from contextlib import contextmanager
from datetime import datetime, timezone

import psycopg
from psycopg.rows import dict_row


SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    task_id TEXT PRIMARY KEY,
    context_id TEXT NOT NULL,
    principal TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    state TEXT NOT NULL,
    history_json TEXT NOT NULL,
    artifacts_json TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS tasks_principal ON tasks(principal);

CREATE TABLE IF NOT EXISTS idempotency (
    principal TEXT NOT NULL,
    message_id TEXT NOT NULL,
    semantic_hash TEXT NOT NULL,
    task_id TEXT NOT NULL,
    PRIMARY KEY (principal, message_id)
);

CREATE TABLE IF NOT EXISTS proposals (
    task_id TEXT NOT NULL,
    action_id TEXT NOT NULL,
    package_id TEXT NOT NULL,
    action TEXT NOT NULL,
    proposal_json TEXT NOT NULL,
    PRIMARY KEY (task_id, action_id)
);

CREATE TABLE IF NOT EXISTS executions (
    action_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    receipt_nonce TEXT NOT NULL,
    execution_json TEXT NOT NULL,
    executed_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS decision_cache (
    cache_key TEXT PRIMARY KEY,
    decision_json TEXT NOT NULL
);
"""


def canonical(obj) -> str:
    return json.dumps(
        obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )


def sha256_hex(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def utcnow() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def task_from_row(row) -> dict:
    return {
        "id": row["task_id"],
        "contextId": row["context_id"],
        "status": {
            "state": row["state"],
            "timestamp": row["updated_at"]
        },
        "history": json.loads(row["history_json"]),
        "artifacts": json.loads(row["artifacts_json"]),
    }


class Store:
    def __init__(self, path: str = None):
        self.database_url = path or os.environ.get("DATABASE_URL")

        if not self.database_url:
            raise RuntimeError("DATABASE_URL environment variable is missing")

        self._conn = psycopg.connect(
            self.database_url,
            autocommit=True,
            row_factory=dict_row,
            connect_timeout=10
        )

        self._conn.execute("SET TIME ZONE 'UTC'")

        self._lock = threading.RLock()
        self._guard = threading.Lock()
        self._key_locks = {}

        with self._conn.cursor() as cur:
            cur.execute(SCHEMA)

    def close(self):
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def key_lock(self, key):
        with self._guard:
            return self._key_locks.setdefault(key, threading.Lock())

    @contextmanager
    def distributed_key_lock(self, key):
        """Serialize an idempotency key across application processes."""
        lock_id = int.from_bytes(
            hashlib.sha256(repr(key).encode("utf-8")).digest()[:8],
            byteorder="big",
            signed=True,
        )
        with self._lock:
            self._conn.execute("SELECT pg_advisory_lock(%s)", (lock_id,))
        try:
            yield
        finally:
            with self._lock:
                self._conn.execute("SELECT pg_advisory_unlock(%s)", (lock_id,))

    @contextmanager
    def transaction(self):
        with self._lock:
            with self._conn.transaction():
                yield self._conn

    def one(self, sql, args=()):
        sql = sql.replace("?", "%s")
        with self._lock:
            return self._conn.execute(sql, args).fetchone()

    def all(self, sql, args=()):
        sql = sql.replace("?", "%s")
        with self._lock:
            return self._conn.execute(sql, args).fetchall()

    def get_cached(self, key):
        row = self.one(
            "SELECT decision_json FROM decision_cache WHERE cache_key=?",
            (key,)
        )
        return json.loads(row["decision_json"]) if row else None

    def put_cached(self, key, decision):
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT INTO decision_cache (cache_key, decision_json)
                VALUES (%s, %s)
                ON CONFLICT (cache_key)
                DO UPDATE SET decision_json = EXCLUDED.decision_json
                """,
                (key, canonical(decision))
            )
