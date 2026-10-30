import hashlib
import json
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone

SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    task_id        TEXT PRIMARY KEY,
    context_id     TEXT NOT NULL,
    principal      TEXT NOT NULL,
    batch_id       TEXT NOT NULL,
    state          TEXT NOT NULL,
    history_json   TEXT NOT NULL,
    artifacts_json TEXT NOT NULL,
    updated_at     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS tasks_principal ON tasks(principal);

-- Message-level idempotency, scoped per user.
CREATE TABLE IF NOT EXISTS idempotency (
    principal      TEXT NOT NULL,
    message_id     TEXT NOT NULL,
    semantic_hash  TEXT NOT NULL,
    task_id        TEXT NOT NULL,
    PRIMARY KEY (principal, message_id)
);

CREATE TABLE IF NOT EXISTS proposals (
    task_id        TEXT NOT NULL,
    action_id      TEXT NOT NULL,
    package_id     TEXT NOT NULL,
    action         TEXT NOT NULL,
    proposal_json  TEXT NOT NULL,
    PRIMARY KEY (task_id, action_id)
);

-- One row per executed action. The primary key makes execution happen at most once.
CREATE TABLE IF NOT EXISTS executions (
    action_id      TEXT PRIMARY KEY,
    task_id        TEXT NOT NULL,
    receipt_nonce  TEXT NOT NULL,
    execution_json TEXT NOT NULL,
    executed_at    TEXT NOT NULL
);

-- Cache of validated model decisions, keyed by canonical package content.
CREATE TABLE IF NOT EXISTS decision_cache (
    cache_key     TEXT PRIMARY KEY,
    decision_json TEXT NOT NULL
);
"""


def canonical(obj) -> str:
    """Recursively key-sorted, compact JSON. Every hash in the system uses this."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha256_hex(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def utcnow() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def task_from_row(row) -> dict:
    return {
        "id": row["task_id"],
        "contextId": row["context_id"],
        "status": {"state": row["state"], "timestamp": row["updated_at"]},
        "history": json.loads(row["history_json"]),
        "artifacts": json.loads(row["artifacts_json"]),
    }


class Store:
    def __init__(self, path: str):
        # isolation_level=None gives us explicit BEGIN/COMMIT control.
        self._conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.executescript(SCHEMA)
        self._lock = threading.RLock()        # serializes all DB access in this process
        self._guard = threading.Lock()
        self._key_locks: dict = {}

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
        """Per-(principal, messageId) lock so concurrent duplicates wait for one result."""
        with self._guard:
            return self._key_locks.setdefault(key, threading.Lock())

    @contextmanager
    def transaction(self):
        # BEGIN IMMEDIATE takes the write lock up front, so check-then-write is atomic.
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield self._conn
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise
            self._conn.execute("COMMIT")

    def one(self, sql, args=()):
        with self._lock:
            return self._conn.execute(sql, args).fetchone()

    def all(self, sql, args=()):
        with self._lock:
            return self._conn.execute(sql, args).fetchall()

    def get_cached(self, key):
        row = self.one("SELECT decision_json FROM decision_cache WHERE cache_key=?", (key,))
        return json.loads(row["decision_json"]) if row else None

    def put_cached(self, key, decision):
        with self.transaction() as c:
            c.execute("INSERT OR REPLACE INTO decision_cache VALUES (?,?)",
                      (key, canonical(decision)))