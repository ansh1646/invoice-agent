import asyncio
from contextvars import ContextVar
import json
import logging
import os
import time
import uuid
from urllib.parse import urlparse

from fastapi import APIRouter, FastAPI, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse
import psycopg

from decisions import DecisionError, decide_batch
from store import Store, canonical, sha256_hex, task_from_row, utcnow
from dotenv import load_dotenv

load_dotenv()

BATCH_MT = "application/vnd.ga5.invoice-claim-batch+json"
PROPOSALS_MT = "application/vnd.ga5.invoice-action-proposals+json"
RESULTS_MT = "application/vnd.ga5.invoice-action-results+json"
RECEIPTS_MT = "application/vnd.ga5.invoice-action-receipts+json"
A2A_MT = "application/a2a+json"

SUBMITTED = "TASK_STATE_SUBMITTED"
INPUT_REQUIRED = "TASK_STATE_INPUT_REQUIRED"
COMPLETED = "TASK_STATE_COMPLETED"
CANCELED = "TASK_STATE_CANCELED"

# Use the exact base URL submitted to the grader.
BASE_URL = os.environ["PUBLIC_BASE_URL"]
parsed = urlparse(BASE_URL)

if (
    parsed.scheme != "https"
    or parsed.query
    or parsed.fragment
    or parsed.username
    or parsed.password
):
    raise SystemExit("PUBLIC_BASE_URL must be a clean HTTPS base URL")

PREFIX = parsed.path.rstrip("/")

# Use the explicit local/test path when provided; Neon is the fallback.
if os.environ.get("TESTING") == "1":
    store = Store(os.environ["TEST_DATABASE_URL"])
else:
    store = Store(os.environ["DATABASE_URL"])

MAX_BODY = 4 * 1024 * 1024
MAX_RESPONSE = 512 * 1024
REQUEST_TIMEOUT_SECONDS = 45
OVERALL_EXECUTION_SECONDS = 160


CARD = {
    "name": "Invoice Action Agent",
    "description": (
        "Reads invoice evidence packages and proposes one auditable "
        "accounts-payable action per invoice."
    ),
    "version": "1.0.0",
    "supportedInterfaces": [
        {
            "url": BASE_URL,
            "protocolBinding": "HTTP+JSON",
            "protocolVersion": "1.0",
        }
    ],
    "capabilities": {
        "streaming": False,
        "pushNotifications": False,
    },
    "defaultInputModes": [BATCH_MT, RESULTS_MT],
    "defaultOutputModes": [PROPOSALS_MT, RECEIPTS_MT],
    "skills": [
        {
            "id": "invoice_action_agent",
            "name": "Invoice action agent",
            "description": (
                "Chooses settle, approval, hold, duplicate-rejection or "
                "exception actions for invoices and records receipts "
                "after grader approval."
            ),
            "tags": [
                "invoice",
                "accounts-payable",
                "evidence",
                "approval",
            ],
        }
    ],
}


class ApiError(Exception):
    def __init__(self, status: int, code: str):
        self.status = status
        self.code = code


def a2a(payload, status: int = 200) -> JSONResponse:
    response = JSONResponse(payload, status_code=status, media_type=A2A_MT)
    if len(response.body) <= MAX_RESPONSE:
        return response
    return JSONResponse(
        {"error": {"code": "RESPONSE_TOO_LARGE"}},
        status_code=413,
        media_type=A2A_MT,
    )


app = FastAPI(
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)
logger = logging.getLogger(__name__)
request_id_var = ContextVar("request_id", default="-")


def log_event(level: int, event: str, **fields) -> None:
    """Emit one machine-readable log record without invoice contents or secrets."""
    record = {"event": event, "request_id": request_id_var.get(), **fields}
    logger.log(level, json.dumps(record, sort_keys=True, default=str))


@app.middleware("http")
async def request_logging(request: Request, call_next):
    request_id = uuid.uuid4().hex
    request.state.request_id = request_id
    token = request_id_var.set(request_id)
    started = time.monotonic()
    log_event(logging.INFO, "request_started", method=request.method)
    try:
        response = await call_next(request)
        route = getattr(request.scope.get("route"), "path", "unmatched")
        log_event(
            logging.INFO if response.status_code < 400 else logging.WARNING,
            "request_finished",
            method=request.method,
            route=route,
            status=response.status_code,
            duration_ms=round((time.monotonic() - started) * 1000),
        )
        response.headers["X-Request-ID"] = request_id
        return response
    except Exception as exc:
        log_event(
            logging.ERROR,
            "request_unhandled_exception",
            method=request.method,
            error_type=type(exc).__name__,
            duration_ms=round((time.monotonic() - started) * 1000),
        )
        raise
    finally:
        request_id_var.reset(token)


@app.exception_handler(ApiError)
async def api_error(request, exc: ApiError):
    # Do not reveal task IDs or whether another user's task exists.
    log_event(
        logging.WARNING,
        "api_error",
        method=request.method,
        status=exc.status,
        code=exc.code,
    )
    return a2a({"error": {"code": exc.code}}, exc.status)


@app.exception_handler(psycopg.Error)
async def database_error(_request, _exc):
    log_event(
        logging.ERROR,
        "database_error",
        error_type=type(_exc).__name__,
        sqlstate=getattr(_exc, "sqlstate", None),
    )
    return a2a({"error": {"code": "DATABASE_UNAVAILABLE"}}, 503)


# ---------- request guards ----------

def authenticate(request: Request) -> str:
    scheme, _, token = request.headers.get(
        "authorization", ""
    ).partition(" ")

    token = token.strip()

    if scheme != "Bearer" or not token:
        log_event(logging.WARNING, "authentication_failed", reason="missing_or_invalid_bearer")
        raise ApiError(401, "UNAUTHENTICATED")

    # Never store raw bearer tokens.
    return sha256_hex("principal:" + token)


def require_protocol(request: Request) -> None:
    if request.headers.get("A2A-Version") != "1.0":
        log_event(logging.WARNING, "protocol_version_rejected")
        raise ApiError(400, "VERSION_NOT_SUPPORTED")


def require_json(request: Request) -> None:
    if not request.headers.get(
        "content-type", ""
    ).lower().startswith(A2A_MT):
        log_event(logging.WARNING, "media_type_rejected")
        raise ApiError(415, "UNSUPPORTED_MEDIA_TYPE")


async def read_body(request: Request) -> dict:
    raw = await request.body()

    if len(raw) > MAX_BODY:
        log_event(logging.WARNING, "request_body_rejected", reason="too_large", bytes=len(raw))
        raise ApiError(413, "PAYLOAD_TOO_LARGE")

    try:
        body = json.loads(raw)
    except ValueError:
        log_event(logging.WARNING, "request_body_rejected", reason="invalid_json")
        raise ApiError(400, "INVALID_JSON")

    if not isinstance(body, dict):
        log_event(logging.WARNING, "request_body_rejected", reason="invalid_envelope")
        raise ApiError(400, "INVALID_ENVELOPE")

    return body


def envelope_data(msg: dict, media_type: str) -> dict:
    for part in msg.get("parts", []):
        if (
            isinstance(part, dict)
            and part.get("mediaType") == media_type
            and isinstance(part.get("data"), dict)
        ):
            return part["data"]

    raise ApiError(400, "INVALID_ENVELOPE")


def artifact_of(name: str, media_type: str, data: dict) -> dict:
    return {
        "artifactId": str(uuid.uuid4()),
        "name": name,
        "parts": [
            {
                "mediaType": media_type,
                "data": data,
            }
        ],
    }


def with_ids(msg: dict, task_id: str, context_id: str) -> dict:
    return {
        **msg,
        "taskId": task_id,
        "contextId": context_id,
    }


def load_owned(c, principal: str, task_id: str):
    """Ownership check inside a transaction."""

    row = c.execute(
        """
        SELECT *
        FROM tasks
        WHERE task_id=%s AND principal=%s
        """,
        (task_id, principal),
    ).fetchone()

    if row is None:
        raise ApiError(404, "TASK_NOT_FOUND")

    return row


def read_task(principal: str, task_id: str) -> dict:
    row = store.one(
        "SELECT * FROM tasks WHERE task_id=? AND principal=?",
        (task_id, principal),
    )

    if row is None:
        raise ApiError(404, "TASK_NOT_FOUND")

    task = task_from_row(row)
    log_event(logging.INFO, "task_read", task_id=task_id, state=task["status"]["state"])
    return task


# ---------- message:send ----------

def handle_send(principal: str, body: dict) -> dict:
    msg = body.get("message")

    if (
        not isinstance(msg, dict)
        or not isinstance(msg.get("messageId"), str)
        or not isinstance(msg.get("parts"), list)
        or msg.get("role") != "ROLE_USER"
    ):
        raise ApiError(400, "INVALID_ENVELOPE")

    # Hash the message only. Configuration is intentionally excluded.
    semantic = sha256_hex(canonical(msg))
    message_fingerprint = sha256_hex(msg["messageId"])[:12]
    log_event(
        logging.INFO,
        "message_received",
        message_fingerprint=message_fingerprint,
        has_task_id=bool(msg.get("taskId")),
    )

    with store.key_lock((principal, msg["messageId"])):
        with store.distributed_key_lock((principal, msg["messageId"])):

            seen = store.one(
                """
                SELECT semantic_hash, task_id
                FROM idempotency
                WHERE principal=? AND message_id=?
                """,
                (principal, msg["messageId"]),
            )

            if seen:
                if seen["semantic_hash"] != semantic:
                    log_event(logging.WARNING, "idempotency_conflict", message_fingerprint=message_fingerprint)
                    raise ApiError(409, "IDEMPOTENCY_CONFLICT")

                log_event(logging.INFO, "idempotency_replay", task_id=seen["task_id"])
                return read_task(principal, seen["task_id"])

            if msg.get("taskId"):
                return continue_task(principal, msg, semantic)

            return start_batch(principal, msg, semantic)


def start_batch(
    principal: str,
    msg: dict,
    semantic: str,
) -> dict:

    data = envelope_data(msg, BATCH_MT)

    batch_id = data.get("batchId")
    policy = data.get("policyRevision")
    packages = data.get("packages")

    if (
        not isinstance(batch_id, str)
        or not isinstance(policy, str)
        or not isinstance(packages, list)
        or not packages
        or not all(isinstance(p, dict) for p in packages)
    ):
        log_event(logging.WARNING, "batch_rejected", reason="invalid_batch_shape")
        raise ApiError(400, "INVALID_BATCH")

    ids = [p.get("packageId") for p in packages]

    if (
        not all(isinstance(i, str) and i for i in ids)
        or len(set(ids)) != len(ids)
    ):
        log_event(logging.WARNING, "batch_rejected", reason="invalid_or_duplicate_package_ids", package_count=len(packages))
        raise ApiError(400, "INVALID_BATCH")

    log_event(logging.INFO, "batch_validated", batch_id=batch_id, package_count=len(packages))
    decision_started = time.monotonic()
    try:
        decisions = decide_batch(packages, policy, store)
    except DecisionError as exc:
        log_event(
            logging.ERROR,
            "invoice_decision_unavailable",
            batch_id=batch_id,
            package_count=len(packages),
            duration_ms=round((time.monotonic() - decision_started) * 1000),
            reason=str(exc)[:1200],
        )
        raise ApiError(503, "DECISION_UNAVAILABLE")
    log_event(
        logging.INFO,
        "batch_decisions_ready",
        batch_id=batch_id,
        decision_count=len(decisions),
        duration_ms=round((time.monotonic() - decision_started) * 1000),
    )

    task_id = str(uuid.uuid4())
    context_id = msg.get("contextId") or str(uuid.uuid4())

    proposals = [
        {
            "packageId": d["packageId"],
            "actionId": f"act-{uuid.uuid4().hex}",
            "action": d["action"],
            "facts": d["facts"],
            "evidenceRefs": d["evidenceRefs"],
            "rationale": d["rationale"],
        }
        for d in decisions
    ]

    artifact = artifact_of(
        "invoice-action-proposals",
        PROPOSALS_MT,
        {
            "batchId": batch_id,
            "proposals": proposals,
        },
    )

    log_event(logging.INFO, "task_persistence_started", task_id=task_id, batch_id=batch_id)
    with store.transaction() as c:

        c.execute(
            """
            INSERT INTO tasks
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                task_id,
                context_id,
                principal,
                batch_id,
                INPUT_REQUIRED,
                canonical([with_ids(msg, task_id, context_id)]),
                canonical([artifact]),
                utcnow(),
            ),
        )

        for p in proposals:
            c.execute(
                """
                INSERT INTO proposals
                VALUES (%s,%s,%s,%s,%s)
                """,
                (
                    task_id,
                    p["actionId"],
                    p["packageId"],
                    p["action"],
                    canonical(p),
                ),
            )

        c.execute(
            """
            INSERT INTO idempotency
            VALUES (%s,%s,%s,%s)
            """,
            (
                principal,
                msg["messageId"],
                semantic,
                task_id,
            ),
        )

    log_event(logging.INFO, "task_persisted", task_id=task_id, state=INPUT_REQUIRED, proposal_count=len(proposals))
    return read_task(principal, task_id)


def validate_results(
    data: dict,
    batch_id: str,
    proposals: dict,
) -> list:

    if data.get("batchId") != batch_id:
        raise ApiError(400, "BINDING_MISMATCH")

    results = data.get("results")

    if not isinstance(results, list):
        raise ApiError(400, "INVALID_ENVELOPE")

    seen = set()
    accepted = []

    for r in results:

        if not isinstance(r, dict):
            raise ApiError(400, "INVALID_ENVELOPE")

        aid = r.get("actionId")
        prop = proposals.get(aid)

        if (
            prop is None
            or prop["package_id"] != r.get("packageId")
            or prop["action"] != r.get("action")
        ):
            raise ApiError(400, "BINDING_MISMATCH")

        if aid in seen:
            raise ApiError(400, "DUPLICATE_RESULT")

        seen.add(aid)

        outcome = r.get("outcome")

        if outcome == "ACCEPTED":
            nonce = r.get("receiptNonce")

            if not isinstance(nonce, str) or len(nonce) < 8:
                raise ApiError(400, "INVALID_RECEIPT")

            accepted.append((prop, nonce))

        elif outcome != "REJECTED":
            raise ApiError(400, "INVALID_OUTCOME")

    if seen != set(proposals):
        raise ApiError(400, "INCOMPLETE_RESULTS")

    return accepted


def record_execution(
    c,
    task_id: str,
    receipt: dict,
) -> None:
    """
    Records the execution in the database.
    This is a stand-in for an external payment-provider integration.
    """

    c.execute(
        """
        INSERT INTO executions
        VALUES (%s,%s,%s,%s,%s)
        """,
        (
            receipt["actionId"],
            task_id,
            receipt["receiptNonce"],
            canonical(receipt),
            utcnow(),
        ),
    )
    log_event(
        logging.INFO,
        "accepted_action_recorded",
        task_id=task_id,
        action_id=receipt["actionId"],
        action=receipt["action"],
    )


def continue_task(
    principal: str,
    msg: dict,
    semantic: str,
) -> dict:

    task_id = msg["taskId"]
    log_event(logging.INFO, "continuation_received", task_id=task_id)
    data = envelope_data(msg, RESULTS_MT)

    with store.transaction() as c:

        row = load_owned(c, principal, task_id)

        if msg.get("contextId") != row["context_id"]:
            raise ApiError(400, "CONTEXT_MISMATCH")

        if row["state"] != INPUT_REQUIRED:
            raise ApiError(409, "TASK_NOT_CONTINUABLE")

        proposals = {
            r["action_id"]: r
            for r in c.execute(
                """
                SELECT *
                FROM proposals
                WHERE task_id=%s
                """,
                (task_id,),
            ).fetchall()
        }

        if msg.get("role") != "ROLE_USER":
            raise ApiError(400, "INVALID_ENVELOPE")

        accepted = validate_results(
            data,
            row["batch_id"],
            proposals,
        )
        log_event(
            logging.INFO,
            "continuation_validated",
            task_id=task_id,
            result_count=len(proposals),
            accepted_count=len(accepted),
            rejected_count=len(proposals) - len(accepted),
        )

        receipts = []

        for prop, nonce in accepted:

            p = json.loads(prop["proposal_json"])

            receipt = {
                "packageId": p["packageId"],
                "actionId": p["actionId"],
                "action": p["action"],
                "receiptNonce": nonce,
                "facts": p["facts"],
                "evidenceRefs": p["evidenceRefs"],
            }

            record_execution(c, task_id, receipt)
            receipts.append(receipt)

        artifacts = json.loads(row["artifacts_json"])

        artifacts.append(
            artifact_of(
                "invoice-action-receipts",
                RECEIPTS_MT,
                {
                    "batchId": row["batch_id"],
                    "executions": receipts,
                },
            )
        )

        history = json.loads(row["history_json"]) + [
            with_ids(msg, task_id, row["context_id"])
        ]

        cur = c.execute(
            """
            UPDATE tasks
            SET state=%s,
                history_json=%s,
                artifacts_json=%s,
                updated_at=%s
            WHERE task_id=%s AND state=%s
            """,
            (
                COMPLETED,
                canonical(history),
                canonical(artifacts),
                utcnow(),
                task_id,
                INPUT_REQUIRED,
            ),
        )

        if cur.rowcount != 1:
            log_event(logging.WARNING, "task_completion_race_lost", task_id=task_id)
            raise ApiError(409, "TASK_NOT_CONTINUABLE")

        c.execute(
            """
            INSERT INTO idempotency
            VALUES (%s,%s,%s,%s)
            """,
            (
                principal,
                msg["messageId"],
                semantic,
                task_id,
            ),
        )

    log_event(logging.INFO, "task_completed", task_id=task_id, receipt_count=len(receipts))
    return read_task(principal, task_id)


# ---------- cancel ----------

def run_cancel(
    principal: str,
    task_id: str,
) -> dict:

    log_event(logging.INFO, "task_cancel_requested", task_id=task_id)
    with store.transaction() as c:

        load_owned(c, principal, task_id)

        cur = c.execute(
            """
            UPDATE tasks
            SET state=%s, updated_at=%s
            WHERE task_id=%s AND state=%s
            """,
            (
                CANCELED,
                utcnow(),
                task_id,
                INPUT_REQUIRED,
            ),
        )

        if cur.rowcount != 1:
            log_event(logging.WARNING, "task_cancel_rejected", task_id=task_id, reason="terminal_or_not_cancelable")
            raise ApiError(409, "TASK_NOT_CANCELABLE")

    log_event(logging.INFO, "task_canceled", task_id=task_id)
    return read_task(principal, task_id)


# ---------- routes ----------

@app.get("/.well-known/agent-card.json")
def agent_card():
    return JSONResponse(CARD)


router = APIRouter(prefix=PREFIX)


@router.post("/message:send")
async def message_send(request: Request):

    principal = authenticate(request)
    require_protocol(request)
    require_json(request)

    body = await read_body(request)

    try:
        task = await asyncio.wait_for(
            run_in_threadpool(handle_send, principal, body),
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
    except asyncio.TimeoutError:
        raise ApiError(504, "REQUEST_TIMEOUT")

    return a2a({"task": task})


@router.get("/tasks/{task_id}")
def task_get(
    task_id: str,
    request: Request,
):

    principal = authenticate(request)
    require_protocol(request)

    return a2a(read_task(principal, task_id))


@router.get("/tasks")
def task_list(request: Request):

    principal = authenticate(request)
    require_protocol(request)

    rows = store.all(
        """
        SELECT *
        FROM tasks
        WHERE principal=?
        ORDER BY updated_at DESC
        LIMIT 200
        """,
        (principal,),
    )

    log_event(logging.INFO, "task_list_read", task_count=len(rows))

    return a2a({
        "tasks": [
            task_from_row(r)
            for r in rows
        ]
    })


@router.post("/tasks/{task_id}:cancel")
def task_cancel(
    task_id: str,
    request: Request,
):

    principal = authenticate(request)
    require_protocol(request)

    return a2a(run_cancel(principal, task_id))


app.include_router(router)
