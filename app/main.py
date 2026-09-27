import logging
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Annotated, Literal

from fastapi import FastAPI, Header, Query, Request
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, Field

from app import failpoints, service
from app.config import load_settings
from app.db import apply_schema, create_pool
from app.observability import (
    REGISTRY,
    TRANSFER_LATENCY,
    TRANSFERS,
    PoolCollector,
    RequestContextMiddleware,
    configure_logging,
    log_event,
)

logger = logging.getLogger("ledger")

# Amounts are integer minor units (cents). 10^15 cents = $10 trillion: well
# inside BIGINT, and small enough that sums of many entries cannot overflow.
MAX_AMOUNT = 10**15
Currency = Literal["USD", "EUR", "GBP"]


@asynccontextmanager
async def lifespan(app: FastAPI):
    configure_logging()
    failpoints.load_from_env()
    settings = load_settings()
    pool = create_pool(settings)
    await pool.open(wait=True)
    await apply_schema(pool)
    REGISTRY.register(PoolCollector(pool))
    app.state.pool = pool
    log_event(logger, "startup", pool_max_size=settings.pool_max_size)
    try:
        yield
    finally:
        await pool.close()


app = FastAPI(title="Ledger", version="1.0.0", lifespan=lifespan)
app.add_middleware(RequestContextMiddleware)


# ------------------------------------------------------------------ models


class CreateAccount(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    currency: Currency


class Account(BaseModel):
    id: uuid.UUID
    name: str
    currency: str
    kind: str
    created_at: datetime


class CreateTransfer(BaseModel):
    from_account_id: uuid.UUID
    to_account_id: uuid.UUID
    amount: int = Field(gt=0, le=MAX_AMOUNT, description="Minor units (cents).")


class CreateDeposit(BaseModel):
    account_id: uuid.UUID
    amount: int = Field(gt=0, le=MAX_AMOUNT)


class Transfer(BaseModel):
    id: uuid.UUID
    idempotency_key: str
    kind: str
    from_account_id: uuid.UUID
    to_account_id: uuid.UUID
    amount: int
    currency: str
    status: Literal["completed", "rejected"]
    failure_reason: str | None
    created_at: datetime


class Balance(BaseModel):
    account_id: uuid.UUID
    currency: str
    balance: int


class HistoryEntry(BaseModel):
    entry_id: int
    transfer_id: uuid.UUID
    amount: int
    direction: Literal["debit", "credit"]
    counterparty_account_id: uuid.UUID
    kind: str
    created_at: datetime


IdempotencyKey = Annotated[
    str,
    Header(
        alias="Idempotency-Key",
        min_length=1,
        max_length=255,
        description="Client-generated unique key (e.g. a UUID). Retries MUST reuse it.",
    ),
]


@app.exception_handler(service.LedgerError)
async def ledger_error_handler(request: Request, exc: service.LedgerError):
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": exc.code, "message": exc.message},
    )


# ------------------------------------------------------------------ routes


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/metrics")
async def metrics():
    return Response(generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST)


@app.post("/accounts", status_code=201, response_model=Account)
async def create_account(body: CreateAccount, request: Request):
    return await service.create_account(request.app.state.pool, body.name, body.currency)


@app.get("/accounts/{account_id}", response_model=Account)
async def get_account(account_id: uuid.UUID, request: Request):
    return await service.get_account(request.app.state.pool, account_id)


@app.get("/accounts/{account_id}/balance", response_model=Balance)
async def get_balance(account_id: uuid.UUID, request: Request):
    return await service.get_balance(request.app.state.pool, account_id)


@app.get("/accounts/{account_id}/transactions", response_model=list[HistoryEntry])
async def get_transactions(
    account_id: uuid.UUID,
    request: Request,
    limit: int = Query(50, ge=1, le=500),
    before_entry_id: int | None = Query(None, description="Keyset pagination cursor."),
):
    return await service.get_history(request.app.state.pool, account_id, limit, before_entry_id)


@app.post("/transfers", response_model=Transfer, status_code=201,
          responses={422: {"model": Transfer}, 409: {}})
async def create_transfer(body: CreateTransfer, request: Request, idempotency_key: IdempotencyKey):
    return await _run_transfer(
        "transfer",
        idempotency_key,
        {"from": body.from_account_id, "to": body.to_account_id, "amount": body.amount},
        service.create_transfer(
            request.app.state.pool,
            idempotency_key=idempotency_key,
            from_account_id=body.from_account_id,
            to_account_id=body.to_account_id,
            amount=body.amount,
        ),
    )


@app.post("/deposits", response_model=Transfer, status_code=201)
async def create_deposit(body: CreateDeposit, request: Request, idempotency_key: IdempotencyKey):
    return await _run_transfer(
        "deposit",
        idempotency_key,
        {"to": body.account_id, "amount": body.amount},
        service.create_deposit(
            request.app.state.pool,
            idempotency_key=idempotency_key,
            account_id=body.account_id,
            amount=body.amount,
        ),
    )


async def _run_transfer(kind: str, idempotency_key: str, log_fields: dict, work) -> Response:
    """Shared wrapper: runs the transfer, logs one structured line, records metrics."""
    start = time.perf_counter()
    fields = {"kind": kind, "idempotency_key": idempotency_key, **log_fields}
    try:
        outcome = await work
    except service.LedgerError as exc:
        label = "key_reused" if isinstance(exc, service.IdempotencyKeyReused) else "invalid"
        _record(kind, label, start, fields, level=logging.WARNING, error=exc.code)
        raise
    except Exception:
        _record(kind, "error", start, fields, level=logging.ERROR, exc_info=True)
        raise

    t = outcome.transfer
    label = "replayed" if outcome.replayed else t["status"]
    _record(kind, label, start, fields, transfer_id=t["id"], status=t["status"],
            failure_reason=t["failure_reason"])
    body = Transfer.model_validate(t).model_dump(mode="json")
    headers = {"Idempotent-Replayed": "true"} if outcome.replayed else {}
    status_code = 201 if t["status"] == "completed" else 422
    return JSONResponse(body, status_code=status_code, headers=headers)


def _record(kind, outcome, start, fields, level=logging.INFO, exc_info=False, **extra):
    elapsed = time.perf_counter() - start
    TRANSFERS.labels(kind, outcome).inc()
    TRANSFER_LATENCY.labels(kind, outcome).observe(elapsed)
    logger.log(
        level,
        f"{kind}.{outcome}",
        exc_info=exc_info,
        extra={"fields": {**fields, **extra, "outcome": outcome,
                          "latency_ms": round(elapsed * 1000, 2)}},
    )
