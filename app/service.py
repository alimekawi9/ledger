"""Transfer processing: double-entry, idempotent, race-free, crash-atomic.

The whole transfer is ONE database transaction:

    BEGIN
      1. validate accounts exist / currencies match            (no locks)
      2. INSERT transfer row ... ON CONFLICT (idempotency_key) DO NOTHING
           -> if the key already exists: return the stored result (replay)
      3. SELECT source account ... FOR NO KEY UPDATE           (row lock)
      4. SELECT SUM(amount) FROM entries WHERE account = source
           -> insufficient funds: mark transfer 'rejected', COMMIT
      5. INSERT debit entry  (-amount, source)
      6. INSERT credit entry (+amount, destination)
      7. UPDATE transfer SET status = 'completed'
    COMMIT   <- deferred trigger verifies entries sum to zero

Why each step is where it is:

* Step 2 before step 3 ("claim the key first"). Two concurrent requests with
  the same key both try to INSERT the same unique value. Postgres makes the
  second INSERT *wait* on the unique index until the first transaction ends.
  If the first commits, the second hits the conflict and replays; if the
  first rolls back (e.g. crash), the second INSERT succeeds and it does the
  work. Duplicates never reach the account lock.

* Step 3 before step 4 ("lock, then read"). Without the lock, two transfers
  could both read balance=100, both decide 100 >= 100, and both debit:
  a lost update / overdraft. With the lock, the second waits until the first
  commits.

* READ COMMITTED, not REPEATABLE READ. Under READ COMMITTED every statement
  takes a fresh snapshot, so the SUM in step 4 (run *after* we waited for
  the lock) sees the entries the previous lock holder committed. Under
  REPEATABLE READ the snapshot is fixed at the first statement, i.e. before
  we waited, so the SUM would miss them and overdraw anyway.
  tests/test_isolation_levels.py demonstrates this.

* FOR NO KEY UPDATE, not FOR UPDATE. Inserting a transfer/entry row takes a
  FOR KEY SHARE lock on the referenced account (foreign key check). FOR
  UPDATE conflicts with KEY SHARE, which deadlocks two concurrent transfers
  from the same account. NO KEY UPDATE does not conflict with KEY SHARE.

* Only the *source* account is locked. The only invariant at risk is
  "customer balance never negative", and only a debit can break it. Locking
  one row per transaction also makes deadlocks between transfers impossible
  (a cycle needs two locks), which tests/test_concurrency.py checks with
  opposite-direction A<->B traffic. The external (funding) account may go
  negative, so it is not locked at all; otherwise every deposit would
  serialize on that single row.
"""

import hashlib
import json
import uuid
from dataclasses import dataclass
from typing import Any

from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

from app.failpoints import failpoint


class LedgerError(Exception):
    status_code = 400
    code = "bad_request"

    def __init__(self, message: str):
        super().__init__(message)
        self.message = message


class NotFound(LedgerError):
    status_code = 404
    code = "not_found"


class InvalidTransfer(LedgerError):
    status_code = 422
    code = "invalid_transfer"


class IdempotencyKeyReused(LedgerError):
    status_code = 409
    code = "idempotency_key_reused"


@dataclass
class TransferOutcome:
    transfer: dict[str, Any]
    replayed: bool


def request_fingerprint(kind: str, from_id: uuid.UUID, to_id: uuid.UUID, amount: int) -> str:
    canonical = json.dumps(
        {"kind": kind, "from": str(from_id), "to": str(to_id), "amount": amount},
        sort_keys=True,
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


# ---------------------------------------------------------------- accounts


async def create_account(pool: AsyncConnectionPool, name: str, currency: str) -> dict:
    account_id = uuid.uuid4()
    async with pool.connection() as conn, conn.transaction():
        # Lazily create the currency's external (funding) account.
        await conn.execute(
            """
            INSERT INTO accounts (id, name, currency, kind)
            VALUES (%s, %s, %s, 'external')
            ON CONFLICT (currency) WHERE kind = 'external' DO NOTHING
            """,
            (uuid.uuid4(), f"external:{currency}", currency),
        )
        cur = await conn.execute(
            """
            INSERT INTO accounts (id, name, currency)
            VALUES (%s, %s, %s)
            RETURNING id, name, currency, kind, created_at
            """,
            (account_id, name, currency),
        )
        return await cur.fetchone()


async def get_account(pool: AsyncConnectionPool, account_id: uuid.UUID) -> dict:
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT id, name, currency, kind, created_at FROM accounts WHERE id = %s",
            (account_id,),
        )
        row = await cur.fetchone()
    if row is None:
        raise NotFound(f"account {account_id} not found")
    return row


async def _balance(conn: AsyncConnection, account_id: uuid.UUID) -> int:
    cur = await conn.execute(
        "SELECT COALESCE(SUM(amount), 0)::bigint AS balance FROM entries WHERE account_id = %s",
        (account_id,),
    )
    return (await cur.fetchone())["balance"]


async def get_balance(pool: AsyncConnectionPool, account_id: uuid.UUID) -> dict:
    account = await get_account(pool, account_id)
    async with pool.connection() as conn:
        balance = await _balance(conn, account_id)
    return {"account_id": account_id, "currency": account["currency"], "balance": balance}


async def get_history(
    pool: AsyncConnectionPool, account_id: uuid.UUID, limit: int, before_id: int | None
) -> list[dict]:
    await get_account(pool, account_id)
    async with pool.connection() as conn:
        cur = await conn.execute(
            """
            SELECT e.id AS entry_id, e.transfer_id, e.amount,
                   CASE WHEN e.amount < 0 THEN 'debit' ELSE 'credit' END AS direction,
                   CASE WHEN e.amount < 0 THEN t.to_account_id ELSE t.from_account_id END
                       AS counterparty_account_id,
                   t.kind, e.created_at
              FROM entries e JOIN transfers t ON t.id = e.transfer_id
             WHERE e.account_id = %s AND (%s::bigint IS NULL OR e.id < %s)
             ORDER BY e.id DESC
             LIMIT %s
            """,
            (account_id, before_id, before_id, limit),
        )
        return await cur.fetchall()


# --------------------------------------------------------------- transfers

_TRANSFER_COLUMNS = (
    "id, idempotency_key, kind, from_account_id, to_account_id, amount, "
    "currency, status, failure_reason, created_at"
)


async def create_transfer(
    pool: AsyncConnectionPool,
    *,
    idempotency_key: str,
    from_account_id: uuid.UUID,
    to_account_id: uuid.UUID,
    amount: int,
    kind: str = "transfer",
) -> TransferOutcome:
    if from_account_id == to_account_id:
        raise InvalidTransfer("from_account_id and to_account_id must differ")
    fingerprint = request_fingerprint(kind, from_account_id, to_account_id, amount)

    async with pool.connection() as conn:
        async with conn.transaction():
            # 1. Validate (unlocked read; accounts are immutable).
            cur = await conn.execute(
                "SELECT id, currency, kind FROM accounts WHERE id = ANY(%s)",
                ([from_account_id, to_account_id],),
            )
            accounts = {row["id"]: row for row in await cur.fetchall()}
            for acct_id in (from_account_id, to_account_id):
                if acct_id not in accounts:
                    raise NotFound(f"account {acct_id} not found")
            source, dest = accounts[from_account_id], accounts[to_account_id]
            if source["currency"] != dest["currency"]:
                raise InvalidTransfer("accounts have different currencies")

            # 2. Claim the idempotency key.
            cur = await conn.execute(
                f"""
                INSERT INTO transfers (id, idempotency_key, request_hash, kind,
                    from_account_id, to_account_id, amount, currency, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'pending')
                ON CONFLICT (idempotency_key) DO NOTHING
                RETURNING {_TRANSFER_COLUMNS}
                """,
                (uuid.uuid4(), idempotency_key, fingerprint, kind,
                 from_account_id, to_account_id, amount, source["currency"]),
            )
            transfer = await cur.fetchone()
            if transfer is None:
                return await _replay(conn, idempotency_key, fingerprint)

            # 3 + 4. Lock the source, then read its balance.
            if source["kind"] != "external":
                await conn.execute(
                    "SELECT 1 FROM accounts WHERE id = %s FOR NO KEY UPDATE",
                    (from_account_id,),
                )
                balance = await _balance(conn, from_account_id)
                await failpoint("after_balance_check")
                if balance < amount:
                    transfer = await _finish(conn, transfer["id"], "rejected", "insufficient_funds")
                    return TransferOutcome(transfer, replayed=False)

            # 5 + 6. The two legs. Either both commit or neither does.
            await conn.execute(
                "INSERT INTO entries (transfer_id, account_id, amount) VALUES (%s, %s, %s)",
                (transfer["id"], from_account_id, -amount),
            )
            await failpoint("after_debit")
            await conn.execute(
                "INSERT INTO entries (transfer_id, account_id, amount) VALUES (%s, %s, %s)",
                (transfer["id"], to_account_id, amount),
            )
            # 7.
            transfer = await _finish(conn, transfer["id"], "completed", None)
        # The transaction has committed here. Crashing now loses only the
        # HTTP response; the client's retry will get a replay.
        await failpoint("after_commit")
    return TransferOutcome(transfer, replayed=False)


async def _finish(conn: AsyncConnection, transfer_id: uuid.UUID, status: str, reason: str | None) -> dict:
    cur = await conn.execute(
        f"UPDATE transfers SET status = %s, failure_reason = %s WHERE id = %s RETURNING {_TRANSFER_COLUMNS}",
        (status, reason, transfer_id),
    )
    return await cur.fetchone()


async def _replay(conn: AsyncConnection, idempotency_key: str, fingerprint: str) -> TransferOutcome:
    # READ COMMITTED: this new statement sees the row the conflicting
    # transaction committed (ON CONFLICT waited for it to finish).
    cur = await conn.execute(
        f"SELECT {_TRANSFER_COLUMNS}, request_hash FROM transfers WHERE idempotency_key = %s",
        (idempotency_key,),
    )
    existing = await cur.fetchone()
    if existing["request_hash"] != fingerprint:
        raise IdempotencyKeyReused(
            "this Idempotency-Key was already used for a different request"
        )
    del existing["request_hash"]
    # 'pending' is never committed, so a visible row is always final.
    assert existing["status"] in ("completed", "rejected"), existing
    return TransferOutcome(existing, replayed=True)


async def create_deposit(
    pool: AsyncConnectionPool, *, idempotency_key: str, account_id: uuid.UUID, amount: int
) -> TransferOutcome:
    """A deposit is a transfer from the currency's external account."""
    account = await get_account(pool, account_id)
    if account["kind"] != "customer":
        raise InvalidTransfer("deposits must target a customer account")
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT id FROM accounts WHERE kind = 'external' AND currency = %s",
            (account["currency"],),
        )
        external_id = (await cur.fetchone())["id"]
    return await create_transfer(
        pool,
        idempotency_key=idempotency_key,
        from_account_id=external_id,
        to_account_id=account_id,
        amount=amount,
        kind="deposit",
    )
