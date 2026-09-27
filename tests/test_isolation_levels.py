"""Control experiment: why READ COMMITTED + a row lock, and not something else.

These tests bypass the API and run four withdrawal strategies directly
against Postgres, with the same race: 20 concurrent withdrawals of 100 from
an account holding 1,000 (so at most 10 may succeed). A pg_sleep between
"read balance" and "write debit" makes the race window wide and the
outcomes deterministic.

  strategy                               result
  -------------------------------------  ---------------------------------
  READ COMMITTED, no lock                OVERDRAWS (lost update)
  REPEATABLE READ + row lock             OVERDRAWS (stale snapshot!)
  READ COMMITTED + row lock  (ours)      correct
  SERIALIZABLE, no lock                  correct, but aborts transactions
                                         that the client must retry

The broken rows matter as much as the correct ones. They prove this
harness actually produces the race, so the passing tests elsewhere are
meaningful, and they show that a "stronger" isolation level is not
automatically safer: under REPEATABLE READ the snapshot is taken at the
first statement, *before* waiting for the lock, so after acquiring the lock
the SUM still cannot see the entries the previous holder committed.

The experiment's connections run with session_replication_role=replica,
which switches off the ledger's triggers (including the balance_after
trigger that would otherwise lock the account and refuse the overdraft
itself). That isolates the strategy under test; the real service always
runs with the triggers on. Every test here is marked corrupts_ledger
because the skipped triggers leave balance_after unset.
"""

import threading
import uuid

import psycopg
import pytest
from psycopg import IsolationLevel, errors

from conftest import TEST_DB_URL

START_BALANCE = 1_000
AMOUNT = 100
WORKERS = 20


def _withdraw(isolation: IsolationLevel, lock: bool, src: str, dst: str) -> str:
    with psycopg.connect(TEST_DB_URL, options="-c session_replication_role=replica") as conn:
        conn.isolation_level = isolation
        try:
            with conn.transaction():
                if lock:
                    conn.execute("SELECT 1 FROM accounts WHERE id = %s FOR NO KEY UPDATE", (src,))
                bal = conn.execute(
                    "SELECT COALESCE(SUM(amount), 0) FROM entries WHERE account_id = %s", (src,)
                ).fetchone()[0]
                conn.execute("SELECT pg_sleep(0.05)")  # the race window
                if bal < AMOUNT:
                    return "rejected"
                tid = uuid.uuid4()
                conn.execute(
                    "INSERT INTO transfers (id, idempotency_key, request_hash, kind, from_account_id,"
                    " to_account_id, amount, currency, status)"
                    " VALUES (%s, %s, 'x', 'transfer', %s, %s, %s, 'USD', 'completed')",
                    (tid, str(tid), src, dst, AMOUNT),
                )
                conn.execute(
                    "INSERT INTO entries (transfer_id, account_id, amount) VALUES (%s,%s,%s),(%s,%s,%s)",
                    (tid, src, -AMOUNT, tid, dst, AMOUNT),
                )
            return "completed"
        except errors.SerializationFailure:
            return "serialization_failure"


def _run_race(api, isolation: IsolationLevel, lock: bool):
    src = api.funded_account(START_BALANCE)
    dst = api.account()
    results: list[str] = []
    barrier = threading.Barrier(WORKERS)

    def worker():
        barrier.wait()
        results.append(_withdraw(isolation, lock, src, dst))

    threads = [threading.Thread(target=worker) for _ in range(WORKERS)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    with psycopg.connect(TEST_DB_URL) as conn:
        balance = conn.execute(
            "SELECT COALESCE(SUM(amount), 0) FROM entries WHERE account_id = %s", (src,)
        ).fetchone()[0]
    return balance, results


@pytest.mark.corrupts_ledger
def test_read_committed_without_lock_overdraws(api):
    balance, results = _run_race(api, IsolationLevel.READ_COMMITTED, lock=False)
    assert balance < 0, "expected the naive strategy to overdraw"
    assert results.count("completed") > START_BALANCE // AMOUNT


@pytest.mark.corrupts_ledger
def test_repeatable_read_with_lock_still_overdraws(api):
    balance, results = _run_race(api, IsolationLevel.REPEATABLE_READ, lock=True)
    assert balance < 0, "expected the stale RR snapshot to overdraw despite the lock"


@pytest.mark.corrupts_ledger
def test_read_committed_with_lock_is_correct(api):
    balance, results = _run_race(api, IsolationLevel.READ_COMMITTED, lock=True)
    assert balance == 0
    assert results.count("completed") == START_BALANCE // AMOUNT
    assert results.count("rejected") == WORKERS - START_BALANCE // AMOUNT


@pytest.mark.corrupts_ledger
def test_serializable_is_correct_but_aborts(api):
    balance, results = _run_race(api, IsolationLevel.SERIALIZABLE, lock=False)
    assert balance >= 0
    # Correctness is bought with aborts the application would have to retry.
    assert results.count("serialization_failure") > 0
