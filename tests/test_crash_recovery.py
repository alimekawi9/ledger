"""Scenarios 3 & 4: the process dies mid-request.

The server is started with a failpoint that SIGKILLs it at an exact line of
the transfer code. SIGKILL cannot be caught: no `finally`, no rollback
issued by Python, no graceful shutdown. It's what the OOM killer or a
pulled power cord does to the process.

What saves us is that the transfer is ONE Postgres transaction. When the
process dies, its TCP connection closes; Postgres sees the dead session and
rolls back everything that transaction wrote. The database, not the app, is
the source of atomicity.

There are two interesting crash points, with different correct outcomes:

  after_debit   crash BEFORE commit  -> nothing happened; retry does the work
  after_commit  crash AFTER commit   -> it happened, response was lost;
                                        retry must REPLAY, not redo
"""

import time

import httpx
import pytest

from conftest import Api, LedgerServer


def _post_expecting_crash(srv: LedgerServer, payload: dict, key: str) -> None:
    with pytest.raises(httpx.TransportError):  # connection dropped, no response
        httpx.post(f"{srv.url}/transfers", json=payload, headers={"Idempotency-Key": key}, timeout=10)
    assert srv.wait_for_exit() == -9, "server should have died from SIGKILL"
    srv.stop()


def _wait_for_no_open_transactions(db, timeout=5.0):
    """The dead process's backend may take a moment to notice the closed
    socket. Wait until no session is still 'idle in transaction'."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        n = db.execute(
            "SELECT count(*) FROM pg_stat_activity "
            "WHERE datname = current_database() AND state LIKE 'idle in transaction%%'"
        ).fetchone()[0]
        if n == 0:
            return
        time.sleep(0.05)
    raise AssertionError("an orphaned transaction is still open")


def test_crash_between_debit_and_credit_rolls_back_completely(api, db):
    alice = api.funded_account(1_000, "alice")
    bob = api.account("bob")
    entries_before = db.execute("SELECT count(*) FROM entries").fetchone()[0]
    payload = {"from_account_id": alice, "to_account_id": bob, "amount": 400}

    # 1. Crash after the debit INSERT, before the credit INSERT.
    doomed = LedgerServer(failpoints="after_debit=crash")
    _post_expecting_crash(doomed, payload, key="crash-mid-transfer")
    _wait_for_no_open_transactions(db)

    # 2. Nothing from the dead transaction survived: no half transfer, no
    #    orphaned debit, not even the idempotency key claim.
    assert db.execute("SELECT count(*) FROM entries").fetchone()[0] == entries_before
    assert db.execute(
        "SELECT count(*) FROM transfers WHERE idempotency_key = 'crash-mid-transfer'"
    ).fetchone()[0] == 0
    assert api.balance(alice) == 1_000
    assert api.balance(bob) == 0

    # 3. The client retries with the same key against a restarted server.
    #    Because the key claim rolled back too, the retry does the work,
    #    and a second retry replays it.
    retry = api.transfer(alice, bob, 400, key="crash-mid-transfer")
    assert retry.status_code == 201
    assert "Idempotent-Replayed" not in retry.headers
    again = api.transfer(alice, bob, 400, key="crash-mid-transfer")
    assert again.headers["Idempotent-Replayed"] == "true"
    assert again.json()["id"] == retry.json()["id"]

    assert api.balance(alice) == 600
    assert api.balance(bob) == 400


def test_crash_after_commit_before_response_is_not_reprocessed(api, db):
    """The dangerous case for naive retry logic: the money moved, but the
    client never heard back. A retry without idempotency would move it
    twice."""
    alice = api.funded_account(1_000, "alice")
    bob = api.account("bob")
    payload = {"from_account_id": alice, "to_account_id": bob, "amount": 400}

    doomed = LedgerServer(failpoints="after_commit=crash")
    _post_expecting_crash(doomed, payload, key="lost-response")

    # The transfer DID commit.
    status = db.execute(
        "SELECT status FROM transfers WHERE idempotency_key = 'lost-response'"
    ).fetchone()
    assert status == ("completed",)
    assert api.balance(alice) == 600

    # The client, having seen only a dropped connection, retries.
    retry = api.transfer(alice, bob, 400, key="lost-response")
    assert retry.status_code == 201
    assert retry.headers["Idempotent-Replayed"] == "true"

    assert api.balance(alice) == 600, "retry must not debit a second time"
    assert api.balance(bob) == 400


def test_crash_while_holding_account_lock_does_not_block_others(api, db, server):
    """A transaction that dies holding the source account's row lock must
    not wedge that account: Postgres releases the lock with the rollback.
    The crash test above covers the ledger contents; this one covers
    liveness for *other* clients of the same account."""
    alice = api.funded_account(1_000)
    bob = api.account()

    doomed = LedgerServer(failpoints="after_debit=crash")
    _post_expecting_crash(
        doomed, {"from_account_id": alice, "to_account_id": bob, "amount": 1}, key="dies"
    )

    start = time.monotonic()
    r = Api(server.url).transfer(alice, bob, 1)
    assert r.status_code == 201
    assert time.monotonic() - start < 5
    assert api.balance(alice) == 999


@pytest.mark.parametrize("failpoint", ["after_debit", "after_commit"])
def test_crash_then_concurrent_retries(api, server, failpoint):
    """Worst realistic case: after the crash, the client (or several client
    replicas, or a queue redelivering) retries many times at once. Exactly
    one transfer must result either way."""
    from conftest import fire_concurrently, transfer_request

    alice = api.funded_account(1_000)
    bob = api.account()
    payload = {"from_account_id": alice, "to_account_id": bob, "amount": 250}

    doomed = LedgerServer(failpoints=f"{failpoint}=crash")
    _post_expecting_crash(doomed, payload, key="storm")

    responses = fire_concurrently(server.url, [transfer_request(alice, bob, 250, key="storm")] * 30)
    assert {r.status_code for r in responses} == {201}
    assert len({r.json()["id"] for r in responses}) == 1
    assert api.balance(alice) == 750
    assert api.balance(bob) == 250
