"""Scenario 5: the database itself refuses to hold an invalid ledger.

Application code has bugs. These tests go around the application and
write directly to Postgres, proving the core invariants hold even then.
"""

import uuid

import psycopg
import pytest
from psycopg import errors

from conftest import TEST_DB_URL


def _transfer_row(conn, src, dst, amount):
    tid = uuid.uuid4()
    conn.execute(
        "INSERT INTO transfers (id, idempotency_key, request_hash, kind, from_account_id,"
        " to_account_id, amount, currency, status)"
        " VALUES (%s, %s, 'x', 'transfer', %s, %s, %s, 'USD', 'completed')",
        (tid, str(tid), src, dst, amount),
    )
    return tid


def test_single_legged_transfer_cannot_commit(api):
    alice = api.funded_account(1_000)
    bob = api.account()
    with psycopg.connect(TEST_DB_URL) as conn:
        with pytest.raises(errors.CheckViolation, match="unbalanced"):
            with conn.transaction():
                tid = _transfer_row(conn, alice, bob, 100)
                conn.execute(
                    "INSERT INTO entries (transfer_id, account_id, amount) VALUES (%s, %s, -100)",
                    (tid, alice),
                )
                # No credit leg. The deferred trigger fires at COMMIT.
    assert api.balance(alice) == 1_000


def test_mismatched_legs_cannot_commit(api):
    alice = api.funded_account(1_000)
    bob = api.account()
    with psycopg.connect(TEST_DB_URL) as conn:
        with pytest.raises(errors.CheckViolation):
            with conn.transaction():
                tid = _transfer_row(conn, alice, bob, 100)
                conn.execute(
                    "INSERT INTO entries (transfer_id, account_id, amount)"
                    " VALUES (%s, %s, -100), (%s, %s, 99)",
                    (tid, alice, tid, bob),
                )


def test_balanced_legs_may_be_written_in_separate_statements(api):
    """Deferral is what lets the service insert debit and credit as two
    statements: the check waits until COMMIT."""
    alice = api.funded_account(1_000)
    bob = api.account()
    with psycopg.connect(TEST_DB_URL) as conn, conn.transaction():
        tid = _transfer_row(conn, alice, bob, 100)
        conn.execute("INSERT INTO entries (transfer_id, account_id, amount) VALUES (%s,%s,-100)", (tid, alice))
        conn.execute("INSERT INTO entries (transfer_id, account_id, amount) VALUES (%s,%s,100)", (tid, bob))
    assert api.balance(alice) == 900


@pytest.mark.parametrize("stmt", [
    "UPDATE entries SET amount = amount * 2",
    "DELETE FROM entries",
])
def test_entries_are_append_only(api, db, stmt):
    api.funded_account(1_000)
    with pytest.raises(errors.InsufficientPrivilege, match="append-only"):
        db.execute(stmt)


def test_no_mutable_balance_exists(db):
    """The design rule, checked mechanically: the only balance-like column
    is entries.balance_after, which lives on append-only rows, so it is
    written once and can never be edited."""
    cols = db.execute(
        "SELECT table_name, column_name FROM information_schema.columns "
        "WHERE table_schema = 'public' AND column_name ILIKE '%%balance%%'"
    ).fetchall()
    assert cols == [("entries", "balance_after")]


def _raw_transfer(conn, src, dst, amount, **entry_extra):
    tid = _transfer_row(conn, src, dst, amount)
    cols = ", balance_after" if entry_extra else ""
    vals = ", %s" if entry_extra else ""
    extra = (entry_extra.get("balance_after"),) if entry_extra else ()
    conn.execute(
        f"INSERT INTO entries (transfer_id, account_id, amount{cols}) VALUES (%s, %s, %s{vals})",
        (tid, src, -amount, *extra),
    )
    conn.execute(
        f"INSERT INTO entries (transfer_id, account_id, amount{cols}) VALUES (%s, %s, %s{vals})",
        (tid, dst, amount, *extra),
    )
    return tid


def test_database_refuses_overdraft_even_bypassing_the_app(api):
    """The app checks funds before debiting, but the database would refuse
    anyway: the balance_after trigger raises if a customer balance would go
    below zero. Two independent guards."""
    alice = api.funded_account(100)
    bob = api.account()
    with psycopg.connect(TEST_DB_URL) as conn:
        with pytest.raises(errors.CheckViolation, match="overdrawn"):
            with conn.transaction():
                _raw_transfer(conn, alice, bob, 101)
    assert api.balance(alice) == 100


def test_client_cannot_forge_balance_after(api, db):
    """Whatever balance_after the writer supplies, the trigger overwrites it
    with the true running balance."""
    alice = api.funded_account(1_000)
    bob = api.account()
    with psycopg.connect(TEST_DB_URL) as conn, conn.transaction():
        tid = _raw_transfer(conn, alice, bob, 100, balance_after=999_999)
    rows = db.execute(
        "SELECT account_id::text, balance_after FROM entries WHERE transfer_id = %s ORDER BY amount",
        (tid,),
    ).fetchall()
    assert rows == [(alice, 900), (bob, 100)]


def test_ledger_writes_refused_outside_read_committed(api):
    """Under REPEATABLE READ the trigger could read a stale previous balance
    (snapshot taken before the lock wait) and fork the chain, so it refuses
    to run at all."""
    alice = api.funded_account(1_000)
    bob = api.account()
    with psycopg.connect(TEST_DB_URL) as conn:
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        with pytest.raises(errors.InvalidTransactionState, match="READ COMMITTED"):
            with conn.transaction():
                _raw_transfer(conn, alice, bob, 1)
    assert api.balance(alice) == 1_000


def test_running_balance_survives_writers_queued_on_the_lock(api, db):
    """Regression test for a real bug found by mutation testing.

    Postgres assigns an entry's id before BEFORE-INSERT triggers run, so a
    writer that queues on the account lock inside the trigger used to keep
    an id LOWER than the entry it ends up building on. "Newest entry =
    highest id" then skipped it, and the balance silently lost money.

    Reproduced deterministically with raw SQL (no service-side locks):
      1. `holder` locks the account.
      2. `queued` inserts a credit; its trigger blocks on the lock
         (and, before the fix, had already drawn its id).
      3. `holder` inserts its own credit and commits.
      4. `queued` gets the lock, finishes, commits.
    """
    import threading
    import time

    acct = api.account()
    external = db.execute(
        "SELECT id::text FROM accounts WHERE kind = 'external' AND currency = 'USD'"
    ).fetchone()[0]

    holder = psycopg.connect(TEST_DB_URL)
    holder.execute("SELECT 1 FROM accounts WHERE id = %s FOR NO KEY UPDATE", (acct,))

    errors_in_thread = []

    def queued_writer():
        try:
            with psycopg.connect(TEST_DB_URL) as conn, conn.transaction():
                _raw_transfer(conn, external, acct, 5)
        except Exception as e:  # surfaced by the assert below
            errors_in_thread.append(e)

    t = threading.Thread(target=queued_writer)
    t.start()
    time.sleep(0.5)  # let it block inside the trigger
    assert t.is_alive(), "queued writer should be waiting on the account lock"
    _raw_transfer(holder, external, acct, 7)
    holder.commit()
    holder.close()
    t.join(10)
    assert not errors_in_thread, errors_in_thread

    assert api.balance(acct) == 12  # both credits, via newest balance_after
    # (the autouse invariant check also verifies the whole chain)
