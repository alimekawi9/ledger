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


def test_no_balance_column_exists(db):
    """The design rule, checked mechanically: nothing in the schema stores
    a balance that could drift from the entries."""
    cols = db.execute(
        "SELECT table_name, column_name FROM information_schema.columns "
        "WHERE table_schema = 'public' AND column_name ILIKE '%%balance%%'"
    ).fetchall()
    assert cols == []
