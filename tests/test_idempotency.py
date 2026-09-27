"""Scenario 1: duplicate requests (retries) must never process twice.

In a distributed system a client cannot distinguish "my request was lost"
from "my request succeeded but the response was lost". The only safe
reaction to a timeout is to retry, so the server must make retries
harmless: same Idempotency-Key => same result, applied at most once.
"""

import pytest

from conftest import fire_concurrently, transfer_request


def test_sequential_duplicate_returns_original_result(api):
    alice = api.funded_account(1_000)
    bob = api.account()

    first = api.transfer(alice, bob, 300, key="pay-invoice-42")
    second = api.transfer(alice, bob, 300, key="pay-invoice-42")

    assert first.status_code == second.status_code == 201
    assert second.json() == first.json()  # byte-for-byte the same transfer
    assert "Idempotent-Replayed" not in first.headers
    assert second.headers["Idempotent-Replayed"] == "true"
    assert api.balance(alice) == 700  # debited once, not twice
    assert api.balance(bob) == 300


@pytest.mark.parametrize("n_duplicates", [2, 25, 100])
def test_concurrent_duplicates_process_exactly_once(api, db, server, n_duplicates):
    """N copies of one request arrive at the same instant.

    Mechanism under test: all N transactions INSERT the same idempotency key.
    Postgres lets one proceed; the others block on the unique index until it
    commits, then see the conflict and replay the stored result.
    """
    alice = api.funded_account(1_000)
    bob = api.account()
    req = transfer_request(alice, bob, 250, key="dup-key")

    responses = fire_concurrently(server.url, [req] * n_duplicates)

    assert {r.status_code for r in responses} == {201}
    assert len({r.json()["id"] for r in responses}) == 1, "all callers see the same transfer"
    fresh = [r for r in responses if "Idempotent-Replayed" not in r.headers]
    assert len(fresh) == 1, "exactly one request did the work"

    assert db.execute("SELECT count(*) FROM transfers WHERE idempotency_key = 'dup-key'").fetchone()[0] == 1
    assert db.execute(
        "SELECT count(*) FROM entries e JOIN transfers t ON t.id = e.transfer_id "
        "WHERE t.idempotency_key = 'dup-key'"
    ).fetchone()[0] == 2
    assert api.balance(alice) == 750
    assert api.balance(bob) == 250


def test_concurrent_duplicates_of_a_rejected_transfer(api, server):
    """A rejection is an outcome too: retries get the same rejection."""
    alice = api.funded_account(100)
    bob = api.account()
    req = transfer_request(alice, bob, 500, key="too-big")

    responses = fire_concurrently(server.url, [req] * 20)

    assert {r.status_code for r in responses} == {422}
    assert len({r.json()["id"] for r in responses}) == 1
    assert all(r.json()["failure_reason"] == "insufficient_funds" for r in responses)
    assert api.balance(alice) == 100


def test_rejected_result_is_sticky_even_after_funds_arrive(api):
    """The key identifies one *attempt*. A retry replays its outcome; it does
    not silently turn into a new attempt (that needs a new key). Otherwise a
    client retrying an old request could move money long after giving up."""
    alice = api.funded_account(100)
    bob = api.account()
    assert api.transfer(alice, bob, 500, key="k").status_code == 422
    api.deposit(alice, 1_000)
    retry = api.transfer(alice, bob, 500, key="k")
    assert retry.status_code == 422
    assert retry.headers["Idempotent-Replayed"] == "true"
    assert api.balance(alice) == 1_100


def test_key_reused_with_different_payload_is_refused(api):
    """Reusing a key for a different request is a client bug. Replaying the
    old result would hide it; processing it would violate the key's meaning.
    The request fingerprint lets the server refuse loudly (409)."""
    alice = api.funded_account(1_000)
    bob = api.account()
    carol = api.account()
    assert api.transfer(alice, bob, 100, key="k").status_code == 201

    for src, dst, amt in [(alice, bob, 101), (alice, carol, 100)]:
        r = api.transfer(src, dst, amt, key="k")
        assert r.status_code == 409
        assert r.json()["error"] == "idempotency_key_reused"

    assert api.balance(alice) == 900
    assert api.balance(carol) == 0


def test_same_key_different_payload_concurrently(api, server, db):
    """Two *different* requests race on one key: exactly one wins; the
    loser gets 409, never a mixed or doubled result."""
    alice = api.funded_account(1_000)
    bob = api.account()
    reqs = [transfer_request(alice, bob, amt, key="contested") for amt in (100, 200)] * 10

    responses = fire_concurrently(server.url, reqs)

    codes = sorted(r.status_code for r in responses)
    winners = [r for r in responses if r.status_code == 201]
    winning_amount = winners[0].json()["amount"]
    assert all(r.json()["amount"] == winning_amount for r in winners)
    assert codes.count(409) == 10  # every request carrying the other amount
    assert db.execute("SELECT count(*) FROM transfers WHERE idempotency_key='contested'").fetchone()[0] == 1
    assert api.balance(alice) == 1_000 - winning_amount
