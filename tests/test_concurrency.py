"""Scenario 2: concurrent transfers touching the same account.

The classic bug is check-then-act: two requests both read balance=100,
both decide "100 >= 100, fine", both debit. Result: -100. The fix is a
row lock on the source account taken BEFORE the balance is read.
"""

import random

import pytest

from conftest import LedgerServer, fire_concurrently, transfer_request


@pytest.fixture(params=["normal", "widened_race_window"])
def contended_server(request, server):
    """Run each test twice: against the normal server, and against one that
    sleeps 20ms between reading the balance and writing the debit.

    The sleep turns a microsecond race window into a 20ms one. If locking
    were broken, the widened variant would overdraw essentially every run,
    so a pass there is strong evidence, not luck.
    """
    if request.param == "normal":
        yield server
    else:
        srv = LedgerServer(failpoints="after_balance_check=sleep:20")
        yield srv
        srv.stop()


def test_hot_account_never_overdraws(api, contended_server):
    """200 concurrent withdrawals of 100 from an account holding 10,000:
    exactly 100 must succeed, 100 must be rejected, final balance exactly 0."""
    hot = api.funded_account(10_000, "hot")
    sinks = [api.account(f"sink{i}") for i in range(10)]
    reqs = [transfer_request(hot, sinks[i % 10], 100) for i in range(200)]

    responses = fire_concurrently(contended_server.url, reqs)

    codes = [r.status_code for r in responses]
    assert codes.count(201) == 100
    assert codes.count(422) == 100
    assert api.balance(hot) == 0
    assert sum(api.balance(s) for s in sinks) == 10_000


def test_random_traffic_final_balances_match_acknowledged_transfers(api, server):
    """500 concurrent random transfers among 8 accounts (lots of shared
    sources and destinations). Afterwards every balance must equal exactly
    what the *client-visible responses* imply: initial deposit, minus
    acknowledged outgoing transfers, plus acknowledged incoming ones.

    This catches lost updates, double-applies, AND lying responses (a 201
    for something that did not stick, or a 422 for something that did)."""
    rng = random.Random(1234)
    accounts = [api.funded_account(5_000, f"a{i}") for i in range(8)]
    reqs = []
    for _ in range(500):
        src, dst = rng.sample(accounts, 2)
        reqs.append(transfer_request(src, dst, rng.randint(1, 900)))

    responses = fire_concurrently(server.url, reqs)

    assert all(r.status_code in (201, 422) for r in responses), \
        {r.status_code for r in responses}
    expected = {a: 5_000 for a in accounts}
    for r in responses:
        t = r.json()
        if r.status_code == 201:
            expected[t["from_account_id"]] -= t["amount"]
            expected[t["to_account_id"]] += t["amount"]
    for acct in accounts:
        assert api.balance(acct) == expected[acct]
    assert sum(expected.values()) == 8 * 5_000  # money is conserved


def test_opposing_transfers_do_not_deadlock(api, contended_server):
    """A->B and B->A at the same time is the textbook deadlock when each
    transaction locks both accounts in request order (T1 holds A wants B,
    T2 holds B wants A). We lock only the source, so no transaction ever
    holds two account locks and no cycle can form. A deadlock would surface
    here as 500s (Postgres aborts one victim with 'deadlock detected')."""
    a = api.funded_account(100_000, "a")
    b = api.funded_account(100_000, "b")
    reqs = []
    for i in range(300):
        reqs.append(transfer_request(a, b, 7) if i % 2 else transfer_request(b, a, 11))

    responses = fire_concurrently(contended_server.url, reqs)

    assert {r.status_code for r in responses} == {201}
    assert api.balance(a) == 100_000 - 150 * 7 + 150 * 11
    assert api.balance(b) == 100_000 + 150 * 7 - 150 * 11


def test_concurrent_deposits_into_one_account(api, server):
    """The destination is not locked, so concurrent credits must still all
    land. (Appending entries never conflicts; only the balance check needs
    serializing.)"""
    acct = api.account()
    reqs = [
        {"url": "/deposits", "json": {"account_id": acct, "amount": 3},
         "headers": {"Idempotency-Key": f"dep-{i}"}}
        for i in range(200)
    ]
    responses = fire_concurrently(server.url, reqs)
    assert {r.status_code for r in responses} == {201}
    assert api.balance(acct) == 600
