"""Basic API behaviour: the double-entry model as seen from outside."""


def test_transfer_creates_balanced_debit_and_credit(api, db):
    alice = api.funded_account(10_000, "alice")
    bob = api.account("bob")

    r = api.transfer(alice, bob, 2_500)
    assert r.status_code == 201, r.text
    transfer = r.json()
    assert transfer["status"] == "completed"

    assert api.balance(alice) == 7_500
    assert api.balance(bob) == 2_500

    # The transfer is represented as exactly two entries summing to zero,
    # not as a mutation of any stored balance.
    rows = db.execute(
        "SELECT account_id::text, amount FROM entries WHERE transfer_id = %s ORDER BY amount",
        (transfer["id"],),
    ).fetchall()
    assert rows == [(alice, -2_500), (bob, 2_500)]


def test_transaction_history_shows_both_sides(api):
    alice = api.funded_account(1_000, "alice")
    bob = api.account("bob")
    t = api.transfer(alice, bob, 400).json()

    alice_hist = api.http.get(f"/accounts/{alice}/transactions").json()
    bob_hist = api.http.get(f"/accounts/{bob}/transactions").json()

    assert [(h["direction"], h["amount"], h["kind"]) for h in alice_hist] == [
        ("debit", -400, "transfer"),
        ("credit", 1_000, "deposit"),
    ]
    assert bob_hist[0]["transfer_id"] == t["id"]
    assert bob_hist[0]["counterparty_account_id"] == alice
    assert bob_hist[0]["direction"] == "credit"


def test_history_pagination(api):
    acct = api.account()
    for _ in range(5):
        api.deposit(acct, 1)
    page1 = api.http.get(f"/accounts/{acct}/transactions", params={"limit": 3}).json()
    cursor = page1[-1]["entry_id"]
    page2 = api.http.get(
        f"/accounts/{acct}/transactions", params={"limit": 3, "before_entry_id": cursor}
    ).json()
    assert len(page1) == 3 and len(page2) == 2
    assert {e["entry_id"] for e in page1}.isdisjoint(e["entry_id"] for e in page2)


def test_insufficient_funds_is_rejected_and_moves_nothing(api):
    alice = api.funded_account(100)
    bob = api.account()
    r = api.transfer(alice, bob, 101)
    assert r.status_code == 422
    assert r.json()["status"] == "rejected"
    assert r.json()["failure_reason"] == "insufficient_funds"
    assert api.balance(alice) == 100
    assert api.balance(bob) == 0


def test_validation_errors(api):
    alice = api.funded_account(100)
    eur = api.account(currency="EUR")
    missing = "00000000-0000-0000-0000-000000000000"

    assert api.transfer(alice, missing, 1).status_code == 404
    assert api.transfer(alice, alice, 1).status_code == 422
    assert api.transfer(alice, eur, 1).status_code == 422  # currency mismatch
    for bad_amount in (0, -5, 1.5):
        r = api.http.post(
            "/transfers",
            json={"from_account_id": alice, "to_account_id": eur, "amount": bad_amount},
            headers={"Idempotency-Key": "k"},
        )
        assert r.status_code == 422
    # Idempotency-Key is mandatory.
    r = api.http.post("/transfers", json={"from_account_id": alice, "to_account_id": eur, "amount": 1})
    assert r.status_code == 422
    assert api.balance(alice) == 100


def test_request_id_is_propagated_and_logged(api, server):
    alice = api.funded_account(100)
    bob = api.account()
    r = api.http.post(
        "/transfers",
        json={"from_account_id": alice, "to_account_id": bob, "amount": 10},
        headers={"Idempotency-Key": "log-check-key", "X-Request-ID": "req-abc-123"},
    )
    assert r.headers["X-Request-ID"] == "req-abc-123"

    import json
    lines = [json.loads(l) for l in server.logs().splitlines() if l.startswith("{")]
    [line] = [l for l in lines if l.get("request_id") == "req-abc-123"]
    assert line["msg"] == "transfer.completed"
    assert line["idempotency_key"] == "log-check-key"
    assert line["transfer_id"] == r.json()["id"]
    assert line["outcome"] == "completed"
    assert "latency_ms" in line


def test_metrics_endpoint_is_prometheus_format(api):
    alice = api.funded_account(100)
    api.transfer(alice, api.account(), 1)
    text = api.http.get("/metrics").text
    assert 'ledger_transfers_total{kind="transfer",outcome="completed"}' in text
    assert "ledger_transfer_duration_seconds_bucket" in text
    assert 'ledger_http_requests_total{method="POST",route="/transfers",status="201"}' in text
    assert "ledger_db_requests_waiting" in text
