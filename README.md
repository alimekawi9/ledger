# Ledger: a payment settlement engine built for correctness under failure

A double-entry ledger service (Python, FastAPI, Postgres) that moves money
between accounts and stays correct under the conditions that break naive
payment code: **duplicate requests, concurrent transfers on the same account,
and the process being killed mid-transfer.**

The feature set is deliberately small. The point is the test suite: each
failure scenario is reproduced against a real server process and a real
Postgres database, and the tests are themselves checked by mutation testing
to make sure they can fail.

```
docker compose up --build        # API on :8000, OpenAPI docs on :8000/docs
```

| | |
|---|---|
| **Correctness** | 36 tests centered on 5 failure scenarios, with an invariant check after every test and mutation testing of the suite ([docs/FAILURE_MODES.md](docs/FAILURE_MODES.md)) |
| **Throughput** | ~440 transfers/s, p99 67 ms, on 4 vCPUs with one API process ([docs/PERFORMANCE.md](docs/PERFORMANCE.md)) |
| **Bottleneck** | API CPU for spread-out traffic; the row lock on a hot account, confirmed causally |

---

## 1. The double-entry model

Nothing in the schema stores a balance. Money moves only by **appending
entries**, and every transfer appends exactly two, which sum to zero:

```
transfer 300 cents, Alice → Bob

entries
 id | transfer_id | account | amount
----+-------------+---------+--------
 17 | t_8c1…      | alice   |   -300   ← debit  (money leaves)
 18 | t_8c1…      | bob     |   +300   ← credit (money arrives)
```

An account's balance is `SUM(amount)` over its entries. Why build it this way:

- **Nothing can drift.** A stored balance field can disagree with the history
  that supposedly produced it. A derived balance can't.
- **Audit trail for free.** Every cent's origin is a row you can point to.
  Corrections are new reversing entries; existing entries are never edited.
- **Money is conserved by construction.** The sum of *all* entries in the
  ledger is always exactly zero.
- **Money entering the system is double-entry too.** Each currency has an
  `external` account standing for money outside the ledger (in a bank, the
  settlement account at the central bank). A deposit is a transfer *from*
  it, so that account's negative balance equals the total money that entered
  the ledger.

These rules are enforced by **Postgres itself**, not just the Python code
([`app/schema.sql`](app/schema.sql)):

- a **deferred constraint trigger** checks at `COMMIT` that each transfer's
  entries sum to zero, so a one-legged transfer can't commit even if the
  application has a bug;
- a trigger rejects `UPDATE` and `DELETE` on `entries` (append-only);
- amounts are `BIGINT` cents, never floats (`0.1 + 0.2 ≠ 0.3`).

## 2. How a transfer executes

Everything below is **one database transaction**
([`app/service.py`](app/service.py)):

```
BEGIN                                            -- READ COMMITTED
  INSERT INTO transfers (idempotency_key, …)     ① claim the key
    ON CONFLICT (idempotency_key) DO NOTHING       (duplicate → replay stored result)
  SELECT … FROM accounts WHERE id = $src
    FOR NO KEY UPDATE                            ② lock the source account
  SELECT SUM(amount) FROM entries                ③ read balance *after* locking
    WHERE account_id = $src                        (insufficient → record 'rejected')
  INSERT entry (-amount, src)                    ④ debit
  INSERT entry (+amount, dst)                    ⑤ credit
  UPDATE transfers SET status = 'completed'
COMMIT                                           -- trigger verifies Σ = 0
```

Each step's position is deliberate, and each is backed by a test (links in the
failure-modes doc):

- **Why one transaction?** Atomicity: either all of it happens or none of
  it does. If the process dies anywhere before `COMMIT`, Postgres rolls
  everything back, including the idempotency-key claim. So a crash never
  leaves a half transfer and the service never needs a recovery job.
- **Why lock before reading the balance?** Otherwise two withdrawals can both
  read the same balance, both pass the check, and both debit: a *lost
  update*. The lock makes the second one wait.
- **Why READ COMMITTED and not a "stronger" level?** In READ COMMITTED each
  statement sees everything committed before it started, so the `SUM` in ③
  sees the entries of whoever held the lock before us. Under REPEATABLE READ,
  the snapshot is taken at the *first* statement, before we waited for
  the lock, so the balance is stale and the account **overdraws despite the
  lock**. `tests/test_isolation_levels.py` demonstrates this directly.
- **Why `FOR NO KEY UPDATE`?** Inserting an entry makes Postgres take a
  "key share" lock on the referenced account to enforce the foreign key. A
  plain `FOR UPDATE` conflicts with that and **deadlocks** two transfers from
  the same account. Mutation testing confirmed it.
- **Why lock only the source?** Only a debit can push a balance negative, so
  only the debit needs serializing. And with at most one lock per
  transaction, transfers can't deadlock with each other.

## 3. Idempotency design

A client whose request times out can't know whether the transfer happened.
The only safe move is to **retry**, so retries must be harmless. Each
`POST /transfers` requires an `Idempotency-Key` header, a unique value the
client generates once per logical payment and reuses on every retry.

| Situation | Response |
|---|---|
| New key | Processed; `201` (or `422` if insufficient funds) |
| Key already completed | **Stored result replayed**: same body, same status, header `Idempotent-Replayed: true` |
| Same key in flight concurrently | The second request **waits** on the unique index until the first commits, then replays. Postgres arbitrates the race. |
| Crash before commit, then retry | The key claim was rolled back with everything else, so the retry processes normally |
| Crash after commit, then retry | The key is committed, so the retry replays. **No double payment.** |
| Same key, different payload | `409 idempotency_key_reused` (detected via a sha256 fingerprint of the request) |

The key is claimed with `INSERT … ON CONFLICT DO NOTHING`, not
"SELECT to check, then INSERT". Two concurrent requests can both pass the
SELECT, and a mutation test proves the suite catches that version.

## 4. The five tested scenarios

Every test runs against a real `uvicorn` subprocess and real Postgres, fires
genuinely concurrent requests (an asyncio start barrier releases them all at
once), and ends with an **invariant check of the whole ledger**: sums to
zero, every completed transfer has exactly one matching debit and credit, no
`pending` rows, no negative customer balances.

| # | Scenario | Key assertion |
|---|---|---|
| 1 | **Concurrent duplicate requests** (2 / 25 / 100 copies) | exactly 1 transfer, 2 entries, 1 non-replayed response |
| 2 | **Concurrent transfers on one account** (200 withdrawals, with and without a deliberately widened 20 ms race window; 500 random transfers; 300 A↔B) | never overdrawn; final balances match exactly what the responses told clients; no deadlocks |
| 3 | **SIGKILL between debit and credit** | nothing persisted; the retry processes once |
| 4 | **SIGKILL after commit, before the response** | the retry replays; the account is debited once |
| 5 | **Writes that bypass the app** | the database rejects unbalanced transfers and edits to entries |

Crashes are injected with **failpoints** ([`app/failpoints.py`](app/failpoints.py)):
`LEDGER_FAILPOINTS="after_debit=crash"` makes the server SIGKILL itself at
that exact line. That's deterministic, unlike a `kill -9` from outside.

**Are the tests actually capable of failing?** Two checks:

- *Control experiments* assert that broken strategies (no lock; lock under
  REPEATABLE READ) **do** overdraw in this harness.
- *Mutation testing*: I injected four real bugs into the service (removed
  the lock, `FOR UPDATE`, check-then-insert idempotency, a debit committed on
  its own). The suite caught the first three; Postgres itself rejected the
  fourth, and with the database checks also disabled the suite caught it too.
  Details in
  [docs/FAILURE_MODES.md](docs/FAILURE_MODES.md).

## 5. Load test results

![Throughput and latency vs. concurrency](docs/load-sweep.png)

| Scenario (best point) | Throughput | p50 | p95 | p99 |
|---|---:|---:|---:|---:|
| Uniform, 16 clients | **442 tx/s** | 35 ms | 52 ms | 67 ms |
| Hot account, 4 clients | 309 tx/s | 12 ms | 22 ms | 31 ms |

**Bottlenecks, identified by measurement rather than guessed**
([full analysis](docs/PERFORMANCE.md)):

- **Uniform traffic → the API process's CPU.** The single worker ran at 95%
  of a core while Postgres connections sat mostly idle, waiting for the app.
  I tested the tempting alternatives and ruled them out: a 3× larger
  connection pool made no difference, and so did turning off the WAL fsync
  wait at commit.
- **Hot account → the row lock.** About 16 of 20 connections were waiting on
  the lock. I confirmed it causally: adding *s* ms inside the locked
  section lowered throughput along the predicted 1/(3.5 ms + s) curve,
  within ~8%.
- **The design's scaling limit:** computing the balance as `SUM(history)`
  inside the lock. A hot account with 500k prior transfers drops to 15 tx/s.
  Fix options and their trade-offs are in the performance doc.

![Diagnosis experiments](docs/load-diagnosis.png)

## 6. Observability

- **Structured JSON logs**, one line per transfer, with request id
  (`X-Request-ID`, accepted from the caller or generated, and echoed back),
  idempotency key, transfer id, outcome and latency:
  ```json
  {"ts": "…", "level": "INFO", "msg": "transfer.completed", "request_id": "req-abc-123",
   "idempotency_key": "pay-invoice-42", "transfer_id": "8c1f…", "amount": 300,
   "outcome": "completed", "latency_ms": 6.1}
  ```
- **`GET /metrics`** in Prometheus format:
  - `ledger_transfers_total{kind, outcome}`, where outcome is `completed`,
    `rejected`, `replayed`, `key_reused`, `invalid` or `error`
  - `ledger_transfer_duration_seconds` (histogram, so p50/p95/p99 can be computed)
  - `ledger_http_requests_total{method, route, status}`, labeled by route
    *template*, so account ids don't blow up the number of series
  - `ledger_db_requests_waiting`, `ledger_db_pool_*`: pool saturation
  - Error rate:
    `sum(rate(ledger_http_requests_total{status=~"5.."}[5m])) / sum(rate(ledger_http_requests_total[5m]))`

## API

| Method | Path | |
|---|---|---|
| `POST` | `/accounts` | `{"name", "currency": "USD"|"EUR"|"GBP"}` |
| `GET` | `/accounts/{id}` | |
| `GET` | `/accounts/{id}/balance` | derived from entries |
| `GET` | `/accounts/{id}/transactions?limit=&before_entry_id=` | history, keyset-paginated |
| `POST` | `/deposits` | `{"account_id", "amount"}` + `Idempotency-Key` |
| `POST` | `/transfers` | `{"from_account_id", "to_account_id", "amount"}` + `Idempotency-Key` |
| `GET` | `/metrics`, `/health` | |

Amounts are integer minor units (cents).

## Running it

```bash
# Everything in containers
docker compose up --build

# Tests (need a Postgres; Compose's also creates the ledger_test database)
pip install -r requirements-dev.txt
LEDGER_TEST_DATABASE_URL=postgresql://ledger:ledger@localhost:5432/ledger_test pytest

# Load tests (drop the `ledger_load` database it creates when done)
LOAD_ADMIN_DATABASE_URL=postgresql://ledger:ledger@localhost:5432/postgres \
  python -m loadtest.experiments sweep
python -m loadtest.plot
```

## Layout

```
app/
  schema.sql        tables, invariant triggers
  service.py        transfer logic (read this first)
  main.py           FastAPI routes, logging and metrics per transfer
  observability.py  JSON logs, request ids, Prometheus metrics
  failpoints.py     crash injection
tests/              the point of the project
loadtest/           load generator, experiments, charts, raw results
docs/               failure modes, performance analysis
```

## Scope, deliberately

Out of scope, and how I'd approach each: **cross-service transfers**
(outbox + saga, because one local transaction no longer covers the whole
operation); **per-client key scoping and key expiry**; **authentication**;
**multi-currency FX**. The first two are discussed in
[docs/FAILURE_MODES.md](docs/FAILURE_MODES.md).
