# Failure modes

What can go wrong when moving money, how Ledger handles each case, and the
test that proves it. Every test runs against a real `uvicorn` process and a
real Postgres, with no mocks.

## The one idea everything rests on

**A transfer is exactly one Postgres transaction.** Claiming the idempotency
key, reading the balance, writing the debit, writing the credit, and
recording the outcome either all commit together or none of them do. Almost
every failure below reduces to "the transaction didn't commit, so nothing
happened" or "the transaction committed, so the stored record is the truth".

The service never has to *repair* anything after a crash. There is no
"in-progress" state that a recovery job must find and finish, because such a
state can't survive a crash: it only exists inside an uncommitted
transaction.

> **Caveat worth saying in an interview:** this works because everything the
> transfer touches lives in one database. If a transfer also called an
> external system (a card network, another bank), one local transaction
> could no longer cover it. You would need a persisted state machine
> (`pending → sent → confirmed`), an outbox table, and reconciliation. That
> is the saga / outbox pattern, and it's deliberately out of scope here.

---

## 1. Duplicate requests (client retries)

**The problem.** A client sends a transfer and the connection times out. It
cannot tell whether the request was lost or whether it succeeded and only the
*response* was lost. In distributed systems these two cases look the same
to the sender, so the only safe move is to retry, and the server must make
retries harmless.

**Mechanism.** Every transfer carries a client-generated `Idempotency-Key`,
and `transfers.idempotency_key` has a `UNIQUE` index. The first statement of
the transaction is:

```sql
INSERT INTO transfers (..., idempotency_key, request_hash, status)
VALUES (..., 'pending')
ON CONFLICT (idempotency_key) DO NOTHING
RETURNING ...
```

- **Key not seen before:** the row is inserted and this request does the
  work.
- **Key already committed:** the insert does nothing, and we return the stored
  transfer with header `Idempotent-Replayed: true` and the original status
  code. A rejected transfer replays as a rejection, too.
- **Key currently being processed by another transaction:** Postgres makes
  our `INSERT` *wait* on the unique index until that transaction finishes.
  If it commits we see a conflict and replay; if it rolls back, our insert
  goes through and we do the work. The database arbitrates the race, not
  application code.
- **Key reused for a different request:** the stored `request_hash`
  (sha256 of the payload) doesn't match, so we return **409**. Replaying
  would hide a client bug, and processing would break the key's meaning.

**Why not "SELECT to check, then INSERT"?** Two concurrent requests can both
SELECT, both see nothing, and both proceed. We confirmed this by mutation
testing: swapping in check-then-insert makes 6 tests fail.

| Test | What it proves |
|---|---|
| `test_idempotency.py::test_sequential_duplicate_returns_original_result` | Replay returns an identical body; balance moves once |
| `test_idempotency.py::test_concurrent_duplicates_process_exactly_once[2/25/100]` | N identical requests fired simultaneously produce exactly 1 transfer row, 2 entries, and 1 non-replayed response |
| `test_idempotency.py::test_concurrent_duplicates_of_a_rejected_transfer` | Outcomes, including rejections, are replayed consistently |
| `test_idempotency.py::test_rejected_result_is_sticky_even_after_funds_arrive` | A key names one *attempt*; a stale retry can't move money later |
| `test_idempotency.py::test_key_reused_with_different_payload_is_refused` | 409 on payload mismatch |
| `test_idempotency.py::test_same_key_different_payload_concurrently` | Two different payloads racing on one key: one wins, the other gets 409 |

## 2. Concurrent transfers from the same account (race conditions)

**The problem.** Check-then-act: two withdrawals both read `balance = 100`,
both decide `100 ≥ 100`, both debit, and the account ends at −100. This is a
*lost update*.

**Mechanism.** Before reading the balance, the transaction takes a row lock on
the source account:

```sql
SELECT 1 FROM accounts WHERE id = $source FOR NO KEY UPDATE;
SELECT SUM(amount) FROM entries WHERE account_id = $source;  -- after the lock
```

A second transfer from the same account blocks at the lock until the first
commits. Three details matter, and each is backed by a test:

1. **Isolation level: READ COMMITTED, on purpose.** In READ COMMITTED every
   statement sees everything committed before *that statement* started, so
   the `SUM` that runs after we waited for the lock sees the previous holder's
   entries. Under REPEATABLE READ the snapshot is frozen at the transaction's
   *first* statement, before the wait, so the `SUM` misses them and the
   account **still overdraws even with the lock**.
   `test_isolation_levels.py` demonstrates all four combinations directly
   against Postgres:

   | Strategy | Result (20 × 100 withdrawn from 1,000) |
   |---|---|
   | READ COMMITTED, no lock | overdraws |
   | REPEATABLE READ + lock | **overdraws** (stale snapshot) |
   | READ COMMITTED + lock (**ours**) | exactly 10 succeed, balance 0 |
   | SERIALIZABLE, no lock | correct, but aborts transactions the client must retry |

2. **`FOR NO KEY UPDATE`, not `FOR UPDATE`.** Inserting a `transfers` or
   `entries` row takes a `FOR KEY SHARE` lock on the referenced account
   (that's how Postgres enforces the foreign key). `FOR UPDATE` conflicts with
   `KEY SHARE`, so two transfers from one account deadlock: each holds a key-share
   lock the other's `FOR UPDATE` needs. `FOR NO KEY UPDATE` doesn't conflict.
   Mutation test: switching to `FOR UPDATE` fails 5 concurrency tests with
   `deadlock detected`.

3. **Only the source account is locked.** The only rule at risk is "a
   customer balance never goes negative", and only a debit can break it.
   Credits are plain appends. Because each transaction holds at most one
   account lock, a deadlock cycle between transfers is impossible (a cycle
   needs someone holding one lock while waiting for another).

| Test | What it proves |
|---|---|
| `test_concurrency.py::test_hot_account_never_overdraws[normal / widened_race_window]` | 200 simultaneous withdrawals of 100 from 10,000: exactly 100 succeed, 100 are rejected, final balance 0. The `widened_race_window` variant sleeps 20 ms *between the balance read and the debit*, so a broken lock would fail every run. |
| `test_concurrency.py::test_random_traffic_final_balances_match_acknowledged_transfers` | 500 random concurrent transfers among 8 accounts: every final balance equals what the **client-visible responses** imply, so no lost updates and no false 201s or 422s |
| `test_concurrency.py::test_opposing_transfers_do_not_deadlock` | 300 simultaneous A→B / B→A transfers, zero failures |
| `test_concurrency.py::test_concurrent_deposits_into_one_account` | 200 unlocked credits to one account all land |
| `test_isolation_levels.py` (4 tests) | The control experiment above. The two *failing* strategies prove the harness really produces the race. |

## 3. Crash between the debit and the credit

**The problem.** The process dies after writing the debit but before writing
the credit, and money vanishes.

**How we test it.** `app/failpoints.py` compiles named crash points into the
transfer path. With `LEDGER_FAILPOINTS="after_debit=crash"`, the server
sends **SIGKILL to itself** right after the debit `INSERT`. SIGKILL can't be
caught: no `finally` blocks, no `ROLLBACK` sent by Python, no graceful
shutdown. It's what the OOM killer or a power cut does. A failpoint is
deterministic, whereas a `kill -9` fired from outside might or might not land
at the right line.

**Mechanism.** The dead process's socket closes, Postgres notices, and it
rolls back the open transaction. Nothing it wrote survives: not the debit,
and not even the idempotency-key claim.

**Second, independent layer.** Suppose a future bug committed the debit on its
own. The database would still refuse it. A deferred constraint trigger
checks at `COMMIT` that each transfer's entries sum to zero, and the
foreign key from `entries` to the not-yet-committed `transfers` row also
fails. Mutation testing confirmed this: committing the debit on a separate
connection was rejected by Postgres (`ForeignKeyViolation`). Only after
also disabling every database-level check did an orphaned debit appear,
and then the crash test and the invariant check caught it
(`ledger does not sum to zero: -400`).

| Test | What it proves |
|---|---|
| `test_crash_recovery.py::test_crash_between_debit_and_credit_rolls_back_completely` | After SIGKILL: exit code −9, no entries, no transfer row, balances untouched, no orphaned open transaction. Retrying the same key on a restarted server then processes **once**, and a further retry replays. |
| `test_crash_recovery.py::test_crash_while_holding_account_lock_does_not_block_others` | The dead transaction's row lock is released, so other clients of that account aren't wedged |
| `test_db_invariants.py::test_single_legged_transfer_cannot_commit` | Writing SQL directly, a one-legged transfer is rejected at COMMIT (`check_violation`) |
| `test_db_invariants.py::test_mismatched_legs_cannot_commit` | −100 / +99 is rejected |

## 4. Crash after commit, before the response

**The problem.** The transfer committed, but the process died before sending
the HTTP response. The client sees a dropped connection and retries. Without
idempotency this is a **double payment**, and it's the case naive retry logic
gets wrong.

**Mechanism.** `after_commit=crash` kills the process right after `COMMIT`.
The transfer row and both entries are durable. The retry's
`INSERT … ON CONFLICT` hits the committed key and replays the original
result.

| Test | What it proves |
|---|---|
| `test_crash_recovery.py::test_crash_after_commit_before_response_is_not_reprocessed` | The transfer is committed despite the crash; the retry returns 201 with `Idempotent-Replayed: true`; the balance is debited once |
| `test_crash_recovery.py::test_crash_then_concurrent_retries[after_debit / after_commit]` | After either crash, 30 simultaneous retries yield exactly one transfer |

## 5. Bugs and manual edits that bypass the application

**The problem.** Application code has bugs, and people run ad-hoc SQL.

**Mechanism.** The invariants live in the database, not just in Python:

- **There is no balance column.** A balance is `SUM(entries.amount)`, so it
  can't drift from the history. `test_no_balance_column_exists` checks the
  schema mechanically.
- **Entries are append-only.** A trigger rejects `UPDATE` and `DELETE`
  (`test_entries_are_append_only`). A correction is a new, reversing
  transfer.
- **Every transfer balances.** The deferred trigger described in section 3.

**Belt and braces in the test suite.** An autouse fixture
(`tests/conftest.py::assert_ledger_consistent`) runs after **every** test
and checks that the whole ledger sums to zero, that each completed transfer
has exactly one `-amount` debit on its source and one `+amount` credit on
its destination, that rejected transfers have no entries, that no `pending`
row was ever committed, and that no customer balance is negative. A test
about something else still fails if it corrupted the ledger.

---

## How we know the tests can fail

A concurrency test that always passes might simply never have produced a
race. Two safeguards:

- **Control experiments** (`test_isolation_levels.py`) assert that the broken
  strategies *do* overdraw under this harness.
- **Mutation testing.** Each deliberate bug below was injected into
  `app/service.py`, and the suite was run against it:

| Injected bug | Caught by |
|---|---|
| Remove the source-account row lock | hot-account tests (both variants) plus the invariant check (overdrawn accounts) |
| `FOR UPDATE` instead of `FOR NO KEY UPDATE` | 5 concurrency tests (deadlocks → 500s) |
| Check-then-insert instead of `INSERT … ON CONFLICT` | 5 idempotency tests + 1 crash-retry test |
| Commit the debit in its own transaction | Postgres itself (FK violation); with DB checks disabled too, the crash test and invariant check |

The suite was also run 5 times back to back with no flakes.

## Known limitations

- **Idempotency keys are global and kept forever.** A production system
  scopes keys per client (or merchant) and expires them after a retention
  window (Stripe uses 24 hours). Both are straightforward: a composite unique
  key and a TTL sweeper.
- **Validation failures (unknown account, currency mismatch) are not stored
  against the key.** They happen before the key is claimed, so a retry
  re-validates. That's safe because nothing moved, but it means a retry after
  the account is created would succeed.
- **Loss of the database itself** (disk failure, failover) is outside this
  project. Durability rests on Postgres's WAL with `synchronous_commit=on`,
  plus, in production, synchronous replication.
