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

## 2. Concurrent transfers touching the same account (race conditions)

**The problem.** Check-then-act: two withdrawals both read `balance = 100`,
both decide `100 ≥ 100`, both debit, and the account ends at −100. This is a
*lost update*.

**Mechanism.** Before reading the balance, the transaction takes row locks on
both customer accounts, lowest id first, then reads the source's current
balance (the newest entry's `balance_after`):

```sql
SELECT 1 FROM accounts WHERE id = $lower_id  FOR NO KEY UPDATE;
SELECT 1 FROM accounts WHERE id = $higher_id FOR NO KEY UPDATE;
SELECT balance_after FROM entries ... newest entry of $source ...;  -- after the locks
```

A second transfer touching either account blocks at the lock until the first
commits. Four details matter, and each is backed by a test:

1. **Isolation level: READ COMMITTED, on purpose.** In READ COMMITTED every
   statement sees everything committed before *that statement* started, so
   the balance read that runs after we waited for the lock sees the previous
   holder's entries. Under REPEATABLE READ the snapshot is frozen at the
   transaction's *first* statement, before the wait, so the read is stale and
   the account **still overdraws even with the lock**. The `balance_after`
   trigger therefore refuses to run under any level except READ COMMITTED
   (`test_ledger_writes_refused_outside_read_committed`).
   `test_isolation_levels.py` demonstrates all four combinations directly
   against Postgres, with the ledger's triggers switched off so only the
   strategy under test is measured:

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

3. **Both accounts are locked, in sorted order.** Each entry stores the
   account's running balance, and a running balance is only right if that
   account's writers take turns, so the destination needs a lock too.
   (Version 1 locked only the source and computed balances with `SUM`. That
   ruled out deadlocks but slowed down as history grew; see
   [PERFORMANCE.md](PERFORMANCE.md).) Two locks per transfer bring back the
   classic deadlock: A→B holds A and wants B while B→A holds B and wants A.
   Always locking the lower account id first makes that cycle impossible.
   Mutation tests: locking in request order, or locking only the source
   (the trigger then locks the destination late, which is unsorted again),
   each fail 3 concurrency tests with deadlocks.

4. **External accounts are never locked.** Funding accounts may go negative,
   so they have nothing to protect, and every deposit touches one. Locking it
   would queue all deposits of a currency on one row.

| Test | What it proves |
|---|---|
| `test_concurrency.py::test_hot_account_never_overdraws[normal / widened_race_window]` | 200 simultaneous withdrawals of 100 from 10,000: exactly 100 succeed, 100 are rejected, final balance 0. The `widened_race_window` variant sleeps 20 ms *between the balance read and the debit*, so a broken lock would fail every run. |
| `test_concurrency.py::test_random_traffic_final_balances_match_acknowledged_transfers` | 500 random concurrent transfers among 8 accounts: every final balance equals what the **client-visible responses** imply, so no lost updates and no false 201s or 422s |
| `test_concurrency.py::test_opposing_transfers_do_not_deadlock` | 300 simultaneous A→B / B→A transfers, zero failures |
| `test_concurrency.py::test_concurrent_deposits_into_one_account` | 200 concurrent credits to one account all land, each with the right running balance |
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

- **No balance is ever edited.** The only balance-like column is
  `entries.balance_after`, the running balance *after* that entry. Postgres
  computes it in a trigger (any value the writer supplies is overwritten) and
  never updates it afterwards, because entries are append-only.
  `test_no_mutable_balance_exists` and `test_client_cannot_forge_balance_after`
  check this.
- **The database refuses overdrafts itself.** The same trigger raises if a
  customer's running balance would go below zero, so the app's funds check
  isn't the only guard (`test_database_refuses_overdraft_even_bypassing_the_app`).
- **Entries are append-only.** A trigger rejects `UPDATE` and `DELETE`
  (`test_entries_are_append_only`). A correction is a new, reversing
  transfer.
- **Every transfer balances.** The deferred trigger described in section 3.

**Belt and braces in the test suite.** An autouse fixture
(`tests/conftest.py::assert_ledger_consistent`) runs after **every** test
and checks that the whole ledger sums to zero, that each completed transfer
has exactly one `-amount` debit on its source and one `+amount` credit on
its destination, that rejected transfers have no entries, that no `pending`
row was ever committed, that no customer balance is negative, and that
**every stored `balance_after` equals the running sum of that account's
history**. A test about something else still fails if it corrupted the
ledger.

### Two bugs the tests found in the running-balance trigger

Both were found by mutation testing: removing the service's own locks so the
trigger had to stand alone.

1. **Queued writers lost money.** Postgres fills in an entry's `id` *before*
   BEFORE-INSERT triggers run, which means before the trigger waits for the
   account lock. A writer that queued behind another kept a *lower* id than
   the entry it built on, so "newest entry = highest id" skipped it and the
   balance silently dropped a credit. The service itself wasn't affected
   (it locks before inserting anything), but hand-written SQL was. Fix: the
   trigger draws a fresh id after it holds the lock.
   `test_running_balance_survives_writers_queued_on_the_lock` reproduces the
   interleaving deterministically: without the fix the balance is 7 instead
   of 12.
2. **External accounts were locked by accident.** The trigger locked the
   account *before* checking its kind, so it locked funding accounts too. That
   serialized all deposits and deadlocked two raw-SQL writers in the test
   above. Fix: read the kind unlocked first, and lock only customer accounts.

A third issue showed up in the load test rather than the suite: a query-plan
choice that turned the O(1) balance lookup into a scan of 500k rows. It's
described in [PERFORMANCE.md](PERFORMANCE.md).

---

## How we know the tests can fail

A concurrency test that always passes might simply never have produced a
race. Two safeguards:

- **Control experiments** (`test_isolation_levels.py`) assert that the broken
  strategies *do* overdraw under this harness.
- **Mutation testing.** Each deliberate bug below was injected into
  `app/service.py`, and the suite was run against it. "v1" is the first
  design (source-only lock, `SUM` balance); "v2" is the current one.

| Injected bug | Version | Caught by |
|---|---|---|
| Remove the source-account row lock | v1 | hot-account tests (both variants) plus the invariant check (overdrawn accounts) |
| `FOR UPDATE` instead of `FOR NO KEY UPDATE` | v1 | 5 concurrency tests (deadlocks → 500s) |
| Check-then-insert instead of `INSERT … ON CONFLICT` | v1 | 5 idempotency tests + 1 crash-retry test |
| Commit the debit in its own transaction | v1 | Postgres itself (FK violation); with DB checks disabled too, the crash test and invariant check |
| Lock both accounts in request order instead of sorted | v2 | 3 concurrency tests (deadlocks) |
| Lock only the source account | v2 | 3 concurrency tests (deadlocks) |
| Remove all service-side locks | v2 | 5 concurrency tests (500s from the database's overdraft guard). Before the trigger fix, the invariant check also reported a forked `balance_after` chain; after it, the ledger stays consistent. |

The suite was run 5 times back to back with no flakes, for each version.

## Known limitations

- **Idempotency keys are global and kept forever.** A production system
  scopes keys per client (or merchant) and expires them after a retention
  window (Stripe uses 24 hours). Both are straightforward: a composite unique
  key and a TTL sweeper.
- **Validation failures (unknown account, currency mismatch) are not stored
  against the key.** They happen before the key is claimed, so a retry
  re-validates. That's safe because nothing moved, but it means a retry after
  the account is created would succeed.
- **Hand-written SQL can deadlock with the service.** The trigger locks
  accounts in the order entries are inserted, not in sorted order. A raw-SQL
  transfer can therefore deadlock with a service transfer. Postgres detects
  that and aborts one of them: an error to retry, never a corrupted balance.
- **Credits to one account are now serialized.** This is the price of the
  running balance, measured in [PERFORMANCE.md](PERFORMANCE.md).
- **Loss of the database itself** (disk failure, failover) is outside this
  project. Durability rests on Postgres's WAL with `synchronous_commit=on`,
  plus, in production, synchronous replication.
