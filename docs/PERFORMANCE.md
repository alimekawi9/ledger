# Load testing and bottleneck analysis

**Short version.** On a 4-core machine a single API process sustains about
**450 transfers/s** (p50 34 ms, p99 74 ms at 16 concurrent clients). With
traffic spread across many accounts, the bottleneck is the **one CPU core
running the Python API process**, not the database. The obvious "fixes" (a
bigger connection pool, faster commits) don't reliably help. When every
transfer touches the **same account**, the bottleneck becomes that account's
row lock, and throughput is set by how long the lock is held. I confirmed
that by cause and effect, not just correlation.

The first version of the design had a real scaling flaw: it computed a
balance as `SUM(history)` while holding the lock. A hot account with 500k
prior transfers dropped to 15 tx/s. The current design stores a running
balance on each entry, and throughput now **stays flat as history grows**
(279 tx/s at 500k). Getting there surfaced a query-planner trap along the
way.

![Throughput and latency vs. concurrency](load-sweep.png)

## Method

- **Load generator** (`loadtest/loadgen.py`): a closed loop. *C* virtual
  clients each send a transfer, wait for the reply, and repeat, for 10 s
  after a 2 s warm-up, split across 2 OS processes. Latency is measured on
  the client, so it includes every queue in the stack.
- **Scenarios.**
  - *Uniform*: random source and destination among 1,000 accounts (little
    contention).
  - *Hot source*: every transfer debits one account (think of a payroll run).
  - *Hot destination*: every transfer credits one account (a merchant being
    paid).
- **Evidence gathered during each run** (`loadtest/experiments.py`), every
  100 ms:
  - `pg_stat_activity`: what each Postgres connection is doing, whether
    running on CPU, waiting on a row lock, waiting on WAL flush, idle, or
    *idle in transaction* (Postgres waiting for the app to send the next
    statement);
  - CPU used by the API processes, by Postgres, and by the whole machine
    (from `/proc`);
  - the pool's queue depth (`ledger_db_requests_waiting` from `/metrics`).
- Each run starts from a freshly created database. All runs: 0 errors.
- **Environment:** 4 vCPUs, Postgres 16, 1 uvicorn worker, pool of 20, with
  the load generator on the **same machine**. That matters, as the
  scale-out result below shows. Numbers are directional: repeating a run
  moves throughput by roughly ±10%.

Reproduce with `python -m loadtest.experiments {sweep|bottleneck|lock_hold|history}`
then `python -m loadtest.plot`. Raw results are in `loadtest/results/*.json`;
the first design's results are kept in `loadtest/results/v1_sum_balance/`.

## Results: concurrency sweep

| Clients | Uniform tx/s | p50 | p95 | p99 | Hot source tx/s | p50 | p95 | p99 | Hot dest tx/s | p50 | p95 | p99 |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 163 | 5.8 | 8.6 | 10.0 | 162 | 5.8 | 8.4 | 10.9 | 153 | 5.9 | 8.7 | 11.8 |
| 4 | 380 | 10.0 | 14.6 | 19.5 | 296 | 12.3 | 23.0 | 31.9 | 294 | 12.6 | 22.4 | 31.2 |
| 16 | **452** | 34.4 | 50.9 | 73.6 | 295 | 30.9 | 178.0 | 282.3 | 296 | 31.0 | 174.9 | 272.0 |
| 32 | 435 | 68.7 | 98.7 | 138.4 | 264 | 83.7 | 310.7 | 479.7 | 278 | 76.3 | 307.2 | 469.0 |
| 64 | 328 | 189.4 | 238.7 | 252.6 | 270 | 197.8 | 417.8 | 600.4 | 283 | 188.2 | 395.2 | 579.0 |
| 128 | 275 | 342.9 | 1241.9 | 2072.0 | 255 | 447.5 | 668.3 | 849.3 | 257 | 447.3 | 668.4 | 842.1 |

Latencies in ms. Three things stand out:

- Past 16–32 clients, **adding load lowers throughput**, so something is
  saturated and paying overhead for the extra work.
- On a hot account, p95 and p99 blow up from 16 clients on while p50 stays
  close to uniform. That's the signature of requests queueing behind a lock.
- **Hot source and hot destination behave the same.** Both accounts are
  locked now, so paying *into* one account is serialized just like paying
  *out of* one. That's the price of the running balance, and it's measured
  here rather than assumed.

## Finding 1 (uniform traffic): the API process's CPU

Evidence at 64 clients:

| Signal | Value | Meaning |
|---|---|---|
| API process CPU | **95%** of one core | saturated (one worker = one core) |
| Postgres CPU | 58% of one core | headroom |
| Postgres connections, on average | 12.5 idle, 6.6 *idle in txn*, ~0 on lock | the database is mostly **waiting for the app** |
| Pool: requests waiting for a connection | ~25 | looks like pool starvation, but see below |

The app is the constraint. "Idle in transaction" means Postgres has finished
a statement and is waiting for Python to send the next one. Each transfer is
about 10 round trips (validate, claim key, 2 locks, balance, debit, credit,
update, commit), and every one costs Python CPU in the driver, event loop,
and serialization. Adding the second lock didn't move the uniform peak:
452 tx/s now vs. 442 in the first design.

I then **changed one knob at a time** to test the competing hypotheses:

| Experiment (64 clients, uniform) | tx/s | p99 ms | v1 run: tx/s | Verdict |
|---|---:|---:|---:|---|
| Baseline: 1 worker, pool 20 | 364 | 428 | 370 | |
| Pool **5** | 372 | 226 | 341 | a quarter of the connections, about the same throughput |
| Pool **60** | 413 | 311 | 356 | +13% this round, −4% last round. API CPU stayed at 93–96% both times, so this is noise, not a fix |
| `synchronous_commit=off` (no WAL fsync wait at commit) | 348 | 248 | 345 | no gain in either round: disk flush is **not** the bottleneck |
| **2** API workers | 388 | 790 | 383 | p50 halves (174 → 94 ms), but throughput is flat… |
| **3** API workers | 382 | 751 | 392 | …because the *machine* is now saturated |

The ~25 "requests waiting for a connection" deserve a note: that queue was
long **while ~12 connections sat idle**. The pool hands a free connection to
a waiting coroutine, but that coroutine only runs when the saturated event
loop gets to it. So pool wait was a *symptom* of CPU saturation, not the
cause.

**The scale-out caveat.** With 2 or 3 workers the whole machine reached ~87%
busy. The load generator itself (Python + httpx) was using about 1.8 of the 4
cores, more than the server. On this box the measuring tool competes with the
thing being measured, so I can't claim a scale-out number from it. The honest
conclusion is that the API is stateless and CPU-bound, so it scales by adding
processes or containers, and verifying that needs the load generator on
separate hardware.

## Finding 2 (hot account): the row lock, confirmed by cause and effect

At 32+ clients on one account, about **16–17 of 20** Postgres connections
are `waiting on row/txn lock`, and the API is *not* CPU-saturated (~80%).
Transfers on one account are serialized by design (that's what prevents
overdrafts and keeps running balances correct), so throughput should be
≈ 1 / (lock hold time).

Correlation isn't proof, so I **tested the prediction**: add *s* ms inside
the locked section (the `after_balance_check=sleep:s` failpoint) and see
whether throughput follows 1 / (h₀ + s). From the baseline 289 tx/s, the
implied lock hold time is h₀ ≈ 3.5 ms.

| Added under lock | Predicted tx/s | Measured tx/s |
|---:|---:|---:|
| 0 ms | (289) | 289 |
| 2 ms | 183 | 182 |
| 5 ms | 118 | 114 |
| 10 ms | 74 | 67 |

The measurements track the model within ~10%, and slightly low because
`asyncio.sleep` overshoots. The first design gave the same fit
(170/111/67 vs. 182/118/74). **Hot-account throughput is determined by lock
hold time.** Anything that shortens the critical section raises the
ceiling; nothing else will.

![Lock-hold experiment and history growth](load-diagnosis.png)

## Finding 3: the balance must not cost more as history grows

**The problem in version 1.** The balance was `SUM(entries)`, computed under
the lock, so lock hold time grew with the account's history:

| Prior transfers on the hot account | v1: `SUM(history)` tx/s | v2: running balance tx/s |
|---:|---:|---:|
| 0 | 232 | 279 |
| 10,000 | 187 | 256 |
| 100,000 | 81 | 261 |
| 500,000 | **15** | **279** |

**The fix.** Each entry stores `balance_after`, the account's running balance
including that entry. Postgres computes it in a trigger, and it's never
updated afterwards, so it's still append-only history rather than a mutable
balance field. The current balance is the newest entry's `balance_after`, a
single index probe. The costs, all measured above or tested:

- **The destination must be locked too**, because a running balance is only
  correct if the account's writers take turns. Hot-destination traffic is
  now serialized (about 296 tx/s, the same ceiling as a hot source).
- **Two locks per transfer can deadlock** unless they're always taken in the
  same order, so the service locks the lower account id first.
- **Ids are assigned before the lock is taken.** Postgres fills in an entry's
  id before the trigger runs, so a writer that queued on the lock could end
  up with a lower id than the entry it built on. The trigger now draws a
  fresh id after it holds the lock. Mutation testing found this; see
  [FAILURE_MODES.md](FAILURE_MODES.md).

**A query-planner trap found on the way.** The first run of the new design
still collapsed at 500k prior transfers: 10 tx/s. `EXPLAIN ANALYZE` showed
why. For `WHERE account_id = X ORDER BY id DESC LIMIT 1`, the planner knew
account X owned half the table, so it walked the *primary key* backwards,
expecting a match almost immediately. But X's entries all sat at old ids (the
seed wrote them in one block), so the walk discarded **500,000 rows**: 42–72 ms
per lookup, while holding the lock. The generic plan was fine; only the
per-value plan was wrong.

Adding `account_id` to the `ORDER BY` doesn't help: Postgres drops sort keys
that an equality condition pins to a single value. The query now asks for
the last row at or before `(X, MAX)` in `(account_id, id)` order, which only
the composite index can answer. That takes 0.025 ms in every plan mode. If
the row found belongs to another account, X has no entries yet. The seeding
exaggerated the clustering, but real hot accounts are skewed too, and a
lookup that runs under a lock shouldn't depend on the planner guessing well.

## What I would change next, in order

1. **Shorten the critical section.** Do lock, balance check, both inserts,
   and the status update in a single statement (a CTE or stored procedure):
   one round trip under the lock instead of ~5. By Finding 2, that should
   raise the hot-account ceiling roughly in proportion.
2. **Hot accounts by design:** split a merchant's settlement account into
   *k* sub-accounts (sharding), or batch many transfers per lock
   acquisition. TigerBeetle, a database built for financial transactions,
   takes batching to its limit.
3. **Uniform throughput:** run more API processes on separate hardware from
   the load generator, then re-measure. The next bottleneck is likely
   Postgres commit throughput, and it will show up in `pg_stat_activity` as
   WAL waits.
