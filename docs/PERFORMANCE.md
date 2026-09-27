# Load testing and bottleneck analysis

**Short version.** On a 4-core machine a single API process sustains about
**440 transfers/s** (p50 35 ms, p99 67 ms at 16 concurrent clients). The
bottleneck is **not** the database. It is the one CPU core running the
Python API process, and the obvious "fixes" (a bigger connection pool,
faster commits) measurably don't help. When every transfer debits the
**same account**, the bottleneck becomes that account's row lock, and
throughput is set by how long the lock is held. I confirmed that causally,
not just by correlation. The same experiments exposed the design's real
scaling limit: computing a balance as `SUM(history)` inside the lock.

![Throughput and latency vs. concurrency](load-sweep.png)

## Method

- **Load generator** (`loadtest/loadgen.py`): a closed loop. *C* virtual
  clients each send a transfer, wait for the reply, and repeat, for 10 s
  after a 2 s warm-up, split across 2 OS processes. Latency is measured on
  the client, so it includes every queue in the stack.
- **Scenarios.** *Uniform*: random source and destination among 1,000
  accounts (little contention). *Hot*: every transfer debits one account
  (maximum contention; think of a merchant's settlement account or a
  payroll run).
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
  scale-out result below shows. Numbers are directional: repeated hot-account
  runs varied by about ±15%.

Reproduce with `python -m loadtest.experiments {sweep|bottleneck|lock_hold|history}`
then `python -m loadtest.plot`. Raw results are in `loadtest/results/*.json`.

## Results: concurrency sweep

| Clients | Uniform tx/s | p50 | p95 | p99 | Hot tx/s | p50 | p95 | p99 |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 173 | 5.5 | 7.7 | 9.2 | 148 | 6.4 | 9.1 | 12.8 |
| 4 | 309 | 12.3 | 18.8 | 25.6 | 309 | 11.7 | 22.2 | 31.3 |
| 16 | **442** | 35.2 | 52.1 | 67.3 | 297 | 29.9 | 177.8 | 289.1 |
| 32 | 395 | 80.9 | 105.4 | 125.3 | 256 | 83.7 | 324.6 | 518.1 |
| 64 | 346 | 174.3 | 233.2 | 270.2 | 276 | 192.4 | 418.5 | 585.4 |
| 128 | 244 | 305.0 | 1353.8 | 1991.1 | 245 | 466.2 | 686.6 | 849.6 |

Latencies in ms. Two things stand out. Past 16 clients, **adding load lowers
throughput**, so something is saturated and paying overhead for the extra
work. And the hot account's p95 and p99 blow up from 16 clients on, while its
p50 stays similar to uniform: the signature of requests queueing behind a
lock.

## Finding 1 (uniform traffic): the API process's CPU

Evidence at 64 clients:

| Signal | Value | Meaning |
|---|---|---|
| API process CPU | **95%** of one core | saturated (one worker = one core) |
| Postgres CPU | 55% of one core | plenty of headroom |
| Postgres connections, on average | 12.7 idle, 6.6 *idle in txn*, 0.3 on CPU, 0.3 WAL flush | the database is mostly **waiting for the app** |
| Pool: requests waiting for a connection | ~24 | looks like pool starvation, but see below |

The app is the constraint. "Idle in transaction" means Postgres has finished
a statement and is waiting for Python to send the next one. Each transfer
is about 9 round trips (validate, claim key, lock, sum, debit, credit,
update, commit), and every one costs Python CPU in the driver, event loop,
and serialization.

I then **changed one knob at a time** to test the competing hypotheses:

| Experiment (64 clients, uniform) | tx/s | p99 ms | Verdict |
|---|---:|---:|---|
| Baseline: 1 worker, pool 20 | 370 | 300 | |
| Pool **5** | 341 | 236 | a quarter of the connections, about the same throughput |
| Pool **60** | 356 | 450 | 3× the connections, no gain: the pool is **not** the bottleneck |
| `synchronous_commit=off` (no WAL fsync wait at commit) | 345 | 242 | no gain: disk flush is **not** the bottleneck |
| **2** API workers | 383 | 746 | p50 halves (171 → 94 ms), but throughput is flat… |
| **3** API workers | 392 | 753 | …because the *machine* is now saturated |

The ~24 "requests waiting for a connection" deserve a note: that queue was
long **while ~13 connections sat idle**. The pool hands a free connection to
a waiting coroutine, but that coroutine only runs when the saturated event
loop gets to it. So pool wait was a *symptom* of CPU saturation, and making
the pool bigger was the wrong fix. The experiment shows it.

**The scale-out caveat.** With 2 or 3 workers the whole machine reached ~86%
busy. The load generator itself (Python + httpx) was using about 1.8 of the 4
cores, more than the server. On this box the measuring tool competes with the
thing being measured, so I can't claim a scale-out number from it. The honest
conclusion is that the API is stateless and CPU-bound, so it scales by adding
processes or containers, and verifying that needs the load generator on
separate hardware.

## Finding 2 (hot account): the row lock, confirmed by cause and effect

At 32+ clients on one account, about **16 of 20** Postgres connections are
`waiting on row/txn lock`, and the API is *not* CPU-saturated (~80%).
Transfers from one account are serialized by design (that's what
prevents overdrafts), so throughput should be ≈ 1 / (lock hold time).

Correlation isn't proof, so I **tested the prediction**: add *s* ms inside
the locked section (the `after_balance_check=sleep:s` failpoint) and see
whether throughput follows 1 / (h₀ + s). From the baseline 286 tx/s, the
implied lock hold time is h₀ ≈ 3.5 ms.

| Added under lock | Predicted tx/s | Measured tx/s |
|---:|---:|---:|
| 0 ms | (286) | 286 |
| 2 ms | 182 | 170 |
| 5 ms | 118 | 111 |
| 10 ms | 74 | 67 |

The measurements track the model within ~8%, and slightly low because
`asyncio.sleep` overshoots. **Hot-account throughput is determined by lock
hold time.** Anything that shortens the critical section raises the
ceiling; nothing else will.

![Lock-hold experiment and history growth](load-diagnosis.png)

## Finding 3: `balance = SUM(entries)` inside the lock doesn't scale with history

The balance is computed under the lock, and its cost grows with the
account's history. So for a long-lived hot account, lock hold time, and with
it throughput, gets worse over time:

| Prior transfers on the hot account | tx/s | p99 ms |
|---:|---:|---:|
| 0 | 232 | 531 |
| 10,000 | 187 | 629 |
| 100,000 | 81 | 1,416 |
| 500,000 | **15** | 4,908 |

This is the main scaling limit of the current design. A real settlement
account accumulates millions of entries.

## What I would change next, in order

1. **Make the balance read O(1) without a mutable balance field.** Store
   `balance_after` on each entry (a running balance, still append-only),
   so the current balance is the newest entry's value. The catch: the
   *credited* account would then need locking too, to compute its running
   balance safely. That brings back two locks per transfer, so they must be
   taken in a consistent order (sort by account id) to prevent deadlocks, and
   `test_opposing_transfers_do_not_deadlock` already exists to catch a
   mistake. An alternative is periodic balance snapshots, with one trap:
   entry ids come from a sequence at *insert* time, not commit time, so
   "snapshot up to id N" can miss a lower-id entry that commits later. The
   snapshot must be taken under the account lock.
2. **Shorten the critical section.** Do lock, balance check, both inserts,
   and the status update in a single statement (a CTE or stored procedure):
   one round trip under the lock instead of ~4. By Finding 2, that should
   raise the hot-account ceiling roughly in proportion.
3. **Hot accounts by design:** split a merchant's settlement account into
   *k* sub-accounts (sharding), or batch many transfers per lock
   acquisition. TigerBeetle, a database built for financial transactions,
   takes batching to its limit.
4. **Uniform throughput:** run more API processes on separate hardware from
   the load generator, then re-measure. The next bottleneck is likely
   Postgres commit throughput, and it will show up in `pg_stat_activity` as
   WAL waits.
