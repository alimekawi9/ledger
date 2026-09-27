"""Closed-loop load generator for the ledger API.

C virtual clients each loop: send one transfer, wait for the response,
record latency, repeat. Throughput is completed requests / measured
seconds; latencies are client-observed (they include queueing anywhere in
the stack). The load is split across several OS processes so the Python
client itself is less likely to be the bottleneck.

Scenarios:
  uniform  random source/destination among many accounts (low contention)
  hot      every transfer debits ONE account (maximum lock contention)
  hot_dest every transfer credits ONE account (a merchant being paid)
"""

import argparse
import asyncio
import json
import multiprocessing as mp
import random
import time
import uuid

import httpx


def setup_accounts(base_url: str, n_accounts: int, funding: int) -> list[str]:
    with httpx.Client(base_url=base_url, timeout=30) as c:
        ids = []
        for i in range(n_accounts):
            acct = c.post("/accounts", json={"name": f"load-{i}", "currency": "USD"}).json()["id"]
            r = c.post("/deposits", json={"account_id": acct, "amount": funding},
                       headers={"Idempotency-Key": str(uuid.uuid4())})
            r.raise_for_status()
            ids.append(acct)
        return ids


async def _client_loop(client, accounts, scenario, deadline, warmup_until, rng, out):
    while True:
        now = time.perf_counter()
        if now >= deadline:
            return
        if scenario == "hot":
            src, dst = accounts[0], rng.choice(accounts[1:])
        elif scenario == "hot_dest":
            src, dst = rng.choice(accounts[1:]), accounts[0]
        else:
            src, dst = rng.sample(accounts, 2)
        start = time.perf_counter()
        try:
            r = await client.post(
                "/transfers",
                json={"from_account_id": src, "to_account_id": dst, "amount": 1},
                headers={"Idempotency-Key": str(uuid.uuid4())},
            )
            status = r.status_code
        except httpx.HTTPError as e:
            status = f"exc:{type(e).__name__}"
        end = time.perf_counter()
        if start >= warmup_until and end <= deadline:
            out.append((end, end - start, status))


async def _run_process(base_url, accounts, scenario, concurrency, duration, warmup, seed):
    rng = random.Random(seed)
    limits = httpx.Limits(max_connections=concurrency, max_keepalive_connections=concurrency)
    samples: list = []
    t0 = time.perf_counter()
    warmup_until, deadline = t0 + warmup, t0 + warmup + duration
    async with httpx.AsyncClient(base_url=base_url, timeout=60, limits=limits) as client:
        await asyncio.gather(*[
            _client_loop(client, accounts, scenario, deadline, warmup_until, rng, samples)
            for _ in range(concurrency)
        ])
    return samples


def _process_entry(args):
    return asyncio.run(_run_process(*args))


def percentile(sorted_vals, p):
    if not sorted_vals:
        return float("nan")
    k = (len(sorted_vals) - 1) * p / 100
    f, c = int(k), min(int(k) + 1, len(sorted_vals) - 1)
    return sorted_vals[f] + (sorted_vals[c] - sorted_vals[f]) * (k - f)


def run_load(base_url, accounts, scenario, concurrency, duration, warmup=2.0, processes=2) -> dict:
    processes = max(1, min(processes, concurrency))
    shares = [concurrency // processes + (1 if i < concurrency % processes else 0)
              for i in range(processes)]
    args = [(base_url, accounts, scenario, share, duration, warmup, i)
            for i, share in enumerate(shares)]
    with mp.get_context("spawn").Pool(processes) as pool:
        samples = [s for chunk in pool.map(_process_entry, args) for s in chunk]

    lat_ok = sorted(l for _, l, s in samples if s in (201, 422))
    statuses: dict[str, int] = {}
    for _, _, s in samples:
        statuses[str(s)] = statuses.get(str(s), 0) + 1
    errors = sum(n for s, n in statuses.items() if s not in ("201", "422"))
    return {
        "scenario": scenario,
        "concurrency": concurrency,
        "duration_s": duration,
        "requests": len(samples),
        "throughput_rps": len(lat_ok) / duration,
        "error_rate": errors / max(1, len(samples)),
        "statuses": statuses,
        "p50_ms": percentile(lat_ok, 50) * 1000,
        "p95_ms": percentile(lat_ok, 95) * 1000,
        "p99_ms": percentile(lat_ok, 99) * 1000,
        "max_ms": (lat_ok[-1] * 1000) if lat_ok else float("nan"),
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--url", default="http://127.0.0.1:8000")
    ap.add_argument("--scenario", choices=["uniform", "hot", "hot_dest"], default="uniform")
    ap.add_argument("--concurrency", type=int, default=32)
    ap.add_argument("--duration", type=float, default=15)
    ap.add_argument("--accounts", type=int, default=200)
    ap.add_argument("--processes", type=int, default=2)
    args = ap.parse_args()
    accounts = setup_accounts(args.url, args.accounts, funding=10**12)
    print(json.dumps(run_load(args.url, accounts, args.scenario, args.concurrency,
                              args.duration, processes=args.processes), indent=2))


if __name__ == "__main__":
    main()
