"""Run load-test experiments and collect evidence about the bottleneck.

For each configuration this script:
  1. resets the load-test database,
  2. starts the API (uvicorn) with the given workers / pool size / PG options,
  3. drives load with loadgen.run_load,
  4. meanwhile samples, every 100ms:
       - what every Postgres backend is doing (pg_stat_activity):
         running on CPU, waiting on a row lock, waiting on WAL flush, idle...
       - CPU used by the API processes vs. Postgres vs. the whole machine
       - the pool's queue depth from /metrics (requests waiting for a conn)
  5. writes everything to loadtest/results/<experiment>.json

Usage:
    python -m loadtest.experiments sweep        # concurrency sweep, both scenarios
    python -m loadtest.experiments bottleneck   # vary one knob at a time
    python -m loadtest.experiments lock_hold    # hot account vs. time under lock
    python -m loadtest.experiments history      # hot account vs. history length
"""

import json
import os
import signal
import subprocess
import sys
import threading
import time
from collections import Counter
from pathlib import Path

import httpx
import psycopg

from loadtest.loadgen import run_load, setup_accounts

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "loadtest" / "results"
ADMIN_URL = os.environ.get("LOAD_ADMIN_DATABASE_URL", "postgresql://postgres@localhost:5432/postgres")
LOAD_DB = "ledger_load"
LOAD_DB_URL = ADMIN_URL.rsplit("/", 1)[0] + "/" + LOAD_DB
PORT = 8123
CLK_TCK = os.sysconf("SC_CLK_TCK")


# ------------------------------------------------------------ process CPU


def _proc_ticks(pid: int) -> int:
    try:
        fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        return int(fields[11]) + int(fields[12])  # utime + stime
    except (FileNotFoundError, ProcessLookupError, IndexError):
        return 0


def _descendants(pid: int) -> set[int]:
    out, frontier = {pid}, [pid]
    children: dict[int, list[int]] = {}
    for p in Path("/proc").iterdir():
        if p.name.isdigit():
            try:
                ppid = int((p / "stat").read_text().rsplit(")", 1)[1].split()[1])
                children.setdefault(ppid, []).append(int(p.name))
            except (FileNotFoundError, ProcessLookupError, IndexError):
                pass
    while frontier:
        for c in children.get(frontier.pop(), []):
            if c not in out:
                out.add(c)
                frontier.append(c)
    return out


def _postgres_pids() -> set[int]:
    pids = set()
    for p in Path("/proc").iterdir():
        if p.name.isdigit():
            try:
                if (p / "comm").read_text().strip() == "postgres":
                    pids.add(int(p.name))
            except (FileNotFoundError, ProcessLookupError):
                pass
    return pids


def _machine_busy_ticks() -> tuple[int, int]:
    vals = [int(v) for v in Path("/proc/stat").read_text().splitlines()[0].split()[1:]]
    idle = vals[3] + vals[4]
    return sum(vals) - idle, sum(vals)


# ---------------------------------------------------------------- sampler


class Sampler(threading.Thread):
    """Samples database wait states, CPU, and pool queue depth during a run."""

    def __init__(self, api_pid: int, base_url: str):
        super().__init__(daemon=True)
        self.api_pid, self.base_url = api_pid, base_url
        self.stop_evt = threading.Event()
        self.backend_states: Counter = Counter()
        self.n_samples = 0
        self.pool_waiting: list[float] = []

    @staticmethod
    def classify(state, wtype, wevent) -> str:
        if state == "idle":
            return "idle (connection unused)"
        if state and state.startswith("idle in transaction"):
            return "idle in txn (waiting on app)"
        if wtype is None:
            return "active: on CPU"
        if wtype == "Lock":
            return "active: waiting on row/txn lock"
        if wevent and wevent.startswith("WAL") or wtype == "IO" and wevent and "WAL" in wevent:
            return "active: WAL write/flush"
        if wtype == "Client":
            return "active: client I/O"
        return f"active: {wtype}:{wevent}"

    def run(self):
        api_pids = _descendants(self.api_pid)
        pg_pids = _postgres_pids()
        cpu0 = (sum(map(_proc_ticks, api_pids)), sum(map(_proc_ticks, pg_pids)), _machine_busy_ticks())
        t0 = time.perf_counter()
        with psycopg.connect(LOAD_DB_URL, autocommit=True) as conn, httpx.Client(timeout=2) as http:
            i = 0
            while not self.stop_evt.wait(0.1):
                rows = conn.execute(
                    "SELECT state, wait_event_type, wait_event FROM pg_stat_activity "
                    "WHERE datname = %s AND backend_type = 'client backend' "
                    "AND pid <> pg_backend_pid()", (LOAD_DB,),
                ).fetchall()
                for r in rows:
                    self.backend_states[self.classify(*r)] += 1
                self.n_samples += 1
                i += 1
                if i % 3 == 0:
                    try:
                        text = http.get(f"{self.base_url}/metrics").text
                        for line in text.splitlines():
                            if line.startswith("ledger_db_requests_waiting "):
                                self.pool_waiting.append(float(line.split()[1]))
                    except httpx.HTTPError:
                        pass
        elapsed = time.perf_counter() - t0
        busy1, total1 = _machine_busy_ticks()
        busy0, total0 = cpu0[2]
        self.cpu = {
            # 100% == one fully used core
            "api_cpu_pct": 100 * (sum(map(_proc_ticks, api_pids)) - cpu0[0]) / CLK_TCK / elapsed,
            "postgres_cpu_pct": 100 * (sum(map(_proc_ticks, _postgres_pids() | pg_pids)) - cpu0[1]) / CLK_TCK / elapsed,
            "machine_busy_pct": 100 * (busy1 - busy0) / max(1, total1 - total0),
            "cores": os.cpu_count(),
        }

    def summary(self) -> dict:
        n = max(1, self.n_samples)
        return {
            "avg_backends_by_state": {k: round(v / n, 2) for k, v in self.backend_states.most_common()},
            "avg_pool_requests_waiting": round(sum(self.pool_waiting) / max(1, len(self.pool_waiting)), 2),
            **{k: round(v, 1) for k, v in self.cpu.items()},
        }


# ------------------------------------------------------------- experiment


def reset_database():
    with psycopg.connect(ADMIN_URL, autocommit=True) as conn:
        conn.execute(f"DROP DATABASE IF EXISTS {LOAD_DB} WITH (FORCE)")
        conn.execute(f"CREATE DATABASE {LOAD_DB}")


def start_server(workers: int, pool_max: int, pgoptions: str = "", failpoints: str = "") -> subprocess.Popen:
    env = {**os.environ, "DATABASE_URL": LOAD_DB_URL, "DB_POOL_MAX_SIZE": str(pool_max),
           "DB_POOL_MIN_SIZE": str(pool_max), "LEDGER_FAILPOINTS": failpoints}
    if pgoptions:
        env["PGOPTIONS"] = pgoptions
    log = open(RESULTS / "server.log", "w")
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.main:app", "--host", "127.0.0.1",
         "--port", str(PORT), "--workers", str(workers), "--no-access-log", "--log-level", "warning"],
        cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT,
    )
    for _ in range(200):
        try:
            if httpx.get(f"http://127.0.0.1:{PORT}/health", timeout=0.5).status_code == 200:
                return proc
        except httpx.TransportError:
            time.sleep(0.1)
    proc.kill()
    raise RuntimeError("server did not start")


def stop_server(proc: subprocess.Popen):
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(15)
    except subprocess.TimeoutExpired:
        proc.kill()


def run_one(label: str, *, scenario: str, concurrency: int, workers: int = 1, pool_max: int = 20,
            pgoptions: str = "", failpoints: str = "", history: int = 0,
            duration: float = 10, n_accounts: int = 1000) -> dict:
    reset_database()
    proc = start_server(workers, pool_max, pgoptions, failpoints)
    base_url = f"http://127.0.0.1:{PORT}"
    try:
        accounts = setup_accounts(base_url, n_accounts, funding=10**12)
        if history:
            seed_history(accounts[0], accounts[1], history)
        sampler = Sampler(proc.pid, base_url)
        sampler.start()
        result = run_load(base_url, accounts, scenario, concurrency, duration)
        sampler.stop_evt.set()
        sampler.join()
    finally:
        stop_server(proc)
    result.update(label=label, workers=workers, pool_max=pool_max, pgoptions=pgoptions,
                  failpoints=failpoints, history=history,
                  evidence=sampler.summary())
    print(f"{label:<44} {result['throughput_rps']:>7.0f} rps  p50 {result['p50_ms']:6.1f}  "
          f"p95 {result['p95_ms']:6.1f}  p99 {result['p99_ms']:6.1f} ms  err {result['error_rate']:.3f}  "
          f"api {result['evidence']['api_cpu_pct']:.0f}% pg {result['evidence']['postgres_cpu_pct']:.0f}%",
          flush=True)
    return result


def sweep():
    out = []
    for scenario in ("uniform", "hot", "hot_dest"):
        for c in (1, 4, 16, 32, 64, 128):
            out.append(run_one(f"{scenario} c={c}", scenario=scenario, concurrency=c))
    (RESULTS / "sweep.json").write_text(json.dumps(out, indent=2))


def bottleneck():
    runs = [
        dict(label="baseline: 1 worker, pool 20", workers=1, pool_max=20),
        dict(label="pool 5", workers=1, pool_max=5),
        dict(label="pool 60", workers=1, pool_max=60),
        dict(label="synchronous_commit=off", workers=1, pool_max=20, pgoptions="-c synchronous_commit=off"),
        dict(label="2 workers x pool 20", workers=2, pool_max=20),
        dict(label="3 workers x pool 20", workers=3, pool_max=20),
    ]
    out = [run_one(r.pop("label"), scenario="uniform", concurrency=64, **r) for r in runs]
    (RESULTS / "bottleneck.json").write_text(json.dumps(out, indent=2))


def seed_history(src: str, dst: str, n: int):
    """Give `src` n prior outgoing transfers (2n entries) so its SUM is expensive."""
    with psycopg.connect(LOAD_DB_URL, autocommit=True) as conn:
        conn.execute(
            """
            WITH t AS (
                INSERT INTO transfers (id, idempotency_key, request_hash, kind, from_account_id,
                                       to_account_id, amount, currency, status)
                SELECT gen_random_uuid(), 'seed-' || g, 'seed', 'transfer', %(src)s, %(dst)s,
                       1, 'USD', 'completed'
                  FROM generate_series(1, %(n)s) g
                RETURNING id
            )
            INSERT INTO entries (transfer_id, account_id, amount)
            SELECT id, %(src)s, -1 FROM t UNION ALL SELECT id, %(dst)s, 1 FROM t
            """,
            {"src": src, "dst": dst, "n": n},
        )
        conn.execute("VACUUM ANALYZE entries")


def lock_hold():
    """Causal test of the hot-account diagnosis: add s ms INSIDE the locked
    section. If the row lock is the bottleneck, throughput ~= 1 / (h0 + s)."""
    out = [run_one(f"hot c=32, +{ms}ms under lock", scenario="hot", concurrency=32,
                   failpoints=f"after_balance_check=sleep:{ms}" if ms else "")
           for ms in (0, 2, 5, 10)]
    (RESULTS / "lock_hold.json").write_text(json.dumps(out, indent=2))


def history():
    """balance = SUM(entries) is O(history). Under the lock, that cost is
    paid serially, so a long-lived hot account gets slower over time."""
    out = [run_one(f"hot c=32, {n:,} prior transfers", scenario="hot", concurrency=32, history=n)
           for n in (0, 10_000, 100_000, 500_000)]
    (RESULTS / "history.json").write_text(json.dumps(out, indent=2))


if __name__ == "__main__":
    RESULTS.mkdir(parents=True, exist_ok=True)
    {"sweep": sweep, "bottleneck": bottleneck, "lock_hold": lock_hold,
     "history": history}[sys.argv[1]]()
