"""Test harness.

Every test talks to a REAL uvicorn process over HTTP, backed by a REAL
Postgres. Nothing is mocked: the point is to exercise actual row locks,
unique-index waits, connection pools, and process death.
"""

import asyncio
import os
import signal
import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path

import httpx
import psycopg
import pytest

ROOT = Path(__file__).resolve().parent.parent
TEST_DB_URL = os.environ.get(
    "LEDGER_TEST_DATABASE_URL", "postgresql://postgres@localhost:5432/ledger_test"
)


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class LedgerServer:
    """A ledger API subprocess. Can be started with failpoints enabled."""

    def __init__(self, failpoints: str = "", pool_max_size: int = 30):
        self.port = _free_port()
        self.url = f"http://127.0.0.1:{self.port}"
        self.log_path = ROOT / ".test-logs" / f"server-{self.port}.log"
        env = {
            **os.environ,
            "DATABASE_URL": TEST_DB_URL,
            "LEDGER_FAILPOINTS": failpoints,
            "DB_POOL_MAX_SIZE": str(pool_max_size),
        }
        self.log_path.parent.mkdir(exist_ok=True)
        self._log = open(self.log_path, "w")
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "app.main:app",
             "--host", "127.0.0.1", "--port", str(self.port), "--no-access-log"],
            cwd=ROOT, env=env, stdout=self._log, stderr=subprocess.STDOUT,
        )
        self._wait_healthy()

    def _wait_healthy(self, timeout: float = 15) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError(f"server exited early; see {self.log_path}")
            try:
                if httpx.get(f"{self.url}/health", timeout=0.5).status_code == 200:
                    return
            except httpx.TransportError:
                pass
            time.sleep(0.05)
        raise RuntimeError(f"server not healthy in {timeout}s; see {self.log_path}")

    def wait_for_exit(self, timeout: float = 10) -> int:
        return self.proc.wait(timeout)

    def stop(self) -> None:
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            try:
                self.proc.wait(10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
        self._log.close()

    def logs(self) -> str:
        self._log.flush()
        return self.log_path.read_text()


class Api:
    """Thin synchronous client for setup and assertions."""

    def __init__(self, base_url: str):
        self.base_url = base_url
        self.http = httpx.Client(base_url=base_url, timeout=30)

    def account(self, name: str = "acct", currency: str = "USD") -> str:
        r = self.http.post("/accounts", json={"name": name, "currency": currency})
        assert r.status_code == 201, r.text
        return r.json()["id"]

    def deposit(self, account_id: str, amount: int, key: str | None = None) -> httpx.Response:
        return self.http.post(
            "/deposits",
            json={"account_id": account_id, "amount": amount},
            headers={"Idempotency-Key": key or f"dep-{uuid.uuid4()}"},
        )

    def funded_account(self, amount: int, name: str = "funded") -> str:
        acct = self.account(name)
        assert self.deposit(acct, amount).status_code == 201
        return acct

    def transfer(self, src: str, dst: str, amount: int, key: str | None = None) -> httpx.Response:
        return self.http.post(
            "/transfers",
            json={"from_account_id": src, "to_account_id": dst, "amount": amount},
            headers={"Idempotency-Key": key or f"tx-{uuid.uuid4()}"},
        )

    def balance(self, account_id: str) -> int:
        r = self.http.get(f"/accounts/{account_id}/balance")
        assert r.status_code == 200, r.text
        return r.json()["balance"]


def transfer_request(src: str, dst: str, amount: int, key: str | None = None) -> dict:
    return {
        "url": "/transfers",
        "json": {"from_account_id": src, "to_account_id": dst, "amount": amount},
        "headers": {"Idempotency-Key": key or f"tx-{uuid.uuid4()}"},
    }


def fire_concurrently(base_url: str, requests: list[dict]) -> list[httpx.Response]:
    """POST all requests at once, each on its own connection.

    An asyncio.Event acts as a starting gun so every request is in flight
    together instead of trickling out as tasks get created.
    """

    async def main():
        go = asyncio.Event()
        limits = httpx.Limits(max_connections=len(requests), max_keepalive_connections=0)
        async with httpx.AsyncClient(base_url=base_url, timeout=60, limits=limits) as client:

            async def one(req):
                await go.wait()
                return await client.post(**req)

            tasks = [asyncio.create_task(one(r)) for r in requests]
            await asyncio.sleep(0.05)
            go.set()
            return await asyncio.gather(*tasks)

    return asyncio.run(main())


# ------------------------------------------------------------------ fixtures


@pytest.fixture(scope="session")
def server():
    srv = LedgerServer()
    yield srv
    srv.stop()


@pytest.fixture
def api(server):
    return Api(server.url)


@pytest.fixture
def db():
    with psycopg.connect(TEST_DB_URL, autocommit=True) as conn:
        yield conn


@pytest.fixture(autouse=True)
def clean_ledger_and_check_invariants(request):
    """Empty the ledger before each test, verify its invariants after.

    The invariant check runs after EVERY test, so any test that corrupted
    the ledger (unbalanced transfer, stuck 'pending' row, overdraft) fails
    even if its own assertions missed it.
    """
    with psycopg.connect(TEST_DB_URL, autocommit=True) as conn:
        _ensure_schema(conn)
        conn.execute("TRUNCATE entries, transfers, accounts")
    yield
    with psycopg.connect(TEST_DB_URL, autocommit=True) as conn:
        if "corrupts_ledger" in request.keywords:
            # Raw-SQL control experiments bypass the ledger's triggers (and
            # some overdraw by design); wipe instead of checking.
            conn.execute("TRUNCATE entries, transfers, accounts")
        else:
            assert_ledger_consistent(conn)


def _ensure_schema(conn) -> None:
    conn.execute((ROOT / "app" / "schema.sql").read_text())


def assert_ledger_consistent(conn) -> None:
    total = conn.execute("SELECT COALESCE(SUM(amount), 0) FROM entries").fetchone()[0]
    assert total == 0, f"ledger does not sum to zero: {total}"

    pending = conn.execute("SELECT count(*) FROM transfers WHERE status = 'pending'").fetchone()[0]
    assert pending == 0, f"{pending} transfers committed in 'pending' state"

    # Every completed transfer: exactly one debit of -amount on the source and
    # one credit of +amount on the destination. Rejected: no entries at all.
    bad = conn.execute(
        """
        SELECT t.id, t.status, count(e.id) AS n_entries
          FROM transfers t LEFT JOIN entries e ON e.transfer_id = t.id
         GROUP BY t.id, t.status
        HAVING (t.status = 'completed' AND (count(e.id) <> 2 OR NOT bool_and(
                 (e.account_id = t.from_account_id AND e.amount = -t.amount) OR
                 (e.account_id = t.to_account_id   AND e.amount =  t.amount))))
            OR (t.status = 'rejected' AND count(e.id) <> 0)
        """
    ).fetchall()
    assert not bad, f"malformed transfers: {bad}"

    # Every customer entry's balance_after equals the running sum of that
    # account's entries in id order: the stored chain matches the history.
    broken_chain = conn.execute(
        """
        SELECT id, account_id, balance_after, running FROM (
            SELECT e.id, e.account_id, e.balance_after,
                   SUM(e.amount) OVER (PARTITION BY e.account_id ORDER BY e.id) AS running
              FROM entries e JOIN accounts a ON a.id = e.account_id
             WHERE a.kind = 'customer') x
         WHERE balance_after IS DISTINCT FROM running
         LIMIT 5
        """
    ).fetchall()
    assert not broken_chain, f"balance_after disagrees with history: {broken_chain}"

    negative = conn.execute(
        """
        SELECT a.id, SUM(e.amount) FROM accounts a JOIN entries e ON e.account_id = a.id
         WHERE a.kind = 'customer' GROUP BY a.id HAVING SUM(e.amount) < 0
        """
    ).fetchall()
    assert not negative, f"customer accounts overdrawn: {negative}"
