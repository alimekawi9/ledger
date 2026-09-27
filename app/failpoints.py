"""Fault injection for crash testing.

Named failpoints are compiled into the transfer path and are no-ops unless
enabled via the LEDGER_FAILPOINTS environment variable, e.g.

    LEDGER_FAILPOINTS="after_debit=crash"
    LEDGER_FAILPOINTS="after_balance_check=sleep:20,after_commit=crash"

Actions:
  crash     SIGKILL this process. Uncatchable: no finally blocks, no rollback
            from Python, no graceful shutdown. Same as the OOM killer or a
            power loss from the database's point of view.
  sleep:MS  pause MS milliseconds (widens race windows in concurrency tests).

This is the same idea as failpoints in FoundationDB / TiKV: the crash happens
at an exact line of code, so the test is deterministic instead of hoping a
`kill -9` from outside lands at the right moment.
"""

import asyncio
import os
import signal

_ACTIVE: dict[str, str] = {}


def load_from_env() -> None:
    _ACTIVE.clear()
    spec = os.environ.get("LEDGER_FAILPOINTS", "").strip()
    for item in filter(None, (s.strip() for s in spec.split(","))):
        name, _, action = item.partition("=")
        _ACTIVE[name.strip()] = action.strip()


async def failpoint(name: str) -> None:
    action = _ACTIVE.get(name)
    if action is None:
        return
    if action == "crash":
        os.kill(os.getpid(), signal.SIGKILL)
    elif action.startswith("sleep:"):
        await asyncio.sleep(int(action.split(":", 1)[1]) / 1000)
    else:
        raise ValueError(f"unknown failpoint action {action!r} for {name!r}")
