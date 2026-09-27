from pathlib import Path

from psycopg import AsyncConnection, IsolationLevel
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from app.config import Settings

SCHEMA_PATH = Path(__file__).with_name("schema.sql")
# Arbitrary constant: serializes schema setup when several processes start at once.
SCHEMA_LOCK_ID = 7_345_001


async def _configure(conn: AsyncConnection) -> None:
    # Set explicitly rather than trusting the server default: the correctness
    # argument in service.py depends on READ COMMITTED semantics (each
    # statement sees everything committed before it started).
    await conn.set_isolation_level(IsolationLevel.READ_COMMITTED)
    conn.row_factory = dict_row


def create_pool(settings: Settings) -> AsyncConnectionPool:
    return AsyncConnectionPool(
        settings.database_url,
        min_size=settings.pool_min_size,
        max_size=settings.pool_max_size,
        timeout=settings.pool_timeout_s,
        configure=_configure,
        open=False,
    )


async def apply_schema(pool: AsyncConnectionPool) -> None:
    async with pool.connection() as conn:
        async with conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock(%s)", (SCHEMA_LOCK_ID,))
            await conn.execute(SCHEMA_PATH.read_text())
