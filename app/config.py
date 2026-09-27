import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    database_url: str
    pool_min_size: int
    pool_max_size: int
    pool_timeout_s: float


def load_settings() -> Settings:
    return Settings(
        database_url=os.environ.get(
            "DATABASE_URL", "postgresql://postgres@localhost:5432/ledger"
        ),
        pool_min_size=int(os.environ.get("DB_POOL_MIN_SIZE", "4")),
        pool_max_size=int(os.environ.get("DB_POOL_MAX_SIZE", "20")),
        pool_timeout_s=float(os.environ.get("DB_POOL_TIMEOUT_S", "10")),
    )
