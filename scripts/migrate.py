from __future__ import annotations

import hashlib
from pathlib import Path

import psycopg

from common.config import get_settings
from common.logging_config import configure_logging, get_logger

LOGGER = get_logger(__name__)
MIGRATION_DIR = Path(__file__).resolve().parents[1] / "migrations"


def migration_checksum(sql: str) -> str:
    return hashlib.sha256(sql.encode("utf-8")).hexdigest()


def migration_files() -> tuple[Path, ...]:
    return tuple(sorted(MIGRATION_DIR.glob("*.sql")))


def apply_migrations() -> None:
    settings = get_settings()
    with psycopg.connect(str(settings.postgres_dsn), autocommit=True) as connection:
        connection.execute("""
            CREATE TABLE IF NOT EXISTS public.schema_migrations (
                version text PRIMARY KEY,
                checksum text NOT NULL,
                applied_at timestamptz NOT NULL DEFAULT now()
            )
            """)

        for path in migration_files():
            sql = path.read_text(encoding="utf-8")
            checksum = migration_checksum(sql)
            row = connection.execute(
                "SELECT checksum FROM public.schema_migrations WHERE version = %s",
                (path.name,),
            ).fetchone()

            if row is not None:
                if row[0] != checksum:
                    raise RuntimeError(f"Applied migration changed: {path.name}")
                LOGGER.info("Migration already applied: %s", path.name)
                continue

            with connection.transaction():
                connection.execute(sql)
                connection.execute(
                    """
                    INSERT INTO public.schema_migrations (version, checksum)
                    VALUES (%s, %s)
                    """,
                    (path.name, checksum),
                )
            LOGGER.info("Migration applied: %s", path.name)


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    apply_migrations()


if __name__ == "__main__":
    main()
