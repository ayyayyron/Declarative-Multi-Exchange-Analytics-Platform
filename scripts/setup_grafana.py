"""Create a restricted Grafana login and private local container configuration."""

import os
import secrets
from pathlib import Path

import psycopg
from psycopg import sql

from common.config import get_settings
from scripts.migrate import apply_migrations

READER = "grafana_reader"
ENV_PATH = Path(".env.grafana")
READ_TABLES = ("candles", "latest_trades", "live_trades", "pipeline_status")


def setup() -> None:
    """Reuse generated credentials; fail rather than adopt an unrelated SQL role."""
    apply_migrations()
    settings = get_settings()
    if ENV_PATH.exists():
        values = dict(
            line.split("=", 1) for line in ENV_PATH.read_text().splitlines() if line
        )
        password = values["GRAFANA_DB_PASSWORD"]
    else:
        values = {
            "GRAFANA_DB_PASSWORD": secrets.token_urlsafe(32),
            "GF_SECURITY_ADMIN_USER": "admin",
            "GF_SECURITY_ADMIN_PASSWORD": secrets.token_urlsafe(32),
        }
        password = values["GRAFANA_DB_PASSWORD"]
    with psycopg.connect(str(settings.postgres_dsn)) as connection:
        existing = connection.execute(
            "SELECT 1 FROM pg_roles WHERE rolname=%s", (READER,)
        ).fetchone()
        if existing and not ENV_PATH.exists():
            raise RuntimeError(
                "grafana_reader already exists without local credentials; "
                "inspect before reusing it"
            )
        if (
            existing
            and connection.execute(
                "SELECT 1 FROM pg_auth_members m JOIN pg_roles r ON r.oid=m.member "
                "WHERE r.rolname=%s",
                (READER,),
            ).fetchone()
        ):
            raise RuntimeError("Grafana reader has unexpected role memberships")
        if not existing:
            connection.execute(
                sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
                    sql.Identifier(READER), sql.Literal(password)
                )
            )
        connection.execute(
            sql.SQL(
                "ALTER ROLE {} NOSUPERUSER NOCREATEDB NOCREATEROLE "
                "NOREPLICATION NOINHERIT"
            ).format(sql.Identifier(READER))
        )
        connection.execute(
            sql.SQL("ALTER ROLE {} SET default_transaction_read_only = on").format(
                sql.Identifier(READER)
            )
        )
        connection.execute(
            sql.SQL("ALTER ROLE {} SET statement_timeout = '5s'").format(
                sql.Identifier(READER)
            )
        )
        database = connection.execute("SELECT current_database()").fetchone()
        assert database is not None
        values["GRAFANA_DB_NAME"] = str(database[0])
        connection.execute(
            sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
                sql.Identifier(database[0]), sql.Identifier(READER)
            )
        )
        connection.execute(
            sql.SQL("GRANT USAGE ON SCHEMA raw_market TO {}").format(
                sql.Identifier(READER)
            )
        )
        for table in READ_TABLES:
            connection.execute(
                sql.SQL("GRANT SELECT ON raw_market.{} TO {}").format(
                    sql.Identifier(table), sql.Identifier(READER)
                )
            )
            writable = connection.execute(
                "SELECT has_table_privilege(%s,%s,'INSERT,UPDATE,DELETE,TRUNCATE')",
                (READER, f"raw_market.{table}"),
            ).fetchone()
            if writable and writable[0]:
                raise RuntimeError("Grafana reader has unexpected write permissions")
        connection.execute(
            sql.SQL(
                "GRANT EXECUTE ON FUNCTION "
                "raw_market.candle_rollups(text,text,text,integer) TO {}"
            ).format(sql.Identifier(READER))
        )
        descriptor = os.open(ENV_PATH, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(descriptor, "w") as output:
            output.write("".join(f"{key}={value}\n" for key, value in values.items()))
        ENV_PATH.chmod(0o600)
    print("Grafana reader configured; private settings saved in .env.grafana")


if __name__ == "__main__":
    setup()
