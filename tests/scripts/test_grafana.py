import json
import os
import re
import stat
from collections.abc import Iterator
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
import yaml
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict

from common.config import Settings
from common.contracts.candle import DISPLAY_CANDLE_INTERVALS
from scripts import setup_grafana
from scripts.grafana_dashboard import build_dashboard
from tests.dashboard.test_rollups import dsn as dsn
from tests.dashboard.test_rollups import rollup_database as rollup_database


def test_provisioned_dashboard_matches_builder() -> None:
    stored = json.loads(Path("infra/grafana/dashboards/market.json").read_text())
    assert stored == build_dashboard()
    assert stored["timezone"] == "utc"
    interval = stored["templating"]["list"][-1]
    assert interval["query"].split(",") == list(DISPLAY_CANDLE_INTERVALS)
    ids = [panel["id"] for panel in stored["panels"]]
    assert len(ids) == len(set(ids))


def test_grafana_is_optional_local_and_has_no_committed_password() -> None:
    compose = yaml.safe_load(Path("docker-compose.yml").read_text())
    service = compose["services"]["grafana"]
    assert service["profiles"] == ["grafana"]
    assert service["ports"] == ["127.0.0.1:3000:3000"]
    assert service["environment"]["GF_AUTH_ANONYMOUS_ORG_ROLE"] == "Viewer"
    assert service["environment"]["GF_PLUGINS_PREINSTALL_DISABLED"] == "true"
    assert all("PASSWORD" not in key for key in service["environment"])
    assert ".env.grafana" in Path(".gitignore").read_text().splitlines()


@pytest.fixture
def reader(
    dsn: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[tuple[str, dict[str, str]]]:
    role = "grafana_test_" + uuid4().hex
    env = tmp_path / ".env.grafana"
    monkeypatch.setattr(setup_grafana, "READER", role)
    monkeypatch.setattr(setup_grafana, "ENV_PATH", env)
    monkeypatch.setattr(
        setup_grafana, "get_settings", lambda: Settings(postgres_dsn=dsn)
    )
    monkeypatch.setattr(setup_grafana, "apply_migrations", lambda: None)
    try:
        setup_grafana.setup()
        values = dict(line.split("=", 1) for line in env.read_text().splitlines())
        assert stat.S_IMODE(env.stat().st_mode) == 0o600
        setup_grafana.setup()
        assert values == dict(
            line.split("=", 1) for line in env.read_text().splitlines()
        )
        params = conninfo_to_dict(dsn) | {
            "user": role,
            "password": values["GRAFANA_DB_PASSWORD"],
        }
        yield role, params
    finally:
        with psycopg.connect(dsn, autocommit=True) as connection:
            connection.execute(sql.SQL("DROP OWNED BY {}").format(sql.Identifier(role)))
            connection.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))


integration = pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1",
    reason="Needs isolated PostgreSQL",
)


@integration
def test_reader_is_read_only_even_when_default_read_only_is_disabled(
    reader: tuple[str, dict[str, str]],
) -> None:
    _, params = reader
    with psycopg.connect(**params) as connection:
        assert connection.execute("SHOW default_transaction_read_only").fetchone() == (
            "on",
        )
        connection.execute("SELECT count(*) FROM raw_market.candles").fetchone()
        connection.execute("SET default_transaction_read_only=off")
        connection.commit()
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            connection.execute("DELETE FROM raw_market.candles WHERE false")
        connection.rollback()
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            connection.execute("SELECT * FROM raw_market.rejected_frames")
        connection.rollback()


@integration
def test_all_panel_queries_execute_for_every_interval(
    reader: tuple[str, dict[str, str]],
) -> None:
    _, params = reader
    for interval in DISPLAY_CANDLE_INTERVALS:
        with psycopg.connect(**params) as connection:
            for panel in build_dashboard()["panels"]:
                for query in panel.get("targets", []):
                    statement = query["rawSql"]
                    for name, value in (
                        ("symbol", "BTCUSDT"),
                        ("exchange", "binance"),
                        ("interval", interval),
                    ):
                        statement = statement.replace(
                            "${" + name + ":sqlstring}", "'" + value + "'"
                        )
                    statement = statement.replace(
                        "$__unixEpochFrom()", "1792000000"
                    ).replace("$__unixEpochTo()", "1793000000")
                    statement = re.sub(
                        r"\$__timeFilter\((\w+)\)",
                        r"\1 >= to_timestamp(1792000000)",
                        statement,
                    )
                    connection.execute(statement).fetchall()
