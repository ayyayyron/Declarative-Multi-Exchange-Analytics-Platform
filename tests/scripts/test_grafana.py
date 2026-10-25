import json
import os
import re
import stat
from collections.abc import Iterator
from pathlib import Path
from unittest.mock import MagicMock
from uuid import uuid4

import psycopg
import pytest
import yaml
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict

from common.config import Settings
from common.contracts.candle import DISPLAY_CANDLE_INTERVALS
from scripts import grafana_dashboard, setup_grafana
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


def test_freshness_threshold_applies_to_all_status_queries() -> None:
    panels = {panel["id"]: panel for panel in build_dashboard(60)["panels"]}
    for identifier in (2, 4, 6):
        query = panels[identifier]["targets"][0]["rawSql"]
        assert "interval '60 seconds'" in query
        assert "interval '15 seconds'" not in query


@pytest.mark.parametrize("threshold", [0, -1, True, "60"])
def test_invalid_freshness_threshold_is_rejected(threshold: object) -> None:
    with pytest.raises(ValueError, match="positive integer"):
        build_dashboard(threshold)


def test_canonical_setup_fails_before_migrations_or_credentials(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    env = tmp_path / ".env.grafana"
    migrate = MagicMock()
    monkeypatch.setattr(setup_grafana, "ENV_PATH", env)
    monkeypatch.setattr(setup_grafana, "apply_migrations", migrate)
    monkeypatch.setattr(
        setup_grafana,
        "get_settings",
        lambda: Settings(_env_file=None, serving_mode="canonical"),
    )
    with pytest.raises(ValueError, match="SERVING_MODE=raw only"):
        setup_grafana.setup()
    migrate.assert_not_called()
    assert not env.exists()


@pytest.mark.parametrize("mode", ["raw", "canonical"])
def test_generator_uses_settings_and_rejects_canonical_before_writing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, mode: str
) -> None:
    destination = tmp_path / "market.json"
    destination.write_text("previous dashboard")
    monkeypatch.setattr(grafana_dashboard, "DASHBOARD_PATH", destination)
    monkeypatch.setattr(
        grafana_dashboard,
        "get_settings",
        lambda: Settings(
            _env_file=None, serving_mode=mode, dashboard_stale_after_seconds=60
        ),
    )
    if mode == "canonical":
        with pytest.raises(ValueError, match="SERVING_MODE=raw only"):
            grafana_dashboard.main()
        assert destination.read_text() == "previous dashboard"
    else:
        grafana_dashboard.main()
        assert json.loads(destination.read_text()) == build_dashboard(60)


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
