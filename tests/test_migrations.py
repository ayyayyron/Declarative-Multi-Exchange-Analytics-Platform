from scripts.migrate import migration_checksum, migration_files


def test_migrations_are_ordered_and_immutable_by_checksum() -> None:
    paths = migration_files()

    assert [path.name for path in paths] == [
        "001_create_analytics_schema.sql",
        "002_create_cross_exchange_view.sql",
        "003_create_live_market_tables.sql",
    ]
    assert migration_checksum("SELECT 1;") == migration_checksum("SELECT 1;")
    assert migration_checksum("SELECT 1;") != migration_checksum("SELECT 2;")
