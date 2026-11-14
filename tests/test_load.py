"""Tests for ingestion.load.

Most tests need the Postgres from docker-compose (settings come from .env). Each test
works in its own throwaway schema, which is dropped afterwards, so the real raw schema
is never touched. If Postgres is not reachable, those tests are skipped.
"""

import datetime as dt
import uuid

import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from psycopg import sql

from ingestion import load as ld
from ingestion.config import Settings
from ingestion.extract import build_datasets
from ingestion.transform import silver_schema

T0 = dt.datetime(2026, 11, 13, 0, 0, tzinfo=dt.UTC)
OLD_RUN = dt.datetime(2026, 11, 13, 20, 0, tzinfo=dt.UTC)
NEW_RUN = dt.datetime(2026, 11, 13, 21, 0, tzinfo=dt.UTC)


@pytest.fixture
def settings(tmp_path):
    # Postgres connection from .env; data files in a temp folder.
    return Settings(data_dir=tmp_path)


@pytest.fixture
def weather(settings):
    return build_datasets(settings)["weather"]


@pytest.fixture
def conn(settings):
    try:
        connection = psycopg.connect(settings.postgres_conninfo(), connect_timeout=3)
    except psycopg.OperationalError as exc:
        pytest.skip(f"Postgres not reachable (run `docker compose up -d`): {exc}")
    with connection:
        yield connection


@pytest.fixture
def schema(conn):
    name = f"test_load_{uuid.uuid4().hex[:8]}"
    yield name
    conn.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(name)))


def write_silver(data_dir, dataset, rows, date="2026-11-13", hour="20"):
    """rows: list of (city, time, value, extracted_at); value goes in every variable."""
    columns = {
        "city": [r[0] for r in rows],
        "time": [r[1] for r in rows],
        **{v: [r[2] for r in rows] for v in dataset.variables},
        "extracted_at": [r[3] for r in rows],
    }
    path = data_dir / "silver" / dataset.name / f"date={date}" / f"hour={hour}"
    path.mkdir(parents=True, exist_ok=True)
    out = path / f"{dataset.name}.parquet"
    pq.write_table(pa.table(columns, schema=silver_schema(dataset)), out)
    return out


def fetch_rows(conn, schema, table):
    return conn.execute(
        sql.SQL("SELECT city, time, temperature_2m, extracted_at FROM {} ORDER BY city, time")
        .format(sql.Identifier(schema, table))
    ).fetchall()


def hours(n):
    return [T0 + dt.timedelta(hours=h) for h in range(n)]


def test_first_load_creates_table_and_inserts_rows(conn, schema, weather, tmp_path):
    path = write_silver(
        tmp_path, weather,
        [("berlin", t, 1.5, OLD_RUN) for t in hours(2)] + [("sarajevo", T0, None, OLD_RUN)],
    )

    assert ld.load_file(conn, path, weather, schema) == 3

    assert fetch_rows(conn, schema, "weather") == [
        ("berlin", T0, 1.5, OLD_RUN),
        ("berlin", T0 + dt.timedelta(hours=1), 1.5, OLD_RUN),
        ("sarajevo", T0, None, OLD_RUN),  # nulls stay null
    ]


def test_loading_the_same_file_twice_does_not_duplicate(conn, schema, weather, tmp_path):
    path = write_silver(tmp_path, weather, [("berlin", t, 1.5, OLD_RUN) for t in hours(3)])

    ld.load_file(conn, path, weather, schema)
    ld.load_file(conn, path, weather, schema)

    assert len(fetch_rows(conn, schema, "weather")) == 3


def test_newer_extraction_replaces_older_one(conn, schema, weather, tmp_path):
    old = write_silver(tmp_path, weather, [("berlin", T0, 1.0, OLD_RUN)], hour="20")
    new = write_silver(tmp_path, weather, [("berlin", T0, 2.0, NEW_RUN)], hour="21")

    ld.load_file(conn, old, weather, schema)
    ld.load_file(conn, new, weather, schema)

    assert fetch_rows(conn, schema, "weather") == [("berlin", T0, 2.0, NEW_RUN)]


def test_older_extraction_never_overwrites_newer_one(conn, schema, weather, tmp_path):
    old = write_silver(tmp_path, weather, [("berlin", T0, 1.0, OLD_RUN)], hour="20")
    new = write_silver(tmp_path, weather, [("berlin", T0, 2.0, NEW_RUN)], hour="21")

    ld.load_file(conn, new, weather, schema)
    assert ld.load_file(conn, old, weather, schema) == 0  # e.g. a backfill with --all

    assert fetch_rows(conn, schema, "weather") == [("berlin", T0, 2.0, NEW_RUN)]


def test_file_with_missing_columns_is_rejected_and_nothing_loads(conn, schema, weather, tmp_path):
    path = tmp_path / "bad.parquet"
    pq.write_table(pa.table({"city": ["berlin"], "time": [T0]}), path)

    with pytest.raises(ValueError, match="missing columns"):
        ld.load_file(conn, path, weather, schema)

    exists = conn.execute(
        "SELECT to_regclass(%s)", [f"{schema}.weather"]
    ).fetchone()[0]
    assert exists is None


def test_run_loads_latest_partition_by_default(conn, schema, settings, weather, tmp_path):
    write_silver(tmp_path, weather, [("berlin", T0, 1.0, OLD_RUN)], hour="20")
    write_silver(tmp_path, weather, [("berlin", T0, 2.0, NEW_RUN)], hour="21")

    assert ld.run(settings, dataset_names=("weather",), schema=schema) == 1
    assert fetch_rows(conn, schema, "weather") == [("berlin", T0, 2.0, NEW_RUN)]


def test_run_without_silver_data_is_an_error(settings):
    # Fails before connecting, so this one needs no database.
    with pytest.raises(FileNotFoundError):
        ld.run(settings, dataset_names=("weather",))
