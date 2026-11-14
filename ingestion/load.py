"""Load silver Parquet into the Postgres warehouse: raw.<dataset>, one row per city per hour.

For each silver file this:

1. creates raw.<dataset> if it does not exist (primary key: city, time),
2. copies the file into a temporary table with COPY (fast bulk insert),
3. upserts from there into raw.<dataset>, all in one transaction.

Extraction runs overlap on purpose (each asks for yesterday and today), and today's
later hours start out as forecasts. So on a (city, time) clash the row with the newer
`extracted_at` wins: a fresh reading replaces an old forecast, and reloading an old
file never overwrites newer data. Loading the same file twice changes nothing.

Usage:
    python -m ingestion.load                        # the latest silver partition
    python -m ingestion.load --all                  # every silver partition
    python -m ingestion.load --date 2026-11-13 --hour 20
"""

import argparse
import logging
import sys
from pathlib import Path

import psycopg
import pyarrow.parquet as pq
from psycopg import sql

from ingestion.config import Settings
from ingestion.extract import Dataset, build_datasets

log = logging.getLogger(__name__)

DEFAULT_SCHEMA = "raw"


def ensure_table(conn: psycopg.Connection, schema: str, dataset: Dataset) -> None:
    """Create raw.<dataset> if needed, and add any variable columns it is missing."""
    table = sql.Identifier(schema, dataset.name)
    variable_columns = sql.SQL(", ").join(
        sql.SQL("{} double precision").format(sql.Identifier(v)) for v in dataset.variables
    )
    conn.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
    conn.execute(
        sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {table} (
                city         text        NOT NULL,
                time         timestamptz NOT NULL,
                {variables},
                extracted_at timestamptz NOT NULL,
                loaded_at    timestamptz NOT NULL DEFAULT now(),
                PRIMARY KEY (city, time)
            )
            """
        ).format(table=table, variables=variable_columns)
    )
    # A variable added to the extract later becomes a new (empty) column, not an error.
    for variable in dataset.variables:
        conn.execute(
            sql.SQL("ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} double precision").format(
                table, sql.Identifier(variable)
            )
        )


def load_file(conn: psycopg.Connection, path: Path, dataset: Dataset, schema: str) -> int:
    """Upsert one silver Parquet file into <schema>.<dataset>. Returns rows inserted/updated."""
    table = pq.read_table(path)
    columns = ["city", "time", *dataset.variables, "extracted_at"]
    missing = set(columns) - set(table.column_names)
    if missing:
        raise ValueError(f"{path.as_posix()} is missing columns: {sorted(missing)}")

    target = sql.Identifier(schema, dataset.name)
    staging = sql.Identifier(f"load_{dataset.name}")
    column_list = sql.SQL(", ").join(map(sql.Identifier, columns))
    updates = sql.SQL(", ").join(
        sql.SQL("{c} = EXCLUDED.{c}").format(c=sql.Identifier(c))
        for c in [*dataset.variables, "extracted_at"]
    )

    with conn.transaction():
        ensure_table(conn, schema, dataset)
        conn.execute(
            sql.SQL(
                "CREATE TEMP TABLE {staging} (LIKE {target} INCLUDING DEFAULTS) ON COMMIT DROP"
            ).format(staging=staging, target=target)
        )
        with conn.cursor() as cur:
            with cur.copy(
                sql.SQL("COPY {} ({}) FROM STDIN").format(staging, column_list)
            ) as copy:
                for row in table.select(columns).to_pylist():
                    copy.write_row([row[c] for c in columns])

            cur.execute(
                sql.SQL(
                    """
                    INSERT INTO {target} ({columns}, loaded_at)
                    SELECT {columns}, now() FROM {staging}
                    ON CONFLICT (city, time) DO UPDATE
                    SET {updates}, loaded_at = EXCLUDED.loaded_at
                    WHERE EXCLUDED.extracted_at >= {target}.extracted_at
                    """
                ).format(target=target, columns=column_list, staging=staging, updates=updates)
            )
            affected = cur.rowcount

    log.info("loaded %s into %s.%s (%d rows upserted)", path.as_posix(), schema,
             dataset.name, affected)
    return affected


def list_partitions(data_dir: Path, dataset: str) -> list[Path]:
    """Silver partition folders for a dataset, oldest first."""
    return sorted((data_dir / "silver" / dataset).glob("date=*/hour=*"))


def run(
    settings: Settings,
    dataset_names: tuple[str, ...] | None = None,
    date: str | None = None,
    hour: str | None = None,
    all_partitions: bool = False,
    schema: str = DEFAULT_SCHEMA,
) -> int:
    """Load the chosen silver partitions for each dataset. Returns total rows upserted."""
    datasets = build_datasets(settings)
    jobs: list[tuple[Dataset, Path]] = []
    for name in dataset_names or tuple(datasets):
        if date is not None and hour is not None:
            partitions = [settings.data_dir / "silver" / name / f"date={date}" / f"hour={hour}"]
        else:
            partitions = list_partitions(settings.data_dir, name)
            if not all_partitions:
                partitions = partitions[-1:]
        if not partitions:
            raise FileNotFoundError(f"no silver partitions found for {name}")
        for partition in partitions:
            path = partition / f"{name}.parquet"
            if not path.exists():
                raise FileNotFoundError(f"missing silver file {path.as_posix()}")
            jobs.append((datasets[name], path))

    total = 0
    with psycopg.connect(settings.postgres_conninfo()) as conn:
        for dataset, path in jobs:
            total += load_file(conn, path, dataset, schema)
    return total


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dataset", choices=["weather", "air_quality"], help="only this dataset")
    parser.add_argument("--date", help="partition date, YYYY-MM-DD (use with --hour)")
    parser.add_argument("--hour", help="partition hour, 00-23 (use with --date)")
    parser.add_argument("--all", action="store_true", help="every silver partition")
    args = parser.parse_args(argv)
    if (args.date is None) != (args.hour is None):
        parser.error("--date and --hour must be given together")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    try:
        run(
            Settings(),
            dataset_names=(args.dataset,) if args.dataset else None,
            date=args.date,
            hour=args.hour,
            all_partitions=args.all,
        )
    except Exception as exc:  # noqa: BLE001 - log and exit non-zero
        log.error("load failed: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
