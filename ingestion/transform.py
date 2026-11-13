"""Convert bronze JSON into silver Parquet: one row per city per hour.

For each bronze partition (one extraction run) this reads every city file and
writes a single Parquet file:

    <data_dir>/silver/<dataset>/date=YYYY-MM-DD/hour=HH/<dataset>.parquet

Only the shape and the file format change. Values are kept as they came from the
API; cleaning and filtering happen later in dbt. Re-running overwrites the same
file, so the step is safe to repeat.

Usage:
    python -m ingestion.transform                       # the latest bronze partition
    python -m ingestion.transform --all                 # every bronze partition
    python -m ingestion.transform --date 2026-11-13 --hour 20
"""

import argparse
import datetime as dt
import json
import logging
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from ingestion.config import Settings
from ingestion.extract import Dataset, build_datasets

log = logging.getLogger(__name__)

TIMESTAMP = pa.timestamp("us", tz="UTC")


def silver_schema(dataset: Dataset) -> pa.Schema:
    """Fixed schema per dataset, so every Parquet file has identical columns."""
    return pa.schema(
        [
            pa.field("city", pa.string(), nullable=False),
            pa.field("time", TIMESTAMP, nullable=False),
            *(pa.field(variable, pa.float64()) for variable in dataset.variables),
            pa.field("extracted_at", TIMESTAMP, nullable=False),
        ]
    )


def read_bronze_file(path: Path, dataset: Dataset) -> pa.Table:
    """Flatten one bronze file (one city) into a table with one row per hour."""
    record = json.loads(path.read_text(encoding="utf-8"))
    hourly = record["response"]["hourly"]
    # The extract step requests timezone=GMT, so these naive timestamps are UTC.
    times = [dt.datetime.fromisoformat(t).replace(tzinfo=dt.UTC) for t in hourly["time"]]
    extracted_at = dt.datetime.fromisoformat(record["extracted_at"])

    columns = {
        "city": [record["city"]] * len(times),
        "time": times,
        **{variable: hourly[variable] for variable in dataset.variables},
        "extracted_at": [extracted_at] * len(times),
    }
    return pa.table(columns, schema=silver_schema(dataset))


def list_partitions(data_dir: Path, dataset: str) -> list[Path]:
    """Bronze partition folders for a dataset, oldest first."""
    return sorted((data_dir / "bronze" / dataset).glob("date=*/hour=*"))


def transform_partition(data_dir: Path, dataset: Dataset, partition: Path) -> Path:
    """Convert one bronze partition folder into one silver Parquet file."""
    files = sorted(partition.glob("city=*.json"))
    if not files:
        raise FileNotFoundError(f"no bronze files in {partition.as_posix()}")

    table = pa.concat_tables(read_bronze_file(f, dataset) for f in files)

    relative = partition.relative_to(data_dir / "bronze")
    out = data_dir / "silver" / relative / f"{dataset.name}.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    # Write to a temp file first so a crash never leaves a half-written file behind.
    tmp = out.with_suffix(".parquet.tmp")
    pq.write_table(table, tmp)
    tmp.replace(out)
    log.info("wrote %s (%d rows from %d files)", out.as_posix(), table.num_rows, len(files))
    return out


def run(
    settings: Settings,
    dataset_names: tuple[str, ...] | None = None,
    date: str | None = None,
    hour: str | None = None,
    all_partitions: bool = False,
) -> list[Path]:
    """Transform the chosen partitions for each dataset and return the files written."""
    datasets = build_datasets(settings)
    written: list[Path] = []
    for name in dataset_names or tuple(datasets):
        if date is not None and hour is not None:
            partitions = [settings.data_dir / "bronze" / name / f"date={date}" / f"hour={hour}"]
        else:
            partitions = list_partitions(settings.data_dir, name)
            if not all_partitions:
                partitions = partitions[-1:]
        if not partitions:
            raise FileNotFoundError(f"no bronze partitions found for {name}")
        for partition in partitions:
            written.append(transform_partition(settings.data_dir, datasets[name], partition))
    return written


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dataset", choices=["weather", "air_quality"], help="only this dataset")
    parser.add_argument("--date", help="partition date, YYYY-MM-DD (use with --hour)")
    parser.add_argument("--hour", help="partition hour, 00-23 (use with --date)")
    parser.add_argument("--all", action="store_true", help="every bronze partition")
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
        log.error("transform failed: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
