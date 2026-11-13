import datetime as dt
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ingestion import transform as tr
from ingestion.config import Settings
from ingestion.extract import build_datasets

EXTRACTED_AT = dt.datetime(2026, 11, 13, 20, 41, tzinfo=dt.UTC)


@pytest.fixture
def settings(tmp_path):
    return Settings(_env_file=None, data_dir=tmp_path)


@pytest.fixture
def weather(settings):
    return build_datasets(settings)["weather"]


def write_bronze(data_dir, dataset, city, date="2026-11-13", hour="20", overrides=None):
    hourly = {"time": ["2026-11-13T00:00", "2026-11-13T01:00"]}
    for variable in dataset.variables:
        hourly[variable] = [1.5, 2.5]
    hourly.update(overrides or {})
    record = {
        "city": city,
        "dataset": dataset.name,
        "extracted_at": EXTRACTED_AT.isoformat(),
        "request": {},
        "response": {"latitude": 0.0, "longitude": 0.0, "hourly": hourly},
    }
    path = data_dir / "bronze" / dataset.name / f"date={date}" / f"hour={hour}"
    path.mkdir(parents=True, exist_ok=True)
    (path / f"city={city}.json").write_text(json.dumps(record), encoding="utf-8")
    return path


def test_partition_becomes_one_row_per_city_per_hour(settings, weather, tmp_path):
    partition = write_bronze(tmp_path, weather, "sarajevo")
    write_bronze(tmp_path, weather, "berlin")

    out = tr.transform_partition(tmp_path, weather, partition)

    assert out == tmp_path / "silver/weather/date=2026-11-13/hour=20/weather.parquet"
    table = pq.read_table(out)
    assert table.schema.equals(tr.silver_schema(weather))
    rows = table.to_pylist()
    assert [(r["city"], r["time"].hour) for r in rows] == [
        ("berlin", 0),
        ("berlin", 1),
        ("sarajevo", 0),
        ("sarajevo", 1),
    ]
    assert rows[0]["time"] == dt.datetime(2026, 11, 13, 0, 0, tzinfo=dt.UTC)
    assert rows[0]["temperature_2m"] == 1.5
    assert rows[0]["extracted_at"] == EXTRACTED_AT


def test_nulls_and_integers_are_kept_as_nullable_floats(settings, weather, tmp_path):
    partition = write_bronze(tmp_path, weather, "sarajevo", overrides={"weather_code": [61, None]})

    table = pq.read_table(tr.transform_partition(tmp_path, weather, partition))

    assert table.schema.field("weather_code").type == pa.float64()
    assert table.column("weather_code").to_pylist() == [61.0, None]


def test_rerun_overwrites_the_same_file(settings, weather, tmp_path):
    partition = write_bronze(tmp_path, weather, "sarajevo")

    tr.transform_partition(tmp_path, weather, partition)
    tr.transform_partition(tmp_path, weather, partition)

    assert len(list((tmp_path / "silver").rglob("*.parquet"))) == 1
    assert not list((tmp_path / "silver").rglob("*.tmp"))


def test_empty_partition_is_an_error(settings, weather, tmp_path):
    partition = tmp_path / "bronze/weather/date=2026-11-13/hour=20"
    partition.mkdir(parents=True)

    with pytest.raises(FileNotFoundError):
        tr.transform_partition(tmp_path, weather, partition)


def test_run_defaults_to_latest_partition(settings, weather, tmp_path):
    write_bronze(tmp_path, weather, "sarajevo", date="2026-11-12", hour="23")
    write_bronze(tmp_path, weather, "sarajevo", date="2026-11-13", hour="02")

    written = tr.run(settings, dataset_names=("weather",))

    assert [p.parent.as_posix().split("silver/")[1] for p in written] == [
        "weather/date=2026-11-13/hour=02"
    ]


def test_run_all_and_specific_partition(settings, weather, tmp_path):
    write_bronze(tmp_path, weather, "sarajevo", date="2026-11-12", hour="23")
    write_bronze(tmp_path, weather, "sarajevo", date="2026-11-13", hour="02")

    assert len(tr.run(settings, dataset_names=("weather",), all_partitions=True)) == 2

    one = tr.run(settings, dataset_names=("weather",), date="2026-11-12", hour="23")
    assert one[0].parent.name == "hour=23"


def test_run_without_bronze_data_is_an_error(settings):
    with pytest.raises(FileNotFoundError):
        tr.run(settings, dataset_names=("weather",))
