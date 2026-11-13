"""Extract hourly weather and air-quality readings from Open-Meteo into the bronze layer.

Each run writes one JSON file per city and dataset:

    <data_dir>/bronze/<dataset>/date=YYYY-MM-DD/hour=HH/city=<slug>.json

The path is derived from the run time (UTC), so re-running within the same hour
overwrites the same files instead of creating duplicates. Every file holds the API
response untouched, wrapped with the city, the request and the extraction time.

Each request asks for yesterday and today, so consecutive runs overlap on purpose:
a missed run loses nothing, and the warehouse load removes the duplicates. Today's
later hours are forecasts, which downstream models filter on `extracted_at`.

Usage:
    python -m ingestion.extract
    python -m ingestion.extract --city sarajevo --dataset weather
"""

import argparse
import datetime as dt
import json
import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests
from pydantic import BaseModel, model_validator
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from ingestion.config import CITIES, City, Settings

log = logging.getLogger(__name__)

REQUEST_TIMEOUT_SECONDS = 30


@dataclass(frozen=True)
class Dataset:
    name: str
    url: str
    variables: tuple[str, ...]


def build_datasets(settings: Settings) -> dict[str, Dataset]:
    return {
        "weather": Dataset(
            name="weather",
            url=settings.weather_api_url,
            variables=(
                "temperature_2m",
                "apparent_temperature",
                "relative_humidity_2m",
                "precipitation",
                "wind_speed_10m",
                "wind_direction_10m",
                "surface_pressure",
                "cloud_cover",
                "weather_code",
            ),
        ),
        "air_quality": Dataset(
            name="air_quality",
            url=settings.air_quality_api_url,
            variables=(
                "pm10",
                "pm2_5",
                "carbon_monoxide",
                "nitrogen_dioxide",
                "sulphur_dioxide",
                "ozone",
                "european_aqi",
                "us_aqi",
            ),
        ),
    }


class HourlyResponse(BaseModel):
    """The parts of an Open-Meteo response the pipeline relies on."""

    latitude: float
    longitude: float
    hourly: dict[str, list[Any]]

    @model_validator(mode="after")
    def _columns_line_up(self) -> "HourlyResponse":
        times = self.hourly.get("time")
        if not times:
            raise ValueError("hourly.time is missing or empty")
        for name, values in self.hourly.items():
            if len(values) != len(times):
                raise ValueError(f"hourly.{name} has {len(values)} values, expected {len(times)}")
        return self


def _is_retryable(exc: BaseException) -> bool:
    """Retry network problems, rate limiting and server errors; fail fast on the rest."""
    if isinstance(exc, requests.ConnectionError | requests.Timeout):
        return True
    if isinstance(exc, requests.HTTPError) and exc.response is not None:
        status = exc.response.status_code
        return status == 429 or status >= 500
    return False


@retry(
    retry=retry_if_exception(_is_retryable),
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=1, min=1, max=20),
    reraise=True,
)
def fetch(session: requests.Session, url: str, params: dict[str, Any]) -> dict[str, Any]:
    response = session.get(url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    return response.json()


def bronze_path(data_dir: Path, dataset: str, city: str, run_time: dt.datetime) -> Path:
    return (
        data_dir
        / "bronze"
        / dataset
        / f"date={run_time:%Y-%m-%d}"
        / f"hour={run_time:%H}"
        / f"city={city}.json"
    )


def extract(
    session: requests.Session,
    city: City,
    dataset: Dataset,
    data_dir: Path,
    run_time: dt.datetime,
) -> Path:
    """Fetch one dataset for one city and write it to the bronze layer."""
    params = {
        "latitude": city.latitude,
        "longitude": city.longitude,
        "hourly": ",".join(dataset.variables),
        "past_days": 1,
        "forecast_days": 1,
        "timezone": "GMT",
    }
    payload = fetch(session, dataset.url, params)

    validated = HourlyResponse.model_validate(payload)
    missing = set(dataset.variables) - validated.hourly.keys()
    if missing:
        raise ValueError(f"response is missing variables: {sorted(missing)}")

    record = {
        "city": city.slug,
        "dataset": dataset.name,
        "extracted_at": run_time.isoformat(),
        "request": {"url": dataset.url, "params": params},
        "response": payload,
    }
    path = bronze_path(data_dir, dataset.name, city.slug, run_time)
    path.parent.mkdir(parents=True, exist_ok=True)
    # Write to a temp file first so a crash never leaves a half-written file behind.
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(record), encoding="utf-8")
    tmp.replace(path)
    return path


def run(
    settings: Settings,
    cities: tuple[City, ...] = CITIES,
    dataset_names: tuple[str, ...] | None = None,
    run_time: dt.datetime | None = None,
) -> list[tuple[str, str, str]]:
    """Extract every city and dataset. Returns the failures as (city, dataset, error).

    One failing city does not stop the others.
    """
    run_time = run_time or dt.datetime.now(dt.UTC)
    datasets = build_datasets(settings)
    selected = [datasets[name] for name in (dataset_names or tuple(datasets))]

    failures: list[tuple[str, str, str]] = []
    with requests.Session() as session:
        for dataset in selected:
            for city in cities:
                try:
                    path = extract(session, city, dataset, settings.data_dir, run_time)
                    log.info("wrote %s", path.as_posix())
                except Exception as exc:  # noqa: BLE001 - report and carry on
                    log.error("failed %s/%s: %s", dataset.name, city.slug, exc)
                    failures.append((city.slug, dataset.name, str(exc)))
    return failures


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--city", choices=[c.slug for c in CITIES], help="only this city")
    parser.add_argument("--dataset", choices=["weather", "air_quality"], help="only this dataset")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    cities = tuple(c for c in CITIES if args.city in (None, c.slug))
    dataset_names = (args.dataset,) if args.dataset else None
    failures = run(Settings(), cities=cities, dataset_names=dataset_names)

    if failures:
        log.error("%d extraction(s) failed", len(failures))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
