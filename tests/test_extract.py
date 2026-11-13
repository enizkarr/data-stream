import datetime as dt
import json

import pytest
import requests
import responses
from tenacity import wait_none

from ingestion import extract as ex
from ingestion.config import CITIES, Settings

RUN_TIME = dt.datetime(2026, 11, 13, 14, 5, tzinfo=dt.UTC)
SARAJEVO = CITIES[0]


@pytest.fixture(autouse=True)
def no_retry_wait(monkeypatch):
    monkeypatch.setattr(ex.fetch.retry, "wait", wait_none())


@pytest.fixture
def settings(tmp_path):
    return Settings(_env_file=None, data_dir=tmp_path)


def payload(dataset: ex.Dataset, hours: int = 3) -> dict:
    hourly = {"time": [f"2026-11-13T{h:02d}:00" for h in range(hours)]}
    for variable in dataset.variables:
        hourly[variable] = [1.0] * hours
    return {"latitude": 43.86, "longitude": 18.41, "hourly": hourly}


@responses.activate
def test_extract_writes_partitioned_bronze_file(settings, tmp_path):
    weather = ex.build_datasets(settings)["weather"]
    body = payload(weather)
    responses.get(weather.url, json=body)

    with requests.Session() as session:
        path = ex.extract(session, SARAJEVO, weather, tmp_path, RUN_TIME)

    expected = tmp_path / "bronze/weather/date=2026-11-13/hour=14/city=sarajevo.json"
    assert path == expected
    record = json.loads(path.read_text(encoding="utf-8"))
    assert record["city"] == "sarajevo"
    assert record["dataset"] == "weather"
    assert record["extracted_at"] == RUN_TIME.isoformat()
    assert record["response"] == body  # raw response is stored unchanged
    assert "temperature_2m" in responses.calls[0].request.url


@responses.activate
def test_rerun_in_same_hour_overwrites_instead_of_duplicating(settings, tmp_path):
    weather = ex.build_datasets(settings)["weather"]
    responses.get(weather.url, json=payload(weather))

    with requests.Session() as session:
        ex.extract(session, SARAJEVO, weather, tmp_path, RUN_TIME)
        ex.extract(session, SARAJEVO, weather, tmp_path, RUN_TIME)

    assert len(list(tmp_path.rglob("*.json"))) == 1
    assert not list(tmp_path.rglob("*.tmp"))


@responses.activate
def test_fetch_retries_server_errors(settings):
    weather = ex.build_datasets(settings)["weather"]
    responses.get(weather.url, status=503)
    responses.get(weather.url, status=429)
    responses.get(weather.url, json={"ok": True})

    with requests.Session() as session:
        assert ex.fetch(session, weather.url, {}) == {"ok": True}
    assert len(responses.calls) == 3


@responses.activate
def test_fetch_does_not_retry_client_errors(settings):
    weather = ex.build_datasets(settings)["weather"]
    responses.get(weather.url, status=400)

    with requests.Session() as session, pytest.raises(requests.HTTPError):
        ex.fetch(session, weather.url, {})
    assert len(responses.calls) == 1


@responses.activate
def test_extract_rejects_misaligned_columns(settings, tmp_path):
    weather = ex.build_datasets(settings)["weather"]
    body = payload(weather)
    body["hourly"]["temperature_2m"] = [1.0]
    responses.get(weather.url, json=body)

    with requests.Session() as session, pytest.raises(ValueError, match="temperature_2m"):
        ex.extract(session, SARAJEVO, weather, tmp_path, RUN_TIME)
    assert not list(tmp_path.rglob("*.json"))


@responses.activate
def test_extract_rejects_missing_variable(settings, tmp_path):
    weather = ex.build_datasets(settings)["weather"]
    body = payload(weather)
    del body["hourly"]["precipitation"]
    responses.get(weather.url, json=body)

    with requests.Session() as session, pytest.raises(ValueError, match="precipitation"):
        ex.extract(session, SARAJEVO, weather, tmp_path, RUN_TIME)


@responses.activate
def test_run_continues_after_one_city_fails(settings, tmp_path):
    datasets = ex.build_datasets(settings)
    weather = datasets["weather"]
    responses.get(weather.url, status=400)  # first city fails
    responses.get(weather.url, json=payload(weather))  # second city succeeds

    failures = ex.run(settings, cities=CITIES[:2], dataset_names=("weather",), run_time=RUN_TIME)

    assert [(city, dataset) for city, dataset, _ in failures] == [("sarajevo", "weather")]
    written = list(tmp_path.rglob("*.json"))
    assert [p.name for p in written] == ["city=berlin.json"]
