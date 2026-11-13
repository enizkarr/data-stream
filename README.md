# City Weather & Air Quality Pipeline

Collects hourly weather and air-quality readings for about 10 cities from
[Open-Meteo](https://open-meteo.com/), stores them in a data lake, loads them into a
warehouse, models and tests them with dbt, and serves the results on a dashboard.

```
Open-Meteo API
     │  ingest (Python)
     ▼
MinIO (S3)        bronze/ raw JSON  →  silver/ Parquet
     │  load
     ▼
Postgres          raw  →  staging  →  intermediate  →  marts   (dbt)
     │
     ▼
Metabase dashboard

Airflow schedules the whole flow hourly. Everything runs in Docker Compose.
```

## Status

| Stage | State |
|---|---|
| Repo skeleton | done |
| Ingestion: extract to bronze (local files) | done |
| Ingestion: bronze → silver Parquet (local files) | done |
| Ingestion: load into Postgres | not started |
| dbt models and tests | not started |
| Airflow DAG | not started |
| Docker Compose | not started |
| CI (ruff, pytest, dbt build) | not started |
| Metabase dashboard | not started |

## Layout

```
ingestion/            Python: extract from the API, convert to Parquet, load into Postgres
dbt_project/          dbt models (staging, intermediate, marts) and tests
orchestration/dags/   Airflow DAGs
tests/                pytest for the ingestion code
.github/workflows/    CI
```

## Local setup

Requires Python 3.12 and Docker Desktop.

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -e ".[dev]"
Copy-Item .env.example .env   # then change the passwords
```

Extract the latest readings for every city into `data/bronze/`:

```powershell
python -m ingestion.extract
python -m ingestion.extract --city sarajevo --dataset weather   # just one
```

Convert bronze JSON into silver Parquet in `data/silver/` (one row per city per hour):

```powershell
python -m ingestion.transform                             # the latest extraction
python -m ingestion.transform --all                       # every extraction
python -m ingestion.transform --date 2026-11-13 --hour 20 # a specific one
```

Checks:

```powershell
ruff check .
pytest
```
