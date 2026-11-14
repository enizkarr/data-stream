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
| Ingestion: silver → Postgres `raw` (upsert) | done |
| dbt models and tests | not started |
| Airflow DAG | not started |
| Docker Compose: Postgres + MinIO | done |
| Docker Compose: Airflow, Metabase | not started |
| CI (ruff, pytest, dbt build) | not started |
| Metabase dashboard | not started |

## Layout

```
ingestion/            Python: extract from the API, convert to Parquet, load into Postgres
docker/               init scripts mounted into the containers
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

Start Postgres and MinIO (Docker Desktop must be running):

```powershell
docker compose up -d
docker compose ps        # postgres and minio should show "healthy"
```

- Postgres: `localhost:5432`, database/user/password from `.env`. Schemas `raw`,
  `staging`, `intermediate` and `marts` are created on first start
  (`docker/postgres/init/`).
- MinIO console: <http://localhost:9001> (log in with `MINIO_ROOT_USER` /
  `MINIO_ROOT_PASSWORD`). The `minio-init` job creates the `LAKE_BUCKET` bucket and exits.
  MinIO no longer publishes Docker images, so this uses the `pgsty/minio` and `pgsty/mc`
  community builds of the same code.

Stop with `docker compose down`. Add `-v` to also delete the data volumes (a full reset;
init scripts run again on the next start).

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

Load silver Parquet into Postgres `raw.weather` and `raw.air_quality` (needs `docker compose up -d`):

```powershell
python -m ingestion.load                             # the latest silver partition
python -m ingestion.load --all                       # every silver partition
python -m ingestion.load --date 2026-11-13 --hour 20 # a specific one
```

Rows are keyed on `(city, time)`. When the same hour arrives again, the copy with the newer
`extracted_at` wins, so forecasts get replaced by later readings and re-running is safe.

Checks:

```powershell
ruff check .
pytest            # tests/test_load.py uses the Docker Postgres; skipped if it is not running
```
