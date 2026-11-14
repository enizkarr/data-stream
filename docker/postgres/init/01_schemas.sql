-- Runs once when the Postgres volume is first created.
-- raw: loaded by ingestion/load.py. The other schemas are built by dbt.
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS marts;
