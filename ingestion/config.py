"""Cities to collect and settings read from the environment (or a local .env file)."""

from dataclasses import dataclass
from pathlib import Path

from psycopg.conninfo import make_conninfo
from pydantic_settings import BaseSettings, SettingsConfigDict


@dataclass(frozen=True)
class City:
    slug: str  # used in file paths and as the key in the warehouse
    name: str
    country: str
    latitude: float
    longitude: float


CITIES: tuple[City, ...] = (
    City("sarajevo", "Sarajevo", "Bosnia and Herzegovina", 43.8563, 18.4131),
    City("berlin", "Berlin", "Germany", 52.5200, 13.4050),
    City("london", "London", "United Kingdom", 51.5074, -0.1278),
    City("paris", "Paris", "France", 48.8566, 2.3522),
    City("madrid", "Madrid", "Spain", 40.4168, -3.7038),
    City("rome", "Rome", "Italy", 41.9028, 12.4964),
    City("vienna", "Vienna", "Austria", 48.2082, 16.3738),
    City("istanbul", "Istanbul", "Turkey", 41.0082, 28.9784),
    City("delhi", "Delhi", "India", 28.6139, 77.2090),
    City("beijing", "Beijing", "China", 39.9042, 116.4074),
)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    weather_api_url: str = "https://api.open-meteo.com/v1/forecast"
    air_quality_api_url: str = "https://air-quality-api.open-meteo.com/v1/air-quality"
    data_dir: Path = Path("data")

    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "weather"
    postgres_user: str = "weather"
    postgres_password: str = ""

    def postgres_conninfo(self) -> str:
        """Connection string for psycopg, built from the POSTGRES_* settings."""
        return make_conninfo(
            host=self.postgres_host,
            port=self.postgres_port,
            dbname=self.postgres_db,
            user=self.postgres_user,
            password=self.postgres_password,
        )
