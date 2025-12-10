"""Configuration management using pydantic-settings."""

from functools import lru_cache
from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    databricks_host: str
    databricks_token: str
    databricks_warehouse_id: str

    databricks_catalog: str = "workspace"
    databricks_schema: str = "f1"

    query_timeout_seconds: int = 120
    max_result_rows: int = 10000

    @property
    def default_catalog_schema(self) -> str:
        return f"{self.databricks_catalog}.{self.databricks_schema}"

    def get_full_table_name(self, table_name: str) -> str:
        if "." in table_name:
            return table_name
        return f"{self.default_catalog_schema}.{table_name}"


@lru_cache
def get_settings() -> Settings:
    return Settings()

