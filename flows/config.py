"""Configuration and credentials for the NYC Taxi flows."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass

from prefect.blocks.system import Secret

DEFAULT_TAXI_TYPE = "all"
TAXI_TYPES = ("yellow", "green")

DEFAULT_SOURCE_BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data"
DEFAULT_B2_BUCKET = "zoomcamp"
DEFAULT_MOTHERDUCK_DATABASE = "nyc_taxi"
DEFAULT_MOTHERDUCK_SCHEMA = "zoomcamp"

SCHEMA_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

SECRET_BLOCKS = {
    "B2_KEY_ID": "b2-key-id",
    "B2_APP_KEY": "b2-app-key",
    "MOTHERDUCK_TOKEN": "motherduck-token",
}


@dataclass(frozen=True)
class Settings:
    b2_endpoint: str
    b2_region: str
    b2_key_id: str
    b2_app_key: str
    b2_bucket: str
    motherduck_database: str
    motherduck_token: str
    motherduck_schema: str


def _env_or_secret(name: str, default: str | None = None) -> str:
    value = os.environ.get(name)
    if value:
        return value
    if default is not None:
        return default

    block_name = SECRET_BLOCKS.get(name)
    if block_name is None:
        raise ValueError(f"Environment variable {name} belum diset.")

    try:
        return Secret.load(block_name).get()
    except ValueError as exc:
        raise ValueError(
            f"Set environment variable {name} atau buat Prefect Secret block {block_name!r}."
        ) from exc


def get_settings() -> Settings:
    schema = os.environ.get("MOTHERDUCK_SCHEMA", DEFAULT_MOTHERDUCK_SCHEMA)
    if not SCHEMA_PATTERN.fullmatch(schema):
        raise ValueError(f"MOTHERDUCK_SCHEMA tidak valid: {schema!r}.")

    return Settings(
        b2_endpoint=_env_or_secret("B2_ENDPOINT").rstrip("/"),
        b2_region=_env_or_secret("B2_REGION"),
        b2_key_id=_env_or_secret("B2_KEY_ID"),
        b2_app_key=_env_or_secret("B2_APP_KEY"),
        b2_bucket=_env_or_secret("B2_BUCKET", DEFAULT_B2_BUCKET),
        motherduck_database=_env_or_secret("MOTHERDUCK_DATABASE", DEFAULT_MOTHERDUCK_DATABASE),
        motherduck_token=_env_or_secret("MOTHERDUCK_TOKEN"),
        motherduck_schema=schema,
    )
