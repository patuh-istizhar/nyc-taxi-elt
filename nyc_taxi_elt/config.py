"""Settings: S3-compatible storage + a DuckDB warehouse, run by Prefect.

``ENV`` only picks defaults and safety rails:

* **dev**  - local S3 endpoint (RustFS via docker compose) and a local warehouse
  file; the bucket is created automatically.
* **prod** - every S3 / warehouse setting must be given explicitly and a missing
  bucket fails the run.

``WAREHOUSE_DATABASE`` is anything ``duckdb.connect`` accepts: a file path for a
local DuckDB file or ``md:<name>`` for MotherDuck (then ``MOTHERDUCK_TOKEN`` is needed).
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from typing import Literal

from prefect.blocks.system import Secret

Env = Literal["dev", "prod"]

DEFAULT_ENV: Env = "dev"
DEFAULT_WAREHOUSE_SCHEMA = "nyc_taxi"
DEV_DEFAULTS = {
    "S3_ENDPOINT_URL": "http://127.0.0.1:9000",
    "S3_REGION": "us-east-1",
    "S3_BUCKET": "nyc-taxi",
    "WAREHOUSE_DATABASE": "./data/nyc_taxi.duckdb",
}

IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# Prefect Secret blocks used when the matching environment variable is not set.
SECRET_BLOCKS: dict[str, str] = {
    "S3_ACCESS_KEY_ID": "s3-access-key-id",
    "S3_SECRET_ACCESS_KEY": "s3-secret-access-key",
    "MOTHERDUCK_TOKEN": "motherduck-token",
}


@dataclass(frozen=True)
class Settings:
    env: Env
    s3_endpoint_url: str | None  # None = AWS S3
    s3_region: str
    s3_access_key_id: str = field(repr=False)
    s3_secret_access_key: str = field(repr=False)
    s3_bucket: str
    warehouse_database: str
    warehouse_schema: str
    motherduck_token: str | None = field(default=None, repr=False)

    @property
    def is_dev(self) -> bool:
        return self.env == "dev"

    @property
    def is_motherduck(self) -> bool:
        return self.warehouse_database.startswith("md:")

    def s3_uri(self, key: str = "") -> str:
        return f"s3://{self.s3_bucket}/{key}"


def _setting(name: str, env: Env, *, required: bool = True) -> str | None:
    """Environment variable -> Prefect Secret block -> dev default."""
    value = os.environ.get(name, "").strip()
    if value:
        return value

    block = SECRET_BLOCKS.get(name)
    if block:
        try:
            return Secret.load(block).get()
        except ValueError:
            pass

    if env == "dev" and name in DEV_DEFAULTS:
        return DEV_DEFAULTS[name]
    if not required:
        return None

    hint = f" atau buat Prefect Secret block {block!r}" if block else ""
    raise ValueError(f"Set environment variable {name}{hint}.")


def _required(name: str, env: Env) -> str:
    value = _setting(name, env)
    assert value is not None
    return value


def get_settings() -> Settings:
    """Read settings once per flow run and pass them to tasks.

    Secret block lookups hit the Prefect API, so avoid calling this per task.
    """
    raw_env = os.environ.get("ENV", DEFAULT_ENV).strip().lower()
    if raw_env not in {"dev", "prod"}:
        raise ValueError(f"ENV harus 'dev' atau 'prod', dapat: {raw_env!r}.")
    env: Env = raw_env  # type: ignore[assignment]

    schema = os.environ.get("WAREHOUSE_SCHEMA", "").strip() or DEFAULT_WAREHOUSE_SCHEMA
    if not IDENTIFIER_PATTERN.fullmatch(schema):
        raise ValueError(f"WAREHOUSE_SCHEMA tidak valid: {schema!r}.")

    endpoint = _setting("S3_ENDPOINT_URL", env, required=False)
    database = _required("WAREHOUSE_DATABASE", env)
    token = _required("MOTHERDUCK_TOKEN", env) if database.startswith("md:") else None

    return Settings(
        env=env,
        s3_endpoint_url=endpoint.rstrip("/") if endpoint else None,
        s3_region=_required("S3_REGION", env),
        s3_access_key_id=_required("S3_ACCESS_KEY_ID", env),
        s3_secret_access_key=_required("S3_SECRET_ACCESS_KEY", env),
        s3_bucket=_required("S3_BUCKET", env),
        warehouse_database=database,
        warehouse_schema=schema,
        motherduck_token=token,
    )
