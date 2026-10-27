"""Settings, read from environment variables.

dev  = local RustFS (S3) + a local DuckDB file. Sensible defaults, nothing to set.
prod = real S3 storage + MotherDuck. Keys come from env vars or Prefect Secret blocks.
"""

import os
from dataclasses import dataclass, field

from prefect.blocks.system import Secret

DEV_DEFAULTS = {
    "S3_ENDPOINT_URL": "http://127.0.0.1:9000",
    "S3_REGION": "us-east-1",
    "S3_BUCKET": "nyc-taxi",
    "WAREHOUSE_DATABASE": "./data/nyc_taxi.duckdb",
    "WAREHOUSE_SCHEMA": "nyc_taxi",
}

# If the env var is empty, look for a Prefect Secret block with this name.
SECRET_BLOCKS = {
    "S3_ACCESS_KEY_ID": "s3-access-key-id",
    "S3_SECRET_ACCESS_KEY": "s3-secret-access-key",
    "MOTHERDUCK_TOKEN": "motherduck-token",
}


@dataclass(frozen=True)
class Settings:
    env: str
    s3_endpoint_url: str | None
    s3_region: str
    s3_bucket: str
    warehouse_database: str  # file path (DuckDB) or md:<name> (MotherDuck)
    warehouse_schema: str
    # repr=False keeps secrets out of logs when Settings is printed.
    s3_access_key_id: str = field(repr=False)
    s3_secret_access_key: str = field(repr=False)
    motherduck_token: str | None = field(default=None, repr=False)

    @property
    def is_dev(self) -> bool:
        return self.env == "dev"


def read_setting(name: str, env: str, required: bool = True) -> str | None:
    """Env var -> Prefect Secret block -> dev default."""
    value = os.environ.get(name, "").strip()
    if value:
        return value
    if name in SECRET_BLOCKS:
        try:
            return Secret.load(SECRET_BLOCKS[name]).get()
        except ValueError:
            pass  # block does not exist
    if env == "dev" and name in DEV_DEFAULTS:
        return DEV_DEFAULTS[name]
    if required:
        raise ValueError(f"Set environment variable {name}.")
    return None


def get_settings() -> Settings:
    env = os.environ.get("ENV", "dev").strip().lower()
    if env not in ("dev", "prod"):
        raise ValueError(f"ENV harus 'dev' atau 'prod', dapat: {env!r}.")

    database = read_setting("WAREHOUSE_DATABASE", env)
    return Settings(
        env=env,
        s3_endpoint_url=read_setting("S3_ENDPOINT_URL", env, required=False),
        s3_region=read_setting("S3_REGION", env),
        s3_bucket=read_setting("S3_BUCKET", env),
        warehouse_database=database,
        warehouse_schema=read_setting("WAREHOUSE_SCHEMA", env),
        s3_access_key_id=read_setting("S3_ACCESS_KEY_ID", env),
        s3_secret_access_key=read_setting("S3_SECRET_ACCESS_KEY", env),
        motherduck_token=read_setting("MOTHERDUCK_TOKEN", env)
        if database.startswith("md:")
        else None,
    )
