"""NYC TLC trip-record source: taxi types, file names, URLs and table layouts."""

from __future__ import annotations

import os
from datetime import date
from typing import Literal, TypedDict

TaxiType = Literal["all", "yellow", "green"]
ALL_TAXI_TYPES = "all"
TAXI_TYPES = ("yellow", "green")
DEFAULT_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

# Columns that make up the row id; a file without them cannot be loaded.
REQUIRED_COLUMNS = ("VendorID", "PULocationID", "DOLocationID")


class TableSpec(TypedDict):
    pickup_column: str
    dropoff_column: str
    columns: list[tuple[str, str]]


class TripFile(TypedDict):
    filename: str
    taxi_type: str
    trip_month: str
    source_uri: str


TRIP_TABLES: dict[str, TableSpec] = {
    "yellow": {
        "pickup_column": "tpep_pickup_datetime",
        "dropoff_column": "tpep_dropoff_datetime",
        "columns": [
            ("VendorID", "VARCHAR"),
            ("tpep_pickup_datetime", "TIMESTAMP"),
            ("tpep_dropoff_datetime", "TIMESTAMP"),
            ("passenger_count", "BIGINT"),
            ("trip_distance", "DOUBLE"),
            ("RatecodeID", "VARCHAR"),
            ("store_and_fwd_flag", "VARCHAR"),
            ("PULocationID", "VARCHAR"),
            ("DOLocationID", "VARCHAR"),
            ("payment_type", "BIGINT"),
            ("fare_amount", "DOUBLE"),
            ("extra", "DOUBLE"),
            ("mta_tax", "DOUBLE"),
            ("tip_amount", "DOUBLE"),
            ("tolls_amount", "DOUBLE"),
            ("improvement_surcharge", "DOUBLE"),
            ("total_amount", "DOUBLE"),
            ("congestion_surcharge", "DOUBLE"),
            ("airport_fee", "DOUBLE"),
        ],
    },
    "green": {
        "pickup_column": "lpep_pickup_datetime",
        "dropoff_column": "lpep_dropoff_datetime",
        "columns": [
            ("VendorID", "VARCHAR"),
            ("lpep_pickup_datetime", "TIMESTAMP"),
            ("lpep_dropoff_datetime", "TIMESTAMP"),
            ("store_and_fwd_flag", "VARCHAR"),
            ("RatecodeID", "VARCHAR"),
            ("PULocationID", "VARCHAR"),
            ("DOLocationID", "VARCHAR"),
            ("passenger_count", "BIGINT"),
            ("trip_distance", "DOUBLE"),
            ("fare_amount", "DOUBLE"),
            ("extra", "DOUBLE"),
            ("mta_tax", "DOUBLE"),
            ("tip_amount", "DOUBLE"),
            ("tolls_amount", "DOUBLE"),
            ("ehail_fee", "DOUBLE"),
            ("improvement_surcharge", "DOUBLE"),
            ("total_amount", "DOUBLE"),
            ("payment_type", "BIGINT"),
            ("trip_type", "BIGINT"),
            ("congestion_surcharge", "DOUBLE"),
        ],
    },
}


def table_name(taxi_type: str) -> str:
    return f"{taxi_type}_tripdata"


def file_name(taxi_type: str, month: date) -> str:
    return f"{table_name(taxi_type)}_{month:%Y-%m}.parquet"


def base_url() -> str:
    return os.environ.get("TLC_BASE_URL", DEFAULT_BASE_URL).rstrip("/")


def resolve_taxi_types(taxi_type: str | None) -> list[str]:
    value = str(taxi_type or ALL_TAXI_TYPES).strip().lower()
    if value == ALL_TAXI_TYPES:
        return list(TAXI_TYPES)
    if value not in TAXI_TYPES:
        raise ValueError(
            f"taxi_type tidak valid: {value!r}. Pilih salah satu: all, {', '.join(TAXI_TYPES)}."
        )
    return [value]


def trip_files(taxi_types: list[str], month: date) -> list[TripFile]:
    root = base_url()
    return [
        {
            "filename": file_name(taxi_type, month),
            "taxi_type": taxi_type,
            "trip_month": f"{month:%Y-%m}",
            "source_uri": f"{root}/{file_name(taxi_type, month)}",
        }
        for taxi_type in taxi_types
    ]
