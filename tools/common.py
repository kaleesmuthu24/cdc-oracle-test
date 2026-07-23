from __future__ import annotations

import os
import time
from contextlib import contextmanager
from typing import Iterator

import oracledb
import psycopg
import requests
from dotenv import load_dotenv

load_dotenv()


def env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value else default


@contextmanager
def oracle_connection() -> Iterator[oracledb.Connection]:
    connection = oracledb.connect(
        user=env("RETAIL_USER", "retail"),
        password=env("RETAIL_PASSWORD", "RetailPwd123"),
        dsn=env("ORACLE_DSN", "localhost:1521/FREEPDB1"),
    )
    try:
        yield connection
    finally:
        connection.close()


@contextmanager
def postgres_connection() -> Iterator[psycopg.Connection]:
    connection = psycopg.connect(
        host=env("POSTGRES_HOST", "localhost"),
        port=int(env("POSTGRES_PORT", "5432")),
        dbname=env("POSTGRES_DB", "cdc"),
        user=env("POSTGRES_USER", "cdc"),
        password=env("POSTGRES_PASSWORD", "PostgresPwd123"),
    )
    try:
        yield connection
    finally:
        connection.close()


def sink_metrics() -> dict:
    response = requests.get(env("SINK_METRICS_URL", "http://localhost:18080/metrics"), timeout=10)
    response.raise_for_status()
    return response.json()


def wait_http(url: str, timeout_seconds: int = 300) -> None:
    deadline = time.time() + timeout_seconds
    last_error: Exception | None = None
    while time.time() < deadline:
        try:
            response = requests.get(url, timeout=5)
            if response.ok:
                return
        except Exception as exc:  # noqa: BLE001
            last_error = exc
        time.sleep(2)
    raise TimeoutError(f"Timed out waiting for {url}: {last_error}")
