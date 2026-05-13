"""Integration tests for the landing zone → Postgres flow.

Requires the docker-compose stack (MinIO + Postgres) to be running:

    docker-compose up -d minio minio-init postgres

Run with:

    pytest tests/integration/ -m integration -v

Environment (matches local.env defaults):
    STORAGE_BACKEND=minio
    MINIO_ENDPOINT=localhost:9000
    MINIO_ACCESS_KEY=minioadmin
    MINIO_SECRET_KEY=minioadmin
    MINIO_BUCKET=data-lake-ipea
    POSTGRES_HOST=localhost
    POSTGRES_PORT=5432
    POSTGRES_USER=postgres
    POSTGRES_PASSWORD=postgres
    POSTGRES_DB=postgres
"""

import os
import sys
import uuid
from datetime import date, datetime
from typing import Generator

import polars as pl
import psycopg2
import pytest

# Add helpers and plugins to path so imports work outside Airflow.
_REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(_REPO, "airflow_lappis", "plugins"))
sys.path.insert(0, os.path.join(_REPO, "airflow_lappis", "helpers"))

os.environ.setdefault("STORAGE_BACKEND", "minio")
os.environ.setdefault("MINIO_ENDPOINT", "localhost:9000")
os.environ.setdefault("MINIO_ACCESS_KEY", "minioadmin")
os.environ.setdefault("MINIO_SECRET_KEY", "minioadmin")
os.environ.setdefault("MINIO_BUCKET", "data-lake-ipea")
os.environ.setdefault("POSTGRES_HOST", "localhost")
os.environ.setdefault("POSTGRES_PORT", "5432")
os.environ.setdefault("POSTGRES_USER", "postgres")
os.environ.setdefault("POSTGRES_PASSWORD", "postgres")
os.environ.setdefault("POSTGRES_DB", "postgres")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _pg_conn_str() -> str:
    return (
        f"dbname={os.environ['POSTGRES_DB']} "
        f"user={os.environ['POSTGRES_USER']} "
        f"password={os.environ['POSTGRES_PASSWORD']} "
        f"host={os.environ['POSTGRES_HOST']} "
        f"port={os.environ['POSTGRES_PORT']}"
    )


_CATEGORIES = ["A", "B", "C", "D"]


def _make_dataframe(n: int) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "id": list(range(1, n + 1)),
            "name": [f"NAME_{i:04d}" for i in range(1, n + 1)],
            "value": [float(i * 100) for i in range(1, n + 1)],
            "category": [_CATEGORIES[i % len(_CATEGORIES)] for i in range(n)],
            "active": [i % 2 == 0 for i in range(n)],
            "dt_ingest": [datetime.now().isoformat()] * n,
        }
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def bucket_ready() -> str:
    """Ensure the landing zone bucket exists; return its name."""
    from cliente_storage import ensure_bucket_exists, get_bucket, get_storage_fs

    fs = get_storage_fs()
    bucket = get_bucket()
    ensure_bucket_exists(fs, bucket)
    return bucket


@pytest.fixture(scope="session")
def pg() -> Generator[psycopg2.extensions.connection, None, None]:
    """Session-scoped Postgres connection."""
    conn = psycopg2.connect(_pg_conn_str())
    conn.autocommit = False
    yield conn
    conn.close()


@pytest.fixture
def tmp_pg_table(
    pg: psycopg2.extensions.connection,
) -> Generator[tuple[str, str], None, None]:
    """Provide a unique (schema, table) and drop it after the test."""
    schema = "test_integration"
    table = f"flow_{uuid.uuid4().hex[:8]}"
    with pg.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    pg.commit()
    yield schema, table
    with pg.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table}")
    pg.commit()


# ---------------------------------------------------------------------------
# Landing zone tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_write_and_read_parquet_roundtrip(bucket_ready: str) -> None:
    from landing_zone import build_landing_path, read_parquet, write_parquet

    df = pl.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["Alpha", "Beta", "Gamma"],
            "value": [10.5, 20.0, 30.75],
            "dt_ingest": [datetime.now().isoformat()] * 3,
        }
    )
    run_id = str(uuid.uuid4())[:8]
    path = build_landing_path("test_source", "roundtrip", date.today(), run_id)

    write_parquet(df, path)
    result = read_parquet(path)

    assert result.shape == df.shape
    assert result.columns == df.columns
    assert result["id"].to_list() == df["id"].to_list()


@pytest.mark.integration
def test_list_files_finds_written_file(bucket_ready: str) -> None:
    from landing_zone import build_landing_path, list_files, write_parquet

    run_date = date.today()
    run_id = str(uuid.uuid4())[:8]
    df = pl.DataFrame({"id": [1], "val": [99.0]})
    path = build_landing_path("test_source", "listing", run_date, run_id)
    write_parquet(df, path)

    bucket = bucket_ready
    prefix = (
        f"{bucket}/test_source/listing/"
        f"{run_date.year}/{run_date.month:02d}/{run_date.day:02d}"
    )
    files = list_files(prefix)
    assert any(run_id in f for f in files)


# ---------------------------------------------------------------------------
# Full homologation flow test
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_full_homologation_flow(
    tmp_pg_table: tuple[str, str],
    pg: psycopg2.extensions.connection,
) -> None:
    """End-to-end: generate N rows → MinIO → quality checks → Postgres → count matches."""
    from homologation_flow import (  # noqa: PLC0415
        load_landing_files,
        validate_dataframe,
        write_to_postgres,
    )
    from landing_zone import build_landing_path, write_parquet

    N = 75
    # Use a unique source so list_files only finds files from this test run.
    run_id = str(uuid.uuid4())[:8]
    source = f"integration_{run_id}"
    entity = "entities"
    run_date = date.today()
    schema, table = tmp_pg_table

    # 1. Generate random data and write to MinIO.
    df_in = _make_dataframe(N)
    path = build_landing_path(source, entity, run_date, run_id)
    write_parquet(df_in, path)

    # 2. Read back from the landing zone via the helper.
    files, df_out = load_landing_files(source, entity, run_date)
    assert len(files) == 1
    assert len(df_out) == N

    # 3. Validate data quality.
    expected_cols = ["id", "name", "value", "category", "active", "dt_ingest"]
    passed, results = validate_dataframe(
        df_out,
        expected_columns=expected_cols,
        not_null_columns=["id", "dt_ingest"],
        key_columns=["id"],
    )
    failed = [r for r in results if not r.passed]
    assert passed, f"Quality checks failed: {[r.check_name for r in failed]}"

    # 4. Write to Postgres.
    rows_written = write_to_postgres(df_out, table, schema, _pg_conn_str())
    assert rows_written == N

    # 5. Query Postgres and confirm the count matches what was generated.
    with pg.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
        pg_count = cur.fetchone()[0]

    assert (
        pg_count == N
    ), f"Postgres row count {pg_count} does not match generated rows {N}"
