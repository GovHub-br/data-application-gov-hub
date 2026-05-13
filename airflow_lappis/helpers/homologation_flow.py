import logging
from datetime import date

import polars as pl

from cliente_storage import get_bucket
from homologation_helpers import (
    CheckResult,
    check_no_duplicates,
    check_not_null,
    check_row_count,
    check_schema,
    run_checks,
)
from landing_zone import list_files, read_parquet


def load_landing_files(
    source: str,
    entity: str,
    run_date: date,
    bucket: str | None = None,
) -> tuple[list[str], pl.DataFrame]:
    """Read all Parquet files for a source/entity/date from the landing zone.

    Returns (file_paths, concatenated_dataframe).
    Raises ValueError if no files are found.
    """
    b = bucket or get_bucket()
    prefix = (
        f"{b}/{source}/{entity}/"
        f"{run_date.year}/{run_date.month:02d}/{run_date.day:02d}"
    )
    files = list_files(prefix)
    if not files:
        raise ValueError(f"No Parquet files found at: {prefix}")
    df = pl.concat([read_parquet(f) for f in files])
    logging.info(
        f"[homologation_flow] Loaded {len(df)} rows from {len(files)} file(s) "
        f"under {prefix}"
    )
    return files, df


def validate_dataframe(
    df: pl.DataFrame,
    expected_columns: list[str],
    not_null_columns: list[str],
    key_columns: list[str],
    min_rows: int = 1,
) -> tuple[bool, list[CheckResult]]:
    """Run standard quality checks and return (all_passed, results)."""
    checks = (
        check_schema(df, expected_columns)
        + check_row_count(df, min_rows)
        + check_not_null(df, not_null_columns)
        + check_no_duplicates(df, key_columns)
    )
    return run_checks(checks)


def write_to_postgres(
    df: pl.DataFrame,
    table_name: str,
    schema: str,
    conn_str: str,
) -> int:
    """Insert a DataFrame into Postgres. Returns the number of rows written."""
    from cliente_postgres import ClientPostgresDB

    db = ClientPostgresDB(conn_str)
    db.insert_data(
        df.to_pandas().to_dict(orient="records"),
        table_name=table_name,
        schema=schema,
    )
    logging.info(f"[homologation_flow] Wrote {len(df)} rows → {schema}.{table_name}")
    return len(df)
