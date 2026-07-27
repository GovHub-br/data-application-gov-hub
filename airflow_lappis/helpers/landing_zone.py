import logging
from datetime import date

import polars as pl

from cliente_storage import ensure_bucket_exists, get_bucket, get_storage_fs


def build_landing_path(
    source: str,
    entity: str,
    run_date: date,
    run_id: str,
    ext: str = "parquet",
) -> str:
    """Return the full bucket-qualified path for a landing zone file.

    Pattern: {bucket}/{source}/{entity}/{year}/{month}/{day}/{run_id}.{ext}
    """
    bucket = get_bucket()
    return (
        f"{bucket}/{source}/{entity}/"
        f"{run_date.year}/{run_date.month:02d}/{run_date.day:02d}/"
        f"{run_id}.{ext}"
    )


def write_parquet(df: pl.DataFrame, path: str) -> str:
    """Write a Polars DataFrame as Parquet to the landing zone. Returns path."""
    fs = get_storage_fs()
    ensure_bucket_exists(fs, get_bucket())
    with fs.open(path, "wb") as f:
        df.write_parquet(f)
    logging.info(f"[landing_zone] Wrote {len(df)} rows → {path}")
    return path


def read_parquet(path: str) -> pl.DataFrame:
    """Read a Parquet file from the landing zone into a Polars DataFrame."""
    fs = get_storage_fs()
    with fs.open(path, "rb") as f:
        df = pl.read_parquet(f)
    logging.info(f"[landing_zone] Read {len(df)} rows ← {path}")
    return df


def list_files(prefix: str, ext: str = "parquet") -> list[str]:
    """List all files under a landing zone prefix with the given extension."""
    fs = get_storage_fs()
    try:
        return fs.glob(f"{prefix}/**/*.{ext}")
    except FileNotFoundError:
        return []
