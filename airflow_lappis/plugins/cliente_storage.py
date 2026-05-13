import logging
import os

import fsspec


def get_storage_fs() -> fsspec.AbstractFileSystem:
    """Return an fsspec filesystem for the configured storage backend.

    Controlled by the STORAGE_BACKEND env var: minio | s3 | adls.
    MinIO uses the S3 protocol with a custom endpoint, so no separate
    code path is needed for on-prem vs cloud S3.
    """
    backend = os.getenv("STORAGE_BACKEND", "minio").lower()
    logging.info(f"[cliente_storage] Using storage backend: {backend}")

    if backend in ("minio", "s3"):
        import s3fs

        kwargs: dict = {
            "key": os.getenv("MINIO_ACCESS_KEY", os.getenv("AWS_ACCESS_KEY_ID")),
            "secret": os.getenv("MINIO_SECRET_KEY", os.getenv("AWS_SECRET_ACCESS_KEY")),
        }
        if backend == "minio":
            endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
            kwargs["client_kwargs"] = {"endpoint_url": f"http://{endpoint}"}
        return s3fs.S3FileSystem(**kwargs)

    if backend == "adls":
        import adlfs

        return adlfs.AzureBlobFileSystem(
            account_name=os.getenv("ADLS_ACCOUNT_NAME", ""),
            account_key=os.getenv("ADLS_ACCOUNT_KEY", ""),
        )

    raise ValueError(f"Unknown STORAGE_BACKEND '{backend}'. Expected: minio, s3, adls.")


def get_bucket() -> str:
    return os.getenv("MINIO_BUCKET", "data-lake-ipea")


def ensure_bucket_exists(fs: fsspec.AbstractFileSystem, bucket: str) -> None:
    """Create the bucket if it does not exist. No-op for ADLS containers."""
    try:
        if not fs.exists(bucket):
            fs.mkdir(bucket)
            logging.info(f"[cliente_storage] Created bucket: {bucket}")
    except Exception as exc:
        logging.warning(f"[cliente_storage] Could not verify/create bucket: {exc}")
