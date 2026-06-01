import logging
import os

import fsspec


def get_storage_fs() -> fsspec.AbstractFileSystem:
    """Return an fsspec filesystem for the configured storage backend.

    Controlled by the STORAGE_BACKEND env var: s3 | adls.
    MinIO uses the S3 protocol with S3_ENDPOINT pointed at MinIO.
    """
    backend = os.getenv("STORAGE_BACKEND", os.getenv("OBJECT_STORAGE", "s3")).lower()
    logging.info(f"[cliente_storage] Using storage backend: {backend}")

    if backend == "s3":
        import s3fs

        kwargs: dict = {
            "key": os.getenv("AWS_ACCESS_KEY_ID", os.getenv("MINIO_ACCESS_KEY")),
            "secret": os.getenv("AWS_SECRET_ACCESS_KEY", os.getenv("MINIO_SECRET_KEY")),
        }
        endpoint = os.getenv("S3_ENDPOINT", os.getenv("MINIO_ENDPOINT", ""))
        if endpoint:
            if "://" not in endpoint:
                endpoint = f"https://{endpoint}"
            kwargs["client_kwargs"] = {"endpoint_url": endpoint}
        return s3fs.S3FileSystem(**kwargs)

    if backend == "adls":
        import adlfs

        return adlfs.AzureBlobFileSystem(
            account_name=os.getenv("AZURE_STORAGE_ACCOUNT", os.getenv("ADLS_ACCOUNT_NAME", "")),
            account_key=os.getenv("AZURE_STORAGE_KEY", os.getenv("ADLS_ACCOUNT_KEY", "")),
        )

    raise ValueError(f"Unknown STORAGE_BACKEND '{backend}'. Expected: s3, adls.")


def get_bucket() -> str:
    return os.getenv(
        "RAW_STORAGE_CONTAINER",
        os.getenv("DATA_BUCKET", os.getenv("MINIO_BUCKET", "data-lake-ipea")),
    )


def ensure_bucket_exists(fs: fsspec.AbstractFileSystem, bucket: str) -> None:
    """Create the bucket if it does not exist. No-op for ADLS containers."""
    try:
        if not fs.exists(bucket):
            fs.mkdir(bucket)
            logging.info(f"[cliente_storage] Created bucket: {bucket}")
    except Exception as exc:
        logging.warning(f"[cliente_storage] Could not verify/create bucket: {exc}")
