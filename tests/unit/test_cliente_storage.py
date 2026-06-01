import os
import sys
from unittest.mock import MagicMock, patch

import pytest


# conftest.py already adds plugins to sys.path and stubs s3fs/adlfs/fsspec.
import cliente_storage


class TestGetBucket:
    def test_returns_env_var(self) -> None:
        with patch.dict(os.environ, {"RAW_STORAGE_CONTAINER": "my-bucket"}):
            assert cliente_storage.get_bucket() == "my-bucket"

    def test_default_when_env_not_set(self) -> None:
        env = {
            k: v
            for k, v in os.environ.items()
            if k not in ("RAW_STORAGE_CONTAINER", "DATA_BUCKET", "MINIO_BUCKET")
        }
        with patch.dict(os.environ, env, clear=True):
            assert cliente_storage.get_bucket() == "data-lake-ipea"


class TestGetStorageFs:
    def _make_s3_mock(self) -> tuple[MagicMock, MagicMock]:
        mock_module = MagicMock()
        mock_instance = MagicMock()
        mock_module.S3FileSystem.return_value = mock_instance
        return mock_module, mock_instance

    def _make_adls_mock(self) -> tuple[MagicMock, MagicMock]:
        mock_module = MagicMock()
        mock_instance = MagicMock()
        mock_module.AzureBlobFileSystem.return_value = mock_instance
        return mock_module, mock_instance

    def test_s3_uses_s3fs_with_endpoint(self) -> None:
        mock_s3fs, mock_instance = self._make_s3_mock()
        env = {
            "STORAGE_BACKEND": "s3",
            "S3_ENDPOINT": "https://minio:9000",
            "AWS_ACCESS_KEY_ID": "testkey",
            "AWS_SECRET_ACCESS_KEY": "testsecret",
        }
        with patch.dict(sys.modules, {"s3fs": mock_s3fs}):
            with patch.dict(os.environ, env):
                result = cliente_storage.get_storage_fs()

        mock_s3fs.S3FileSystem.assert_called_once_with(
            key="testkey",
            secret="testsecret",
            client_kwargs={"endpoint_url": "https://minio:9000"},
        )
        assert result is mock_instance

    def test_s3_endpoint_can_use_minio_alias(self) -> None:
        mock_s3fs, _ = self._make_s3_mock()
        env = {
            "STORAGE_BACKEND": "s3",
            "MINIO_ENDPOINT": "https://minio:9000",
        }
        with patch.dict(sys.modules, {"s3fs": mock_s3fs}):
            with patch.dict(os.environ, env, clear=True):
                cliente_storage.get_storage_fs()

        call_kwargs = mock_s3fs.S3FileSystem.call_args.kwargs
        assert call_kwargs["client_kwargs"] == {"endpoint_url": "https://minio:9000"}

    def test_s3_does_not_set_endpoint(self) -> None:
        mock_s3fs, _ = self._make_s3_mock()
        env = {
            "STORAGE_BACKEND": "s3",
            "AWS_ACCESS_KEY_ID": "k",
            "AWS_SECRET_ACCESS_KEY": "s",
        }
        with patch.dict(sys.modules, {"s3fs": mock_s3fs}):
            with patch.dict(os.environ, env):
                cliente_storage.get_storage_fs()

        call_kwargs = mock_s3fs.S3FileSystem.call_args.kwargs
        assert "endpoint_url" not in call_kwargs
        assert "client_kwargs" not in call_kwargs

    def test_s3_uses_aws_env_fallback(self) -> None:
        mock_s3fs, _ = self._make_s3_mock()
        env = {
            "STORAGE_BACKEND": "s3",
            "AWS_ACCESS_KEY_ID": "aws-key",
            "AWS_SECRET_ACCESS_KEY": "aws-secret",
        }
        # Remove MINIO_* so the AWS_ fallback is exercised
        clean_env = {k: v for k, v in env.items()}
        with patch.dict(sys.modules, {"s3fs": mock_s3fs}):
            with patch.dict(os.environ, clean_env, clear=True):
                cliente_storage.get_storage_fs()

        call_kwargs = mock_s3fs.S3FileSystem.call_args.kwargs
        assert call_kwargs["key"] == "aws-key"
        assert call_kwargs["secret"] == "aws-secret"

    def test_adls_uses_adlfs(self) -> None:
        mock_adlfs, mock_instance = self._make_adls_mock()
        env = {
            "STORAGE_BACKEND": "adls",
            "AZURE_STORAGE_ACCOUNT": "myaccount",
            "AZURE_STORAGE_KEY": "mykey",
        }
        with patch.dict(sys.modules, {"adlfs": mock_adlfs}):
            with patch.dict(os.environ, env):
                result = cliente_storage.get_storage_fs()

        mock_adlfs.AzureBlobFileSystem.assert_called_once_with(
            account_name="myaccount",
            account_key="mykey",
        )
        assert result is mock_instance

    def test_unknown_backend_raises(self) -> None:
        with patch.dict(os.environ, {"STORAGE_BACKEND": "hdfs"}):
            with pytest.raises(ValueError, match="Unknown STORAGE_BACKEND"):
                cliente_storage.get_storage_fs()


class TestEnsureBucketExists:
    def test_creates_bucket_when_missing(self) -> None:
        mock_fs = MagicMock()
        mock_fs.exists.return_value = False

        cliente_storage.ensure_bucket_exists(mock_fs, "my-bucket")

        mock_fs.mkdir.assert_called_once_with("my-bucket")

    def test_skips_mkdir_when_bucket_exists(self) -> None:
        mock_fs = MagicMock()
        mock_fs.exists.return_value = True

        cliente_storage.ensure_bucket_exists(mock_fs, "my-bucket")

        mock_fs.mkdir.assert_not_called()

    def test_swallows_exceptions(self) -> None:
        mock_fs = MagicMock()
        mock_fs.exists.side_effect = RuntimeError("connection refused")

        # Should not raise
        cliente_storage.ensure_bucket_exists(mock_fs, "my-bucket")
