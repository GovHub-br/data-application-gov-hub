import io
from contextlib import contextmanager
from datetime import date
from unittest.mock import MagicMock, patch

import polars as pl

# conftest.py wires up sys.path; import helpers directly.
import landing_zone


def _make_mock_fs(buffer: io.BytesIO | None = None) -> MagicMock:
    """Return a mock fsspec filesystem whose open() wraps a BytesIO buffer."""
    buf = buffer if buffer is not None else io.BytesIO()
    mock_fs = MagicMock()
    mock_fs.exists.return_value = True

    @contextmanager
    def _open(path: str, mode: str):  # type: ignore[override]
        yield buf

    mock_fs.open = _open
    return mock_fs


class TestBuildLandingPath:
    def test_path_structure(self) -> None:
        with patch("landing_zone.get_bucket", return_value="my-bucket"):
            path = landing_zone.build_landing_path(
                "siape", "servidores", date(2024, 3, 5), "abc123"
            )
        assert path == "my-bucket/siape/servidores/2024/03/05/abc123.parquet"

    def test_month_and_day_are_zero_padded(self) -> None:
        with patch("landing_zone.get_bucket", return_value="b"):
            path = landing_zone.build_landing_path("src", "ent", date(2024, 1, 7), "run1")
        assert "/2024/01/07/" in path

    def test_custom_extension(self) -> None:
        with patch("landing_zone.get_bucket", return_value="b"):
            path = landing_zone.build_landing_path(
                "src", "ent", date(2024, 6, 1), "run1", ext="csv"
            )
        assert path.endswith(".csv")

    def test_bucket_prefix_comes_first(self) -> None:
        with patch("landing_zone.get_bucket", return_value="data-lake"):
            path = landing_zone.build_landing_path("src", "ent", date(2024, 1, 1), "id")
        assert path.startswith("data-lake/")


class TestWriteParquet:
    def test_returns_the_path(self) -> None:
        df = pl.DataFrame({"id": [1, 2]})
        buf = io.BytesIO()
        mock_fs = _make_mock_fs(buf)

        with patch("landing_zone.get_storage_fs", return_value=mock_fs):
            with patch("landing_zone.get_bucket", return_value="b"):
                with patch("landing_zone.ensure_bucket_exists"):
                    result = landing_zone.write_parquet(df, "b/path/file.parquet")

        assert result == "b/path/file.parquet"

    def test_writes_valid_parquet(self) -> None:
        df = pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        buf = io.BytesIO()
        mock_fs = _make_mock_fs(buf)

        with patch("landing_zone.get_storage_fs", return_value=mock_fs):
            with patch("landing_zone.get_bucket", return_value="b"):
                with patch("landing_zone.ensure_bucket_exists"):
                    landing_zone.write_parquet(df, "b/file.parquet")

        buf.seek(0)
        result = pl.read_parquet(buf)
        assert result.shape == (3, 2)
        assert result["id"].to_list() == [1, 2, 3]

    def test_calls_ensure_bucket_exists(self) -> None:
        df = pl.DataFrame({"x": [1]})
        mock_fs = _make_mock_fs()

        with patch("landing_zone.get_storage_fs", return_value=mock_fs):
            with patch("landing_zone.get_bucket", return_value="b"):
                with patch("landing_zone.ensure_bucket_exists") as mock_ensure:
                    landing_zone.write_parquet(df, "b/p.parquet")

        mock_ensure.assert_called_once_with(mock_fs, "b")


class TestReadParquet:
    def test_returns_correct_dataframe(self) -> None:
        original = pl.DataFrame({"id": [10, 20], "val": [1.5, 2.5]})
        buf = io.BytesIO()
        original.write_parquet(buf)
        buf.seek(0)

        mock_fs = _make_mock_fs(buf)
        with patch("landing_zone.get_storage_fs", return_value=mock_fs):
            result = landing_zone.read_parquet("b/file.parquet")

        assert result.shape == original.shape
        assert result["id"].to_list() == [10, 20]

    def test_roundtrip_preserves_dtypes(self) -> None:
        original = pl.DataFrame(
            {
                "id": pl.Series([1, 2], dtype=pl.Int64),
                "active": pl.Series([True, False], dtype=pl.Boolean),
                "score": pl.Series([3.14, 2.71], dtype=pl.Float64),
            }
        )
        buf = io.BytesIO()
        original.write_parquet(buf)
        buf.seek(0)

        mock_fs = _make_mock_fs(buf)
        with patch("landing_zone.get_storage_fs", return_value=mock_fs):
            result = landing_zone.read_parquet("b/file.parquet")

        assert result.dtypes == original.dtypes


class TestListFiles:
    def test_returns_glob_results(self) -> None:
        expected = ["b/src/ent/2024/01/01/run1.parquet"]
        mock_fs = MagicMock()
        mock_fs.glob.return_value = expected

        with patch("landing_zone.get_storage_fs", return_value=mock_fs):
            result = landing_zone.list_files("b/src/ent/2024/01/01")

        assert result == expected
        mock_fs.glob.assert_called_once_with("b/src/ent/2024/01/01/**/*.parquet")

    def test_returns_empty_on_file_not_found(self) -> None:
        mock_fs = MagicMock()
        mock_fs.glob.side_effect = FileNotFoundError("no such prefix")

        with patch("landing_zone.get_storage_fs", return_value=mock_fs):
            result = landing_zone.list_files("b/missing/prefix")

        assert result == []

    def test_custom_extension_passed_to_glob(self) -> None:
        mock_fs = MagicMock()
        mock_fs.glob.return_value = []

        with patch("landing_zone.get_storage_fs", return_value=mock_fs):
            landing_zone.list_files("b/src", ext="csv")

        mock_fs.glob.assert_called_once_with("b/src/**/*.csv")
