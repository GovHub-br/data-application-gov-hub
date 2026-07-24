from datetime import date
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from homologation_flow import load_landing_files, validate_dataframe, write_to_postgres

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _sample_df(n: int = 3) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "id": list(range(1, n + 1)),
            "name": ["A"] * n,
            "value": [1.0] * n,
            "category": ["X"] * n,
            "active": [True] * n,
            "dt_ingest": ["2024-01-01T00:00:00"] * n,
        }
    )


# ---------------------------------------------------------------------------
# load_landing_files
# ---------------------------------------------------------------------------


class TestLoadLandingFiles:
    def test_returns_files_and_dataframe(self) -> None:
        df = _sample_df(5)
        files = ["bucket/src/ent/2024/01/01/run1.parquet"]

        with patch("homologation_flow.list_files", return_value=files):
            with patch("homologation_flow.read_parquet", return_value=df):
                returned_files, returned_df = load_landing_files(
                    "src", "ent", date(2024, 1, 1)
                )

        assert returned_files == files
        assert returned_df.shape == df.shape

    def test_raises_when_no_files(self) -> None:
        with patch("homologation_flow.list_files", return_value=[]):
            with pytest.raises(ValueError, match="No Parquet files found"):
                load_landing_files("src", "ent", date(2024, 1, 1))

    def test_prefix_uses_zero_padded_date(self) -> None:
        df = _sample_df()
        with patch(
            "homologation_flow.list_files", return_value=["f.parquet"]
        ) as mock_list:
            with patch("homologation_flow.read_parquet", return_value=df):
                with patch("homologation_flow.get_bucket", return_value="b"):
                    load_landing_files("src", "ent", date(2024, 3, 7))

        prefix_used = mock_list.call_args[0][0]
        assert "2024/03/07" in prefix_used

    def test_uses_provided_bucket_over_env(self) -> None:
        df = _sample_df()
        with patch(
            "homologation_flow.list_files", return_value=["f.parquet"]
        ) as mock_list:
            with patch("homologation_flow.read_parquet", return_value=df):
                load_landing_files("src", "ent", date(2024, 1, 1), bucket="custom-bucket")

        prefix_used = mock_list.call_args[0][0]
        assert prefix_used.startswith("custom-bucket/")

    def test_concatenates_multiple_files(self) -> None:
        df_a = _sample_df(2)
        df_b = _sample_df(3)
        files = ["file_a.parquet", "file_b.parquet"]

        with patch("homologation_flow.list_files", return_value=files):
            with patch("homologation_flow.read_parquet", side_effect=[df_a, df_b]):
                _, result = load_landing_files("src", "ent", date(2024, 1, 1))

        assert len(result) == 5

    def test_read_parquet_called_once_per_file(self) -> None:
        df = _sample_df()
        files = ["a.parquet", "b.parquet", "c.parquet"]

        with patch("homologation_flow.list_files", return_value=files):
            with patch("homologation_flow.read_parquet", return_value=df) as mock_read:
                load_landing_files("src", "ent", date(2024, 1, 1))

        assert mock_read.call_count == 3


# ---------------------------------------------------------------------------
# validate_dataframe
# ---------------------------------------------------------------------------


class TestValidateDataframe:
    COLS = ["id", "name", "value", "category", "active", "dt_ingest"]
    NOT_NULL = ["id", "dt_ingest"]
    KEYS = ["id"]

    def test_passes_on_valid_data(self) -> None:
        df = _sample_df(10)
        passed, results = validate_dataframe(df, self.COLS, self.NOT_NULL, self.KEYS)
        assert passed
        assert all(r.passed for r in results)

    def test_fails_on_missing_column(self) -> None:
        df = _sample_df().drop("value")
        passed, results = validate_dataframe(df, self.COLS, self.NOT_NULL, self.KEYS)
        assert not passed
        failed = [r for r in results if not r.passed]
        assert any("value" in r.message for r in failed)

    def test_fails_on_null_in_required_column(self) -> None:
        df = _sample_df(3).with_columns(
            pl.when(pl.col("id") == 2).then(None).otherwise(pl.col("id")).alias("id")
        )
        passed, _ = validate_dataframe(df, self.COLS, self.NOT_NULL, self.KEYS)
        assert not passed

    def test_fails_below_min_rows(self) -> None:
        df = _sample_df(2)
        passed, _ = validate_dataframe(
            df, self.COLS, self.NOT_NULL, self.KEYS, min_rows=5
        )
        assert not passed

    def test_fails_on_duplicates(self) -> None:
        df = pl.concat([_sample_df(3), _sample_df(3)])  # duplicate IDs
        passed, _ = validate_dataframe(df, self.COLS, self.NOT_NULL, self.KEYS)
        assert not passed

    def test_returns_all_check_results(self) -> None:
        df = _sample_df(5)
        _, results = validate_dataframe(df, self.COLS, self.NOT_NULL, self.KEYS)
        # schema + row_count + one per not_null column + no_duplicates
        assert len(results) == 1 + 1 + len(self.NOT_NULL) + 1


# ---------------------------------------------------------------------------
# write_to_postgres
# ---------------------------------------------------------------------------


class TestWriteToPostgres:
    def test_returns_row_count(self) -> None:
        df = _sample_df(7)
        mock_db = MagicMock()

        with patch("cliente_postgres.ClientPostgresDB", return_value=mock_db):
            count = write_to_postgres(df, "my_table", "my_schema", "conn_str")

        assert count == 7

    def test_calls_insert_data_with_correct_args(self) -> None:
        df = _sample_df(2)
        mock_db = MagicMock()

        with patch("cliente_postgres.ClientPostgresDB", return_value=mock_db):
            write_to_postgres(df, "my_table", "my_schema", "conn_str")

        mock_db.insert_data.assert_called_once()
        call_kwargs = mock_db.insert_data.call_args
        assert call_kwargs.kwargs["table_name"] == "my_table"
        assert call_kwargs.kwargs["schema"] == "my_schema"

    def test_passes_records_as_list_of_dicts(self) -> None:
        df = _sample_df(2)
        mock_db = MagicMock()

        with patch("cliente_postgres.ClientPostgresDB", return_value=mock_db):
            write_to_postgres(df, "t", "s", "conn")

        data_arg = mock_db.insert_data.call_args.args[0]
        assert isinstance(data_arg, list)
        assert isinstance(data_arg[0], dict)
        assert len(data_arg) == 2

    def test_constructs_client_with_conn_str(self) -> None:
        df = _sample_df(1)

        with patch("cliente_postgres.ClientPostgresDB") as mock_cls:
            mock_cls.return_value = MagicMock()
            write_to_postgres(df, "t", "s", "my-conn-str")

        mock_cls.assert_called_once_with("my-conn-str")
