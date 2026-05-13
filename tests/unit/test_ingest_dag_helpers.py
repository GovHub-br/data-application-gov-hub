"""
Tests for generate_test_dataframe, which lives in the test ingest DAG module.
Airflow and cloud SDK stubs are set up by conftest.py before any import runs.
"""

import importlib.util
import os
import sys
from datetime import datetime
from unittest.mock import MagicMock, patch

import polars as pl

_DAG_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "airflow_lappis",
    "dags",
    "data_ingest",
    "test_source",
    "test_ingest_dag.py",
)


def _load_generate_fn():  # type: ignore[return]
    # Stub landing_zone only for the duration of the DAG module load so it
    # doesn't leak into other test modules that import the real landing_zone.
    _stubs = {"landing_zone": MagicMock()}
    with patch.dict(sys.modules, _stubs):
        spec = importlib.util.spec_from_file_location("test_ingest_dag", _DAG_FILE)
        assert spec and spec.loader
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module.generate_test_dataframe


generate_test_dataframe = _load_generate_fn()

EXPECTED_COLUMNS = ["id", "name", "value", "category", "active", "dt_ingest"]
VALID_CATEGORIES = {"A", "B", "C", "D"}


class TestGenerateTestDataframe:
    def test_returns_polars_dataframe(self) -> None:
        df = generate_test_dataframe(10)
        assert isinstance(df, pl.DataFrame)

    def test_correct_number_of_rows(self) -> None:
        for n in (1, 50, 150):
            assert len(generate_test_dataframe(n)) == n

    def test_has_all_expected_columns(self) -> None:
        df = generate_test_dataframe(5)
        assert df.columns == EXPECTED_COLUMNS

    def test_id_is_sequential_from_one(self) -> None:
        df = generate_test_dataframe(5)
        assert df["id"].to_list() == [1, 2, 3, 4, 5]

    def test_name_is_six_uppercase_chars(self) -> None:
        df = generate_test_dataframe(20)
        for name in df["name"].to_list():
            assert len(name) == 6
            assert name.isupper()

    def test_value_in_expected_range(self) -> None:
        df = generate_test_dataframe(100)
        assert df["value"].min() >= 0.0
        assert df["value"].max() <= 100_000.0

    def test_category_only_valid_values(self) -> None:
        df = generate_test_dataframe(100)
        assert set(df["category"].to_list()).issubset(VALID_CATEGORIES)

    def test_active_is_boolean(self) -> None:
        df = generate_test_dataframe(10)
        assert df["active"].dtype == pl.Boolean

    def test_dt_ingest_is_parseable_iso(self) -> None:
        df = generate_test_dataframe(5)
        for val in df["dt_ingest"].to_list():
            datetime.fromisoformat(val)  # raises if not valid ISO

    def test_no_nulls(self) -> None:
        df = generate_test_dataframe(50)
        for col in df.columns:
            assert df[col].null_count() == 0, f"Unexpected nulls in '{col}'"
