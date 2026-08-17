import polars as pl

from homologation_helpers import (
    CheckResult,
    check_no_duplicates,
    check_not_null,
    check_null_rate,
    check_row_count,
    check_schema,
    run_checks,
)


class TestCheckResult:
    def test_fields(self) -> None:
        r = CheckResult("my_check", True, "all good")
        assert r.check_name == "my_check"
        assert r.passed is True
        assert r.message == "all good"


class TestCheckNotNull:
    def test_passes_when_no_nulls(self) -> None:
        df = pl.DataFrame({"id": [1, 2, 3]})
        results = check_not_null(df, ["id"])
        assert len(results) == 1
        assert results[0].passed

    def test_fails_when_nulls_present(self) -> None:
        df = pl.DataFrame({"id": [1, None, 3]})
        results = check_not_null(df, ["id"])
        assert not results[0].passed
        assert "1 null" in results[0].message

    def test_fails_when_column_missing(self) -> None:
        df = pl.DataFrame({"id": [1]})
        results = check_not_null(df, ["nonexistent"])
        assert not results[0].passed
        assert "not found" in results[0].message

    def test_one_result_per_column(self) -> None:
        df = pl.DataFrame({"a": [1], "b": [None], "c": [3]})
        results = check_not_null(df, ["a", "b", "c"])
        assert len(results) == 3
        assert results[0].passed  # a: no nulls
        assert not results[1].passed  # b: null
        assert results[2].passed  # c: no nulls

    def test_check_name_includes_column(self) -> None:
        df = pl.DataFrame({"cpf": [1]})
        results = check_not_null(df, ["cpf"])
        assert "cpf" in results[0].check_name

    def test_empty_columns_list_returns_empty(self) -> None:
        df = pl.DataFrame({"id": [1]})
        assert check_not_null(df, []) == []


class TestCheckRowCount:
    def test_passes_at_exact_minimum(self) -> None:
        df = pl.DataFrame({"id": [1, 2, 3]})
        assert check_row_count(df, min_rows=3)[0].passed

    def test_passes_above_minimum(self) -> None:
        df = pl.DataFrame({"id": [1, 2, 3]})
        assert check_row_count(df, min_rows=1)[0].passed

    def test_fails_below_minimum(self) -> None:
        df = pl.DataFrame({"id": [1, 2]})
        assert not check_row_count(df, min_rows=3)[0].passed

    def test_fails_on_empty_dataframe(self) -> None:
        df = pl.DataFrame({"id": pl.Series([], dtype=pl.Int64)})
        assert not check_row_count(df, min_rows=1)[0].passed

    def test_message_contains_actual_and_expected(self) -> None:
        df = pl.DataFrame({"id": [1]})
        msg = check_row_count(df, min_rows=10)[0].message
        assert "1" in msg
        assert "10" in msg


class TestCheckSchema:
    def test_passes_when_all_present(self) -> None:
        df = pl.DataFrame({"a": [1], "b": [2], "c": [3]})
        assert check_schema(df, ["a", "b", "c"])[0].passed

    def test_passes_with_extra_columns(self) -> None:
        df = pl.DataFrame({"a": [1], "b": [2], "extra": [3]})
        assert check_schema(df, ["a", "b"])[0].passed

    def test_fails_when_column_missing(self) -> None:
        df = pl.DataFrame({"a": [1]})
        result = check_schema(df, ["a", "b"])[0]
        assert not result.passed
        assert "b" in result.message

    def test_missing_columns_sorted_in_message(self) -> None:
        df = pl.DataFrame({"x": [1]})
        result = check_schema(df, ["z", "a", "x"])[0]
        assert not result.passed
        assert result.message.index("a") < result.message.index("z")

    def test_empty_expected_list_always_passes(self) -> None:
        df = pl.DataFrame({"id": [1]})
        assert check_schema(df, [])[0].passed


class TestCheckNoDuplicates:
    def test_passes_with_unique_keys(self) -> None:
        df = pl.DataFrame({"id": [1, 2, 3]})
        assert check_no_duplicates(df, ["id"])[0].passed

    def test_fails_with_duplicate_keys(self) -> None:
        df = pl.DataFrame({"id": [1, 1, 3]})
        result = check_no_duplicates(df, ["id"])[0]
        assert not result.passed
        assert "1 duplicate" in result.message

    def test_composite_key(self) -> None:
        df = pl.DataFrame({"org": ["A", "A", "B"], "year": [2023, 2024, 2023]})
        assert check_no_duplicates(df, ["org", "year"])[0].passed

    def test_composite_key_detects_duplicate(self) -> None:
        df = pl.DataFrame({"org": ["A", "A"], "year": [2023, 2023]})
        assert not check_no_duplicates(df, ["org", "year"])[0].passed

    def test_message_contains_duplicate_count(self) -> None:
        df = pl.DataFrame({"id": [1, 1, 2, 2, 2]})
        msg = check_no_duplicates(df, ["id"])[0].message
        assert "3" in msg  # 5 total - 2 unique = 3 duplicates


class TestCheckNullRate:
    def test_passes_below_threshold(self) -> None:
        df = pl.DataFrame({"val": [1.0, None, 3.0, 4.0]})
        # 25% null rate, max 50% → pass
        assert check_null_rate(df, "val", max_rate=0.5)[0].passed

    def test_passes_at_exact_threshold(self) -> None:
        df = pl.DataFrame({"val": [1.0, None]})
        # 50% null rate, max 50% → pass (<=)
        assert check_null_rate(df, "val", max_rate=0.5)[0].passed

    def test_fails_above_threshold(self) -> None:
        df = pl.DataFrame({"val": [None, None, 1.0]})
        # 66.7% null rate, max 50% → fail
        assert not check_null_rate(df, "val", max_rate=0.5)[0].passed

    def test_fails_when_column_missing(self) -> None:
        df = pl.DataFrame({"a": [1]})
        result = check_null_rate(df, "missing", max_rate=0.5)[0]
        assert not result.passed
        assert "not found" in result.message

    def test_zero_row_dataframe_passes(self) -> None:
        df = pl.DataFrame({"val": pl.Series([], dtype=pl.Float64)})
        # 0 nulls / max(0,1)=1 → 0% → passes any threshold
        assert check_null_rate(df, "val", max_rate=0.0)[0].passed


class TestRunChecks:
    def test_all_pass(self) -> None:
        checks = [CheckResult("a", True, "ok"), CheckResult("b", True, "ok")]
        all_passed, results = run_checks(checks)
        assert all_passed
        assert results is checks

    def test_one_fail_means_not_all_passed(self) -> None:
        checks = [CheckResult("a", True, "ok"), CheckResult("b", False, "bad")]
        all_passed, _ = run_checks(checks)
        assert not all_passed

    def test_empty_checks_passes(self) -> None:
        all_passed, results = run_checks([])
        assert all_passed
        assert results == []

    def test_returns_original_list(self) -> None:
        checks = [CheckResult("x", True, "")]
        _, returned = run_checks(checks)
        assert returned is checks
