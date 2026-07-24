from dataclasses import dataclass

import polars as pl


@dataclass
class CheckResult:
    check_name: str
    passed: bool
    message: str


def check_not_null(df: pl.DataFrame, columns: list[str]) -> list[CheckResult]:
    """Fail if any of the given columns contain null values."""
    results = []
    for col in columns:
        if col not in df.columns:
            results.append(
                CheckResult(f"not_null:{col}", False, f"Column '{col}' not found")
            )
            continue
        null_count = df[col].null_count()
        passed = null_count == 0
        results.append(
            CheckResult(
                f"not_null:{col}",
                passed,
                f"'{col}' has {null_count} null(s)" if not passed else f"'{col}' OK",
            )
        )
    return results


def check_row_count(df: pl.DataFrame, min_rows: int) -> list[CheckResult]:
    """Fail if the DataFrame has fewer rows than min_rows."""
    passed = len(df) >= min_rows
    return [
        CheckResult(
            "row_count",
            passed,
            f"{len(df)} rows (minimum {min_rows})",
        )
    ]


def check_schema(df: pl.DataFrame, expected_columns: list[str]) -> list[CheckResult]:
    """Fail if any expected columns are missing from the DataFrame."""
    missing = sorted(set(expected_columns) - set(df.columns))
    passed = len(missing) == 0
    return [
        CheckResult(
            "schema",
            passed,
            f"Missing columns: {missing}" if not passed else "Schema OK",
        )
    ]


def check_no_duplicates(df: pl.DataFrame, key_columns: list[str]) -> list[CheckResult]:
    """Fail if there are duplicate rows based on key_columns."""
    n_total = len(df)
    n_unique = len(df.unique(subset=key_columns))
    dup_count = n_total - n_unique
    passed = dup_count == 0
    return [
        CheckResult(
            f"no_duplicates:{key_columns}",
            passed,
            (
                f"{dup_count} duplicate row(s) on {key_columns}"
                if not passed
                else f"No duplicates on {key_columns}"
            ),
        )
    ]


def check_null_rate(df: pl.DataFrame, column: str, max_rate: float) -> list[CheckResult]:
    """Fail if the null rate in column exceeds max_rate (0.0–1.0)."""
    if column not in df.columns:
        return [CheckResult(f"null_rate:{column}", False, f"Column '{column}' not found")]
    rate = df[column].null_count() / max(len(df), 1)
    passed = rate <= max_rate
    return [
        CheckResult(
            f"null_rate:{column}",
            passed,
            f"'{column}' null rate {rate:.2%} (max {max_rate:.2%})",
        )
    ]


def run_checks(checks: list[CheckResult]) -> tuple[bool, list[CheckResult]]:
    """Aggregate a list of CheckResults. Returns (all_passed, checks)."""
    return all(c.passed for c in checks), checks
