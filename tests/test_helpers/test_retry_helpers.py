import logging
from typing import Any, Callable
from unittest.mock import MagicMock, patch

import pytest

from retry_helpers import retry_on_exception


@pytest.fixture
def sleep_mock() -> Any:
    with patch("retry_helpers.time.sleep") as mock_sleep:
        yield mock_sleep


def _scripted(outcomes: list[Any]) -> tuple[Callable[..., Any], MagicMock]:
    queue = list(outcomes)
    spy = MagicMock()

    def target(*args: Any, **kwargs: Any) -> Any:
        spy(*args, **kwargs)
        outcome = queue.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    return target, spy


def _delays(sleep_mock: Any) -> list[float]:
    return [call.args[0] for call in sleep_mock.call_args_list]


# Particionamento de equivalência


@pytest.mark.parametrize("failures", [0, 1, 2])
def test_returns_value_when_success_comes_within_allowed_attempts(
    failures: int, sleep_mock: Any
) -> None:
    target, spy = _scripted([ValueError(i) for i in range(failures)] + ["ok"])

    assert retry_on_exception(max_attempts=3)(target)() == "ok"
    assert spy.call_count == failures + 1
    assert sleep_mock.call_count == failures


@pytest.mark.parametrize(
    "raised, retry_on",
    [
        (ValueError("a"), (ValueError,)),
        (FileNotFoundError("b"), (OSError,)),
        (KeyError("c"), (ValueError, KeyError)),
        (ZeroDivisionError("d"), (Exception,)),
    ],
)
def test_listed_exception_or_subclass_is_retried(
    raised: Exception, retry_on: tuple, sleep_mock: Any
) -> None:
    target, spy = _scripted([raised, "ok"])

    assert retry_on_exception(exceptions_to_retry=retry_on)(target)() == "ok"
    assert spy.call_count == 2


@pytest.mark.parametrize("retry_on", [(ValueError,), ()])
def test_unlisted_exception_propagates_immediately(
    retry_on: tuple, sleep_mock: Any
) -> None:
    target, spy = _scripted([KeyError("x"), "ok"])

    wrapped = retry_on_exception(max_attempts=3, exceptions_to_retry=retry_on)(target)

    with pytest.raises(KeyError):
        wrapped()

    assert spy.call_count == 1
    sleep_mock.assert_not_called()


@pytest.mark.parametrize("value", [None, 0, "", [], False])
def test_falsy_return_value_is_not_treated_as_failure(
    value: Any, sleep_mock: Any
) -> None:
    target, spy = _scripted([value])

    assert retry_on_exception(max_attempts=3)(target)() == value
    assert spy.call_count == 1


def test_forwards_arguments_and_preserves_metadata(sleep_mock: Any) -> None:
    calls: list[tuple] = []

    @retry_on_exception(max_attempts=2)
    def documented(*args: Any, **kwargs: Any) -> str:
        """Docstring original."""
        calls.append((args, kwargs))
        if len(calls) == 1:
            raise ValueError("retry")
        return "ok"

    assert documented(1, key="v") == "ok"
    assert calls == [((1,), {"key": "v"})] * 2
    assert documented.__name__ == "documented"
    assert documented.__doc__ == "Docstring original."


def test_default_parameters_use_three_attempts_and_doubling_delay(
    sleep_mock: Any,
) -> None:
    target, spy = _scripted([RuntimeError(i) for i in range(3)])

    wrapped = retry_on_exception()(target)

    with pytest.raises(RuntimeError):
        wrapped()

    assert spy.call_count == 3
    assert _delays(sleep_mock) == [1.0, 2.0]


# Análise de valor limite


@pytest.mark.parametrize("max_attempts", [1, 2, 3])
def test_boundary_max_attempts_raises_last_exception_after_exact_count(
    max_attempts: int, sleep_mock: Any
) -> None:
    errors = [ValueError(i) for i in range(max_attempts + 1)]
    target, spy = _scripted(list(errors))

    wrapped = retry_on_exception(max_attempts=max_attempts)(target)

    with pytest.raises(ValueError) as exc_info:
        wrapped()

    assert exc_info.value is errors[max_attempts - 1]
    assert spy.call_count == max_attempts
    assert sleep_mock.call_count == max_attempts - 1


@pytest.mark.parametrize(
    "initial_delay, backoff_factor, expected",
    [
        (1.0, 2.0, [1.0, 2.0, 4.0]),
        (0.5, 3.0, [0.5, 1.5, 4.5]),
        (0.0, 2.0, [0.0, 0.0, 0.0]),
        (2.0, 1.0, [2.0, 2.0, 2.0]),
        (4.0, 0.5, [4.0, 2.0, 1.0]),
        (2.0, 0.0, [2.0, 0.0, 0.0]),
    ],
)
def test_boundary_delay_and_backoff_factor(
    initial_delay: float,
    backoff_factor: float,
    expected: list[float],
    sleep_mock: Any,
) -> None:
    target, _ = _scripted([ValueError(i) for i in range(4)])

    wrapped = retry_on_exception(
        max_attempts=4, initial_delay=initial_delay, backoff_factor=backoff_factor
    )(target)

    with pytest.raises(ValueError):
        wrapped()

    assert _delays(sleep_mock) == expected


# Lacuna estrutural e comportamento observável dos branches


@pytest.mark.parametrize("max_attempts", [0, -1])
def test_non_positive_max_attempts_raises_runtime_error_without_calling(
    max_attempts: int, sleep_mock: Any
) -> None:
    target, spy = _scripted(["ok"])

    wrapped = retry_on_exception(max_attempts=max_attempts)(target)

    with pytest.raises(RuntimeError, match="Erro inesperado no mecanismo de retry"):
        wrapped()

    spy.assert_not_called()


def test_retry_progress_is_logged_without_error(
    caplog: pytest.LogCaptureFixture, sleep_mock: Any
) -> None:
    target, _ = _scripted([ValueError("boom"), ValueError("bang"), "ok"])

    with caplog.at_level(logging.INFO, logger="retry_helpers"):
        retry_on_exception(max_attempts=3, initial_delay=1.0)(target)()

    by_level = {
        level: [r.getMessage() for r in caplog.records if r.levelno == level]
        for level in (logging.INFO, logging.WARNING, logging.ERROR)
    }
    assert by_level[logging.WARNING] == [
        "Tentativa 1 falhou para target: boom",
        "Tentativa 2 falhou para target: bang",
    ]
    assert by_level[logging.INFO] == [
        "Aguardando 1.00s antes da próxima tentativa...",
        "Tentativa 2/3 para target",
        "Aguardando 2.00s antes da próxima tentativa...",
        "Tentativa 3/3 para target",
    ]
    assert by_level[logging.ERROR] == []


def test_exhausted_attempts_log_single_error(
    caplog: pytest.LogCaptureFixture, sleep_mock: Any
) -> None:
    target, _ = _scripted([ValueError(1), ValueError(2)])

    wrapped = retry_on_exception(max_attempts=2)(target)

    with caplog.at_level(logging.INFO, logger="retry_helpers"):
        with pytest.raises(ValueError):
            wrapped()

    errors = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
    assert errors == ["Todas as 2 tentativas falharam: target"]


def test_sleep_happens_between_attempts_and_not_after_the_last(
    sleep_mock: Any,
) -> None:
    order: list[str] = []
    sleep_mock.side_effect = lambda delay: order.append("sleep")

    def target() -> None:
        order.append("call")
        raise ValueError("x")

    wrapped = retry_on_exception(max_attempts=2)(target)

    with pytest.raises(ValueError):
        wrapped()

    assert order == ["call", "sleep", "call"]


# MC/DC D1: o except com cada tipo tenta de novo se o erro for de um dos tipos


@pytest.mark.parametrize(
    "raised, retried",
    [
        pytest.param(ValueError("a"), True, id="T1-A-true-B-false"),
        pytest.param(KeyError("b"), True, id="T2-A-false-B-true"),
        pytest.param(TypeError("c"), False, id="T3-A-false-B-false"),
    ],
)
def test_mcdc_each_listed_type_independently_decides_retry(
    raised: Exception, retried: bool, sleep_mock: Any
) -> None:
    target, spy = _scripted([raised, "ok"])
    wrapped = retry_on_exception(
        max_attempts=2, exceptions_to_retry=(ValueError, KeyError)
    )(target)

    if retried:
        assert wrapped() == "ok"
        assert spy.call_count == 2
    else:
        with pytest.raises(TypeError):
            wrapped()
        assert spy.call_count == 1


# MC/DC D2: espera só se a exceção casa com a lista e ainda há tentativas


@pytest.mark.parametrize(
    "retry_on, max_attempts, sleeps",
    [
        pytest.param((ValueError,), 2, 1, id="S1-M-true-L-true"),
        pytest.param((ValueError,), 1, 0, id="S2-M-true-L-false"),
        pytest.param((KeyError,), 2, 0, id="S3-M-false-L-true"),
    ],
)
def test_mcdc_sleep_requires_match_and_remaining_attempts(
    retry_on: tuple, max_attempts: int, sleeps: int, sleep_mock: Any
) -> None:
    target, _ = _scripted([ValueError("x"), ValueError("y")])

    wrapped = retry_on_exception(max_attempts=max_attempts, exceptions_to_retry=retry_on)(
        target
    )

    with pytest.raises(ValueError):
        wrapped()

    assert sleep_mock.call_count == sleeps
