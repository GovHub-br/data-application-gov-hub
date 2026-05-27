from http import HTTPStatus
from unittest.mock import Mock, patch

import httpx
import pytest

from cliente_base import ClienteBase


@pytest.fixture
def cliente_base() -> ClienteBase:
    return ClienteBase(base_url="https://example.com")


def _mock_response(
    status_code: HTTPStatus = HTTPStatus.OK,
    json_body: dict | list | None = None,
) -> Mock:
    mock_response = Mock()
    mock_response.status_code = status_code
    mock_response.json.return_value = json_body
    mock_response.raise_for_status.return_value = None
    return mock_response


@pytest.mark.parametrize(
    "json_body",
    [
        {"key": "value"},
        [{"id": 1}, {"id": 2}],
    ],
)
def test_request_success_json_body(
    cliente_base: ClienteBase, json_body: dict | list
) -> None:
    mock_response = _mock_response(json_body=json_body)

    with patch.object(cliente_base.client, "request", return_value=mock_response):
        status, data = cliente_base.request("GET", "/test")

    assert status == HTTPStatus.OK
    assert data == json_body
    mock_response.json.assert_called_once()


def test_request_sets_default_timeout(cliente_base: ClienteBase) -> None:
    mock_response = _mock_response(json_body={"ok": True})

    with patch.object(
        cliente_base.client, "request", return_value=mock_response
    ) as mock_request:
        cliente_base.request("GET", "/test")

    assert mock_request.call_args.kwargs["timeout"] == ClienteBase.DEFAULT_TIMEOUT


def test_request_retries_then_succeeds(cliente_base: ClienteBase) -> None:
    failing_response = _mock_response(status_code=HTTPStatus.SERVICE_UNAVAILABLE)
    failing_response.raise_for_status.side_effect = httpx.HTTPStatusError(
        "Server Error",
        request=Mock(),
        response=failing_response,
    )
    success_response = _mock_response(json_body={"recovered": True})

    with (
        patch.object(
            cliente_base.client,
            "request",
            side_effect=[failing_response, failing_response, success_response],
        ),
        patch("cliente_base.time.sleep") as mock_sleep,
    ):
        status, data = cliente_base.request("GET", "/test")

    assert status == HTTPStatus.OK
    assert data == {"recovered": True}
    assert mock_sleep.call_args_list == [((0,),), ((2,),)]


def test_request_exponential_backoff_on_failures(cliente_base: ClienteBase) -> None:
    failing_response = _mock_response(status_code=HTTPStatus.BAD_GATEWAY)
    failing_response.raise_for_status.side_effect = httpx.HTTPStatusError(
        "Bad Gateway",
        request=Mock(),
        response=failing_response,
    )

    with (
        patch.object(cliente_base.client, "request", return_value=failing_response),
        patch("cliente_base.time.sleep") as mock_sleep,
        pytest.raises(
            Exception, match="API failed after the maximum number of attempts!"
        ),
    ):
        cliente_base.request("GET", "/test")

    assert mock_sleep.call_args_list == [((0,),), ((2,),), ((8,),)]


def test_request_http_error_after_max_retries(cliente_base: ClienteBase) -> None:
    mock_response = _mock_response(status_code=HTTPStatus.INTERNAL_SERVER_ERROR)
    mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
        "Internal Server Error",
        request=Mock(),
        response=mock_response,
    )

    with (
        patch.object(cliente_base.client, "request", return_value=mock_response),
        patch("cliente_base.time.sleep"),
        pytest.raises(
            Exception, match="API failed after the maximum number of attempts!"
        ) as exc_info,
    ):
        cliente_base.request("GET", "/test")

    assert isinstance(exc_info.value.__cause__, httpx.HTTPError)


def test_request_network_error_without_response(cliente_base: ClienteBase) -> None:
    with (
        patch.object(
            cliente_base.client,
            "request",
            side_effect=httpx.ConnectError("Connection refused"),
        ),
        patch("cliente_base.time.sleep"),
        pytest.raises(
            Exception, match="API failed after the maximum number of attempts!"
        ) as exc_info,
    ):
        cliente_base.request("GET", "/test")

    assert isinstance(exc_info.value.__cause__, httpx.ConnectError)


def test_request_fallback_when_no_attempts(cliente_base: ClienteBase) -> None:
    with patch.object(ClienteBase, "DEFAULT_MAX_RETRIES", -1):
        status, data = cliente_base.request("GET", "/test")

    assert status == HTTPStatus.INTERNAL_SERVER_ERROR
    assert data is None
