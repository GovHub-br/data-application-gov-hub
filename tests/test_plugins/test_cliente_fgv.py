from http import HTTPStatus
from unittest.mock import Mock, patch

import pytest

from cliente_fgv import ClienteFgv


@pytest.fixture
def cliente_fgv() -> ClienteFgv:
    return ClienteFgv()


def test_get_indices_success(cliente_fgv: ClienteFgv) -> None:
    expected_path = "/indices"
    mock_payload = [
        {"codigo": "IGP-M", "nome": "Índice Geral de Preços do Mercado"},
        {"codigo": "IPA", "nome": "Índice de Preços ao Produtor Amplo"},
    ]

    with patch.object(cliente_fgv, "request") as mock_request:
        mock_request.return_value = (HTTPStatus.OK, mock_payload)

        status, data = cliente_fgv.get_indices()

        assert status == HTTPStatus.OK
        assert data == mock_payload
        mock_request.assert_called_once_with("GET", expected_path)


def test_get_serie_success_with_all_params(cliente_fgv: ClienteFgv) -> None:
    codigo_serie = "IGP-M"
    data_inicio = "2023-01-01"
    data_fim = "2023-12-31"
    expected_path = "/series"
    expected_params = {
        "codigo": codigo_serie,
        "data_inicio": data_inicio,
        "data_fim": data_fim,
    }
    mock_payload = [{"data": "2023-01-01", "valor": "0.54"}]

    with patch.object(cliente_fgv, "request") as mock_request:
        mock_request.return_value = (HTTPStatus.OK, mock_payload)

        status, data = cliente_fgv.get_serie(
            codigo_serie=codigo_serie,
            data_inicio=data_inicio,
            data_fim=data_fim,
        )

        assert status == HTTPStatus.OK
        assert data == mock_payload
        mock_request.assert_called_once_with(
            "GET", expected_path, params=expected_params
        )


def test_get_serie_success_only_codigo(cliente_fgv: ClienteFgv) -> None:
    codigo_serie = "IPA"
    expected_path = "/series"
    expected_params = {"codigo": codigo_serie}
    mock_payload = [{"data": "2023-06-01", "valor": "1.20"}]

    with patch.object(cliente_fgv, "request") as mock_request:
        mock_request.return_value = (HTTPStatus.OK, mock_payload)

        status, data = cliente_fgv.get_serie(codigo_serie=codigo_serie)

        assert status == HTTPStatus.OK
        assert data == mock_payload
        mock_request.assert_called_once_with(
            "GET", expected_path, params=expected_params
        )


def test_get_serie_network_isolation_and_payload_parsing(
    cliente_fgv: ClienteFgv,
) -> None:
    codigo_serie = "IGP-M"
    expected_path = "/series"
    expected_params = {"codigo": codigo_serie}
    mock_payload = [{"data": "2023-01-01", "valor": "0.54"}]

    mock_response = Mock()
    mock_response.status_code = HTTPStatus.OK
    mock_response.json.return_value = mock_payload
    mock_response.raise_for_status.return_value = None

    with patch.object(cliente_fgv.client, "request") as mock_httpx_request:
        mock_httpx_request.return_value = mock_response

        status, data = cliente_fgv.get_serie(codigo_serie=codigo_serie)

        assert status == HTTPStatus.OK
        assert data == mock_payload
        mock_httpx_request.assert_called_once_with(
            "GET",
            expected_path,
            params=expected_params,
            timeout=cliente_fgv.DEFAULT_TIMEOUT,
        )