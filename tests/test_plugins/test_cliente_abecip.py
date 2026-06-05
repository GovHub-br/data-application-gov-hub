from http import HTTPStatus
from unittest.mock import patch, Mock

import pytest

from cliente_abecip import ClienteAbecip


@pytest.fixture
def cliente_abecip() -> ClienteAbecip:
    return ClienteAbecip()


def test_get_financiamentos_success(cliente_abecip: ClienteAbecip) -> None:
    ano = 2023
    mes = 10
    expected_path = "/dados/financiamentos"
    expected_params = {"ano": ano, "mes": mes}
    mock_payload = {"total": 1000, "financiamentos": []}

    with patch.object(cliente_abecip, "request") as mock_request:
        mock_request.return_value = (HTTPStatus.OK, mock_payload)

        status, data = cliente_abecip.get_financiamentos(ano=ano, mes=mes)

        assert status == HTTPStatus.OK
        assert data == mock_payload
        mock_request.assert_called_once_with(
            "GET", expected_path, params=expected_params
        )


def test_get_indicadores_success(cliente_abecip: ClienteAbecip) -> None:
    expected_path = "/dados/indicadores"
    mock_payload = [{"indicador": "IGP-M", "valor": "0.5"}]

    with patch.object(cliente_abecip, "request") as mock_request:
        mock_request.return_value = (HTTPStatus.OK, mock_payload)

        status, data = cliente_abecip.get_indicadores()

        assert status == HTTPStatus.OK
        assert data == mock_payload
        mock_request.assert_called_once_with("GET", expected_path)


def test_get_financiamentos_network_isolation_and_payload_parsing(
    cliente_abecip: ClienteAbecip,
) -> None:
    ano = 2023
    expected_path = "/dados/financiamentos"
    expected_params = {"ano": ano}
    
    # Payload format simulating an API response
    mock_payload = {"total": 500, "financiamentos": [{"id": 1}]}

    mock_response = Mock()
    mock_response.status_code = HTTPStatus.OK
    mock_response.json.return_value = mock_payload
    mock_response.raise_for_status.return_value = None

    with patch.object(cliente_abecip.client, "request") as mock_httpx_request:
        mock_httpx_request.return_value = mock_response

        status, data = cliente_abecip.get_financiamentos(ano=ano)

        assert status == HTTPStatus.OK
        # Assegura que o parseamento retornou o dicionário correspondente ao JSON da rede
        assert data == mock_payload

        mock_httpx_request.assert_called_once_with(
            "GET",
            expected_path,
            params=expected_params,
            timeout=cliente_abecip.DEFAULT_TIMEOUT,
        )
