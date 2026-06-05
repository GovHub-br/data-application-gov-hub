from http import HTTPStatus
from unittest.mock import patch, Mock

import pytest

from cliente_bacen import ClienteBacen


@pytest.fixture
def cliente_bacen() -> ClienteBacen:
    return ClienteBacen()


def test_get_serie_success_with_params(cliente_bacen: ClienteBacen) -> None:
    codigo_serie = 432
    data_inicial = "01/01/2020"
    data_final = "31/12/2020"
    expected_path = f"/dados/serie/bcdata.sgs.{codigo_serie}/dados"
    expected_params = {
        "formato": "json",
        "dataInicial": data_inicial,
        "dataFinal": data_final,
    }

    with patch.object(cliente_bacen, "request") as mock_request:
        mock_request.return_value = (
            HTTPStatus.OK,
            [{"data": "01/01/2020", "valor": "100.0"}],
        )

        status, data = cliente_bacen.get_serie(
            codigo_serie=codigo_serie,
            data_inicial=data_inicial,
            data_final=data_final,
        )

        assert status == HTTPStatus.OK
        assert data == [{"data": "01/01/2020", "valor": "100.0"}]
        mock_request.assert_called_once_with(
            "GET", expected_path, params=expected_params
        )


def test_get_serie_success_without_date_params(cliente_bacen: ClienteBacen) -> None:
    codigo_serie = 432
    expected_path = f"/dados/serie/bcdata.sgs.{codigo_serie}/dados"
    expected_params = {"formato": "json"}

    with patch.object(cliente_bacen, "request") as mock_request:
        mock_request.return_value = (
            HTTPStatus.OK,
            [{"data": "01/01/2020", "valor": "100.0"}],
        )

        status, data = cliente_bacen.get_serie(codigo_serie=codigo_serie)

        assert status == HTTPStatus.OK
        assert data == [{"data": "01/01/2020", "valor": "100.0"}]
        mock_request.assert_called_once_with(
            "GET", expected_path, params=expected_params
        )


def test_get_serie_network_isolation(cliente_bacen: ClienteBacen) -> None:
    codigo_serie = 432
    expected_path = f"/dados/serie/bcdata.sgs.{codigo_serie}/dados"
    expected_params = {"formato": "json"}

    mock_response = Mock()
    mock_response.status_code = HTTPStatus.OK
    mock_response.json.return_value = [{"data": "01/01/2020", "valor": "100.0"}]
    mock_response.raise_for_status.return_value = None

    with patch.object(cliente_bacen.client, "request") as mock_httpx_request:
        mock_httpx_request.return_value = mock_response

        status, data = cliente_bacen.get_serie(codigo_serie=codigo_serie)

        assert status == HTTPStatus.OK
        assert data == [{"data": "01/01/2020", "valor": "100.0"}]
        
        mock_httpx_request.assert_called_once_with(
            "GET",
            expected_path,
            params=expected_params,
            timeout=cliente_bacen.DEFAULT_TIMEOUT,
        )
