from http import HTTPStatus
from unittest.mock import MagicMock, patch

from cliente_contratos import ClienteContratos

# Helpers

# Cada método de ClienteContratos delega a self.request e aplica a mesma
# regra: retorna os dados apenas se status == 200 E data for uma lista;
# caso contrário, retorna None. Os testes abaixo cobrem, para os seis
# métodos, três cenários (sucesso, status de erro, status OK com data que
# não é lista) e validam o endpoint chamado.

CONTRATOS_FAKE = [{"id": 1, "numero": "001/2024"}, {"id": 2, "numero": "002/2024"}]


# get_contratos_by_ug
@patch("cliente_base.ClienteBase.request")
def test_get_contratos_by_ug_sucesso(mock_request: MagicMock) -> None:
    mock_request.return_value = (HTTPStatus.OK, CONTRATOS_FAKE)
    cliente = ClienteContratos()

    resultado = cliente.get_contratos_by_ug("153173")

    assert resultado == CONTRATOS_FAKE
    args = mock_request.call_args.args
    assert "/contrato/ug/153173" in args


@patch("cliente_base.ClienteBase.request")
def test_get_contratos_by_ug_status_erro(mock_request: MagicMock) -> None:
    mock_request.return_value = (HTTPStatus.NOT_FOUND, None)
    cliente = ClienteContratos()

    assert cliente.get_contratos_by_ug("153173") is None


@patch("cliente_base.ClienteBase.request")
def test_get_contratos_by_ug_data_nao_lista(mock_request: MagicMock) -> None:
    mock_request.return_value = (HTTPStatus.OK, {"erro": "formato inesperado"})
    cliente = ClienteContratos()

    assert cliente.get_contratos_by_ug("153173") is None


# get_contratos_inativos_by_ug
@patch("cliente_base.ClienteBase.request")
def test_get_contratos_inativos_by_ug_sucesso(mock_request: MagicMock) -> None:
    mock_request.return_value = (HTTPStatus.OK, CONTRATOS_FAKE)
    cliente = ClienteContratos()

    resultado = cliente.get_contratos_inativos_by_ug("153173")

    assert resultado == CONTRATOS_FAKE
    args = mock_request.call_args.args
    assert "/contrato/inativo/ug/153173" in args


@patch("cliente_base.ClienteBase.request")
def test_get_contratos_inativos_by_ug_status_erro(mock_request: MagicMock) -> None:
    mock_request.return_value = (HTTPStatus.INTERNAL_SERVER_ERROR, None)
    cliente = ClienteContratos()

    assert cliente.get_contratos_inativos_by_ug("153173") is None


@patch("cliente_base.ClienteBase.request")
def test_get_contratos_inativos_by_ug_data_nao_lista(mock_request: MagicMock) -> None:
    mock_request.return_value = (HTTPStatus.OK, {"erro": "formato inesperado"})
    cliente = ClienteContratos()

    assert cliente.get_contratos_inativos_by_ug("153173") is None


# get_faturas_by_contrato_id
@patch("cliente_base.ClienteBase.request")
def test_get_faturas_by_contrato_id_sucesso(mock_request: MagicMock) -> None:
    mock_request.return_value = (HTTPStatus.OK, CONTRATOS_FAKE)
    cliente = ClienteContratos()

    resultado = cliente.get_faturas_by_contrato_id("12345")

    assert resultado == CONTRATOS_FAKE
    args = mock_request.call_args.args
    assert "/contrato/12345/faturas" in args


@patch("cliente_base.ClienteBase.request")
def test_get_faturas_by_contrato_id_status_erro(mock_request: MagicMock) -> None:
    mock_request.return_value = (HTTPStatus.NOT_FOUND, None)
    cliente = ClienteContratos()

    assert cliente.get_faturas_by_contrato_id("12345") is None


@patch("cliente_base.ClienteBase.request")
def test_get_faturas_by_contrato_id_data_nao_lista(mock_request: MagicMock) -> None:
    mock_request.return_value = (HTTPStatus.OK, {"erro": "formato inesperado"})
    cliente = ClienteContratos()

    assert cliente.get_faturas_by_contrato_id("12345") is None


# get_empenhos_by_contrato_id
@patch("cliente_base.ClienteBase.request")
def test_get_empenhos_by_contrato_id_sucesso(mock_request: MagicMock) -> None:
    mock_request.return_value = (HTTPStatus.OK, CONTRATOS_FAKE)
    cliente = ClienteContratos()

    resultado = cliente.get_empenhos_by_contrato_id("12345")

    assert resultado == CONTRATOS_FAKE
    args = mock_request.call_args.args
    assert "/contrato/12345/empenhos" in args


@patch("cliente_base.ClienteBase.request")
def test_get_empenhos_by_contrato_id_status_erro(mock_request: MagicMock) -> None:
    mock_request.return_value = (HTTPStatus.NOT_FOUND, None)
    cliente = ClienteContratos()

    assert cliente.get_empenhos_by_contrato_id("12345") is None


@patch("cliente_base.ClienteBase.request")
def test_get_empenhos_by_contrato_id_data_nao_lista(mock_request: MagicMock) -> None:
    mock_request.return_value = (HTTPStatus.OK, {"erro": "formato inesperado"})
    cliente = ClienteContratos()

    assert cliente.get_empenhos_by_contrato_id("12345") is None


# get_cronograma_by_contrato_id
@patch("cliente_base.ClienteBase.request")
def test_get_cronograma_by_contrato_id_sucesso(mock_request: MagicMock) -> None:
    mock_request.return_value = (HTTPStatus.OK, CONTRATOS_FAKE)
    cliente = ClienteContratos()

    resultado = cliente.get_cronograma_by_contrato_id("12345")

    assert resultado == CONTRATOS_FAKE
    args = mock_request.call_args.args
    assert "/contrato/12345/cronograma" in args


@patch("cliente_base.ClienteBase.request")
def test_get_cronograma_by_contrato_id_status_erro(mock_request: MagicMock) -> None:
    mock_request.return_value = (HTTPStatus.NOT_FOUND, None)
    cliente = ClienteContratos()

    assert cliente.get_cronograma_by_contrato_id("12345") is None


@patch("cliente_base.ClienteBase.request")
def test_get_cronograma_by_contrato_id_data_nao_lista(mock_request: MagicMock) -> None:
    mock_request.return_value = (HTTPStatus.OK, {"erro": "formato inesperado"})
    cliente = ClienteContratos()

    assert cliente.get_cronograma_by_contrato_id("12345") is None


# get_terceirizados_by_contrato_id
@patch("cliente_base.ClienteBase.request")
def test_get_terceirizados_by_contrato_id_sucesso(mock_request: MagicMock) -> None:
    mock_request.return_value = (HTTPStatus.OK, CONTRATOS_FAKE)
    cliente = ClienteContratos()

    resultado = cliente.get_terceirizados_by_contrato_id("12345")

    assert resultado == CONTRATOS_FAKE
    args = mock_request.call_args.args
    assert "/contrato/12345/terceirizados" in args


@patch("cliente_base.ClienteBase.request")
def test_get_terceirizados_by_contrato_id_status_erro(mock_request: MagicMock) -> None:
    mock_request.return_value = (HTTPStatus.NOT_FOUND, None)
    cliente = ClienteContratos()

    assert cliente.get_terceirizados_by_contrato_id("12345") is None


@patch("cliente_base.ClienteBase.request")
def test_get_terceirizados_by_contrato_id_data_nao_lista(
    mock_request: MagicMock,
) -> None:
    mock_request.return_value = (HTTPStatus.OK, {"erro": "formato inesperado"})
    cliente = ClienteContratos()

    assert cliente.get_terceirizados_by_contrato_id("12345") is None
