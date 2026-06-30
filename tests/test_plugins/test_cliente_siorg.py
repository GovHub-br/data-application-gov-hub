import http
from http import HTTPStatus
from unittest.mock import patch

import pytest

from cliente_siorg import ClienteSiorg


@pytest.fixture
def cliente_siorg() -> ClienteSiorg:
    with patch("cliente_base.httpx.Client"):
        return ClienteSiorg()


def test_init_sets_base_url() -> None:
    with patch("cliente_base.httpx.Client") as mock_client:
        cliente = ClienteSiorg()

    assert cliente.base_url == ClienteSiorg.BASE_URL
    mock_client.assert_called_once_with(base_url=ClienteSiorg.BASE_URL, headers=None)


# ---------------------------------------------------------------------------
# get_estrutura_organizacional_resumida
# ---------------------------------------------------------------------------
def test_get_estrutura_resumida_success(cliente_siorg: ClienteSiorg) -> None:
    unidades = [{"codigoUnidade": "1"}, {"codigoUnidade": "2"}]
    with patch.object(
        cliente_siorg,
        "request",
        return_value=(HTTPStatus.OK, {"unidades": unidades}),
    ) as mock_request:
        result = cliente_siorg.get_estrutura_organizacional_resumida(
            codigo_poder="1", codigo_esfera="2", codigo_unidade="3"
        )

    assert result == unidades
    mock_request.assert_called_once_with(
        http.HTTPMethod.GET,
        "/estrutura-organizacional/resumida",
        params={"codigoPoder": "1", "codigoEsfera": "2", "codigoUnidade": "3"},
    )


def test_get_estrutura_resumida_no_params(cliente_siorg: ClienteSiorg) -> None:
    with patch.object(
        cliente_siorg,
        "request",
        return_value=(HTTPStatus.OK, {"unidades": []}),
    ) as mock_request:
        result = cliente_siorg.get_estrutura_organizacional_resumida()

    assert result == []
    mock_request.assert_called_once_with(
        http.HTTPMethod.GET,
        "/estrutura-organizacional/resumida",
        params={},
    )


def test_get_estrutura_resumida_partial_params(cliente_siorg: ClienteSiorg) -> None:
    with patch.object(
        cliente_siorg,
        "request",
        return_value=(HTTPStatus.OK, {"unidades": []}),
    ) as mock_request:
        cliente_siorg.get_estrutura_organizacional_resumida(codigo_poder="1")

    assert mock_request.call_args.kwargs["params"] == {"codigoPoder": "1"}


def test_get_estrutura_resumida_missing_unidades_key(
    cliente_siorg: ClienteSiorg,
) -> None:
    with patch.object(
        cliente_siorg, "request", return_value=(HTTPStatus.OK, {"outro": "x"})
    ):
        result = cliente_siorg.get_estrutura_organizacional_resumida()

    assert result == []


def test_get_estrutura_resumida_non_ok_status(cliente_siorg: ClienteSiorg) -> None:
    with patch.object(
        cliente_siorg,
        "request",
        return_value=(HTTPStatus.NOT_FOUND, {"unidades": [{"codigoUnidade": "1"}]}),
    ):
        result = cliente_siorg.get_estrutura_organizacional_resumida()

    assert result is None


def test_get_estrutura_resumida_non_dict_data(cliente_siorg: ClienteSiorg) -> None:
    with patch.object(
        cliente_siorg, "request", return_value=(HTTPStatus.OK, ["nao-eh-dict"])
    ):
        result = cliente_siorg.get_estrutura_organizacional_resumida()

    assert result is None


# ---------------------------------------------------------------------------
# get_estrutura_organizacional_cargos
# ---------------------------------------------------------------------------
def test_get_estrutura_cargos_success(cliente_siorg: ClienteSiorg) -> None:
    unidade = {"codigoUnidade": "42", "nome": "Unidade X"}
    with patch.object(
        cliente_siorg,
        "request",
        return_value=(HTTPStatus.OK, {"unidade": unidade}),
    ) as mock_request:
        result = cliente_siorg.get_estrutura_organizacional_cargos(
            codigo_unidade="42"
        )

    assert result == unidade
    mock_request.assert_called_once_with(
        http.HTTPMethod.GET,
        "/instancias/consulta-unidade",
        params={"codigoUnidade": "42"},
        headers={"accept": "*/*"},
    )


def test_get_estrutura_cargos_no_params(cliente_siorg: ClienteSiorg) -> None:
    with patch.object(
        cliente_siorg,
        "request",
        return_value=(HTTPStatus.OK, {"unidade": {}}),
    ) as mock_request:
        cliente_siorg.get_estrutura_organizacional_cargos()

    assert mock_request.call_args.kwargs["params"] == {}


def test_get_estrutura_cargos_missing_unidade_key(
    cliente_siorg: ClienteSiorg,
) -> None:
    with patch.object(
        cliente_siorg, "request", return_value=(HTTPStatus.OK, {"outro": "x"})
    ):
        result = cliente_siorg.get_estrutura_organizacional_cargos("42")

    assert result == []


def test_get_estrutura_cargos_non_ok_status(cliente_siorg: ClienteSiorg) -> None:
    with patch.object(
        cliente_siorg,
        "request",
        return_value=(HTTPStatus.BAD_REQUEST, {"unidade": {"id": 1}}),
    ):
        result = cliente_siorg.get_estrutura_organizacional_cargos("42")

    assert result is None


def test_get_estrutura_cargos_non_dict_data(cliente_siorg: ClienteSiorg) -> None:
    with patch.object(
        cliente_siorg, "request", return_value=(HTTPStatus.OK, None)
    ):
        result = cliente_siorg.get_estrutura_organizacional_cargos("42")

    assert result is None


# ---------------------------------------------------------------------------
# get_cargos_funcao
# ---------------------------------------------------------------------------
def test_get_cargos_funcao_success(cliente_siorg: ClienteSiorg) -> None:
    tipos = [{"codigo": "DAS"}, {"codigo": "FCPE"}]
    with patch.object(
        cliente_siorg,
        "request",
        return_value=(HTTPStatus.OK, {"tipoCargoFuncao": tipos}),
    ) as mock_request:
        result = cliente_siorg.get_cargos_funcao()

    assert result == tipos
    mock_request.assert_called_once_with(http.HTTPMethod.GET, "/cargo-funcao")


def test_get_cargos_funcao_missing_key(cliente_siorg: ClienteSiorg) -> None:
    with patch.object(
        cliente_siorg, "request", return_value=(HTTPStatus.OK, {"outro": "x"})
    ):
        result = cliente_siorg.get_cargos_funcao()

    assert result == []


def test_get_cargos_funcao_non_ok_status(cliente_siorg: ClienteSiorg) -> None:
    with patch.object(
        cliente_siorg,
        "request",
        return_value=(HTTPStatus.INTERNAL_SERVER_ERROR, None),
    ):
        result = cliente_siorg.get_cargos_funcao()

    assert result is None


def test_get_cargos_funcao_non_dict_data(cliente_siorg: ClienteSiorg) -> None:
    with patch.object(
        cliente_siorg, "request", return_value=(HTTPStatus.OK, ["nao-eh-dict"])
    ):
        result = cliente_siorg.get_cargos_funcao()

    assert result is None
