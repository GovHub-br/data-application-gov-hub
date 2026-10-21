import http
from http import HTTPStatus
from unittest.mock import Mock, patch

import pytest

from cliente_ted import ClienteTed


@pytest.fixture
def cliente_ted() -> ClienteTed:
    with patch("cliente_base.httpx.Client"):
        return ClienteTed()


def test_init_sets_base_url() -> None:
    with patch("cliente_base.httpx.Client") as mock_client:
        cliente = ClienteTed()

    assert cliente.base_url == ClienteTed.BASE_URL
    mock_client.assert_called_once_with(
        base_url=ClienteTed.BASE_URL, headers=None
    )


# ---------------------------------------------------------------------------
# get_ted_by_programa_beneficiario
# ---------------------------------------------------------------------------
def test_get_ted_by_programa_beneficiario_success(cliente_ted: ClienteTed) -> None:
    expected = [{"id_programa": 1}]
    with patch.object(
        cliente_ted, "request", return_value=(HTTPStatus.OK, expected)
    ) as mock_request:
        result = cliente_ted.get_ted_by_programa_beneficiario("123")

    assert result == expected
    mock_request.assert_called_once_with(
        http.HTTPMethod.GET,
        "programa_beneficiario?tx_codigo_siorg=eq.123",
        headers=ClienteTed.BASE_HEADER,
    )


def test_get_ted_by_programa_beneficiario_non_ok_status(
    cliente_ted: ClienteTed,
) -> None:
    with patch.object(
        cliente_ted, "request", return_value=(HTTPStatus.NOT_FOUND, [])
    ):
        result = cliente_ted.get_ted_by_programa_beneficiario("123")

    assert result is None


def test_get_ted_by_programa_beneficiario_non_list_data(
    cliente_ted: ClienteTed,
) -> None:
    with patch.object(
        cliente_ted, "request", return_value=(HTTPStatus.OK, {"erro": "x"})
    ):
        result = cliente_ted.get_ted_by_programa_beneficiario("123")

    assert result is None


# ---------------------------------------------------------------------------
# get_programa_by_id_programa
# ---------------------------------------------------------------------------
def test_get_programa_by_id_programa_success(cliente_ted: ClienteTed) -> None:
    expected = [{"id_programa": 99}]
    with patch.object(
        cliente_ted, "request", return_value=(HTTPStatus.OK, expected)
    ) as mock_request:
        result = cliente_ted.get_programa_by_id_programa("99")

    assert result == expected
    mock_request.assert_called_once_with(
        http.HTTPMethod.GET,
        "programa?id_programa=eq.99",
        headers=ClienteTed.BASE_HEADER,
    )


def test_get_programa_by_id_programa_failure(cliente_ted: ClienteTed) -> None:
    with patch.object(
        cliente_ted, "request", return_value=(HTTPStatus.BAD_REQUEST, None)
    ):
        result = cliente_ted.get_programa_by_id_programa("99")

    assert result is None


# ---------------------------------------------------------------------------
# get_planos_acao_by_id_programa
# ---------------------------------------------------------------------------
def test_get_planos_acao_by_id_programa_success(cliente_ted: ClienteTed) -> None:
    expected = [{"id_plano_acao": 5}]
    with patch.object(
        cliente_ted, "request", return_value=(HTTPStatus.OK, expected)
    ) as mock_request:
        result = cliente_ted.get_planos_acao_by_id_programa("7")

    assert result == expected
    mock_request.assert_called_once_with(
        http.HTTPMethod.GET,
        "plano_acao?id_programa=eq.7",
        headers=ClienteTed.BASE_HEADER,
    )


def test_get_planos_acao_by_id_programa_failure(cliente_ted: ClienteTed) -> None:
    with patch.object(
        cliente_ted, "request", return_value=(HTTPStatus.OK, "nao-eh-lista")
    ):
        result = cliente_ted.get_planos_acao_by_id_programa("7")

    assert result is None


# ---------------------------------------------------------------------------
# get_programas_by_sigla_unidade_descentralizadora
# ---------------------------------------------------------------------------
def test_get_programas_by_sigla_success(cliente_ted: ClienteTed) -> None:
    expected = [{"sigla_unidade_descentralizadora": "MEC"}]
    with patch.object(
        cliente_ted, "request", return_value=(HTTPStatus.OK, expected)
    ) as mock_request:
        result = cliente_ted.get_programas_by_sigla_unidade_descentralizadora("MEC")

    assert result == expected
    mock_request.assert_called_once_with(
        http.HTTPMethod.GET,
        "programa?sigla_unidade_descentralizadora=eq.MEC",
        headers=ClienteTed.BASE_HEADER,
    )


def test_get_programas_by_sigla_failure(cliente_ted: ClienteTed) -> None:
    with patch.object(
        cliente_ted, "request", return_value=(HTTPStatus.FORBIDDEN, [])
    ):
        result = cliente_ted.get_programas_by_sigla_unidade_descentralizadora("MEC")

    assert result is None


# ---------------------------------------------------------------------------
# get_notas_de_credito_by_id_plano_acao
# ---------------------------------------------------------------------------
def test_get_notas_de_credito_success(cliente_ted: ClienteTed) -> None:
    expected = [{"id_nota_credito": 1}]
    with patch.object(
        cliente_ted, "request", return_value=(HTTPStatus.OK, expected)
    ) as mock_request:
        result = cliente_ted.get_notas_de_credito_by_id_plano_acao(42)

    assert result == expected
    mock_request.assert_called_once_with(
        http.HTTPMethod.GET,
        "nota_credito?id_plano_acao=eq.42",
        headers=ClienteTed.BASE_HEADER,
    )


def test_get_notas_de_credito_failure(cliente_ted: ClienteTed) -> None:
    with patch.object(
        cliente_ted,
        "request",
        return_value=(HTTPStatus.INTERNAL_SERVER_ERROR, None),
    ):
        result = cliente_ted.get_notas_de_credito_by_id_plano_acao(42)

    assert result is None


# ---------------------------------------------------------------------------
# get_programacao_financeira_by_id_plano_acao
# ---------------------------------------------------------------------------
def test_get_programacao_financeira_success(cliente_ted: ClienteTed) -> None:
    expected = [{"id_programacao_financeira": 3}]
    with patch.object(
        cliente_ted, "request", return_value=(HTTPStatus.OK, expected)
    ) as mock_request:
        result = cliente_ted.get_programacao_financeira_by_id_plano_acao(8)

    assert result == expected
    mock_request.assert_called_once_with(
        http.HTTPMethod.GET,
        "programacao_financeira?id_plano_acao=eq.8",
        headers=ClienteTed.BASE_HEADER,
    )


def test_get_programacao_financeira_failure(cliente_ted: ClienteTed) -> None:
    with patch.object(
        cliente_ted, "request", return_value=(HTTPStatus.OK, {"nao": "lista"})
    ):
        result = cliente_ted.get_programacao_financeira_by_id_plano_acao(8)

    assert result is None


# ---------------------------------------------------------------------------
# get_todos_programas
# ---------------------------------------------------------------------------
def test_get_todos_programas_success(cliente_ted: ClienteTed) -> None:
    expected = [{"id_programa": 1}, {"id_programa": 2}]
    with patch.object(
        cliente_ted, "request", return_value=(HTTPStatus.OK, expected)
    ) as mock_request:
        result = cliente_ted.get_todos_programas(limit=500, offset=1000)

    assert result == expected
    mock_request.assert_called_once_with(
        http.HTTPMethod.GET,
        "programa",
        headers={
            **ClienteTed.BASE_HEADER,
            "Range-Unit": "items",
            "Range": "1000-1499",
        },
    )


def test_get_todos_programas_default_pagination(cliente_ted: ClienteTed) -> None:
    with patch.object(
        cliente_ted, "request", return_value=(HTTPStatus.OK, [])
    ) as mock_request:
        cliente_ted.get_todos_programas()

    assert mock_request.call_args.kwargs["headers"]["Range"] == "0-999"


def test_get_todos_programas_failure(cliente_ted: ClienteTed) -> None:
    with patch.object(
        cliente_ted,
        "request",
        return_value=(HTTPStatus.SERVICE_UNAVAILABLE, None),
    ):
        result = cliente_ted.get_todos_programas()

    assert result is None


def test_get_todos_programas_non_list_data(cliente_ted: ClienteTed) -> None:
    with patch.object(
        cliente_ted, "request", return_value=(HTTPStatus.OK, {"x": 1})
    ):
        result = cliente_ted.get_todos_programas()

    assert result is None


# ---------------------------------------------------------------------------
# get_all_programas
# ---------------------------------------------------------------------------
def test_get_all_programas_single_partial_page(cliente_ted: ClienteTed) -> None:
    page = [{"id": 1}, {"id": 2}]
    with patch.object(
        cliente_ted, "get_todos_programas", return_value=page
    ) as mock_get:
        result = cliente_ted.get_all_programas(limit=10)

    assert result == page
    mock_get.assert_called_once_with(limit=10, offset=0)


def test_get_all_programas_multiple_pages(cliente_ted: ClienteTed) -> None:
    first_page = [{"id": i} for i in range(2)]
    second_page = [{"id": 99}]
    with patch.object(
        cliente_ted,
        "get_todos_programas",
        side_effect=[first_page, second_page],
    ) as mock_get:
        result = cliente_ted.get_all_programas(limit=2)

    assert result == first_page + second_page
    assert mock_get.call_args_list == [
        ((), {"limit": 2, "offset": 0}),
        ((), {"limit": 2, "offset": 2}),
    ]


def test_get_all_programas_stops_on_empty_page(cliente_ted: ClienteTed) -> None:
    full_page = [{"id": 1}, {"id": 2}]
    with patch.object(
        cliente_ted,
        "get_todos_programas",
        side_effect=[full_page, []],
    ) as mock_get:
        result = cliente_ted.get_all_programas(limit=2)

    assert result == full_page
    assert mock_get.call_count == 2


def test_get_all_programas_first_page_none(cliente_ted: ClienteTed) -> None:
    with patch.object(
        cliente_ted, "get_todos_programas", return_value=None
    ) as mock_get:
        result = cliente_ted.get_all_programas()

    assert result == []
    mock_get.assert_called_once_with(limit=1000, offset=0)
