import http
from http import HTTPStatus
from unittest.mock import Mock, patch

import pytest

from cliente_ted import ClienteTed, numeros_como_texto


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


# ---------------------------------------------------------------------------
# Endpoints filtrados por id (metas, etapas, termos, analises, pareceres,
# eventos, TRF, acoes orcamentarias e beneficiarios)
# ---------------------------------------------------------------------------
ENDPOINTS_POR_ID = [
    ("get_metas_by_id_plano_acao", "plano_acao_meta?id_plano_acao=eq.7"),
    ("get_etapas_by_id_meta", "plano_acao_etapa?id_meta=eq.7"),
    ("get_termos_execucao_by_id_plano_acao", "termo_execucao?id_plano_acao=eq.7"),
    ("get_analises_by_id_plano_acao", "plano_acao_analise?id_plano_acao=eq.7"),
    ("get_pareceres_by_id_plano_acao", "plano_acao_parecer?id_plano_acao=eq.7"),
    ("get_eventos_by_id_nota", "evento?id_nota=eq.7"),
    ("get_trf_by_id_programacao", "trf?id_programacao=eq.7"),
    (
        "get_acoes_orcamentarias_by_id_programa",
        "programa_acao_orcamentaria?id_programa=eq.7",
    ),
    ("get_beneficiarios_by_id_programa", "programa_beneficiario?id_programa=eq.7"),
]


@pytest.mark.parametrize("metodo, endpoint", ENDPOINTS_POR_ID)
def test_endpoint_por_id_success(
    cliente_ted: ClienteTed, metodo: str, endpoint: str
) -> None:
    expected = [{"id": 1}, {"id": 2}]
    with patch.object(
        cliente_ted, "request", return_value=(HTTPStatus.OK, expected)
    ) as mock_request:
        result = getattr(cliente_ted, metodo)(7)

    assert result == expected
    mock_request.assert_called_once_with(
        http.HTTPMethod.GET, endpoint, headers=ClienteTed.BASE_HEADER
    )


@pytest.mark.parametrize("metodo, endpoint", ENDPOINTS_POR_ID)
def test_endpoint_por_id_lista_vazia_nao_e_falha(
    cliente_ted: ClienteTed, metodo: str, endpoint: str
) -> None:
    with patch.object(cliente_ted, "request", return_value=(HTTPStatus.OK, [])):
        assert getattr(cliente_ted, metodo)(7) == []


@pytest.mark.parametrize("metodo, endpoint", ENDPOINTS_POR_ID)
@pytest.mark.parametrize(
    "resposta", [(HTTPStatus.INTERNAL_SERVER_ERROR, []), (HTTPStatus.OK, {"x": 1})]
)
def test_endpoint_por_id_falha_retorna_none(
    cliente_ted: ClienteTed, metodo: str, endpoint: str, resposta: tuple
) -> None:
    with patch.object(cliente_ted, "request", return_value=resposta):
        assert getattr(cliente_ted, metodo)(7) is None


# ---------------------------------------------------------------------------
# numeros_como_texto
# ---------------------------------------------------------------------------
def test_numeros_como_texto_mantem_inteiro_e_nulo() -> None:
    linhas = [
        {"aa_instrumento": 2024, "sq_instrumento": None, "vl": 650000.5, "ativo": True},
        {"aa_instrumento": 2023, "sq_instrumento": 7, "vl": 10, "ativo": None},
    ]

    assert numeros_como_texto(linhas) == [
        {
            "aa_instrumento": "2024",
            "sq_instrumento": None,
            "vl": "650000.5",
            "ativo": True,
        },
        {"aa_instrumento": "2023", "sq_instrumento": "7", "vl": "10", "ativo": None},
    ]


def test_numeros_como_texto_evita_float_do_pandas() -> None:
    # o ClientPostgresDB achata tudo junto no pandas: sem a conversao, a coluna que
    # mistura inteiro e nulo vira float ('2024.0' e 'NaN')
    from cliente_postgres import ClientPostgresDB

    linhas = [{"aa": 2024, "sq": None}, {"aa": None, "sq": 7}]
    achatado = ClientPostgresDB("dsn")._flatten_data(
        numeros_como_texto(linhas), preserva_nulos=True
    )

    assert achatado == [{"aa": "2024", "sq": None}, {"aa": None, "sq": "7"}]
