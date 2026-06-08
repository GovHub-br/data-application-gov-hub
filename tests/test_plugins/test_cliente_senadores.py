import http
from unittest.mock import MagicMock, patch




def _make_client():
    """Instantiate ClienteSenadores without network calls."""
    with patch("cliente_senadores.ClienteBase.__init__", return_value=None):
        from cliente_senadores import ClienteSenadores
        client = ClienteSenadores.__new__(ClienteSenadores)
        client.request = MagicMock()
        return client


# ---------------------------------------------------------------------------
# get_senadores_atuais
# ---------------------------------------------------------------------------

class TestGetSenadoresAtuais:

    # Verifica se uma lista de senadores é retornada corretamente 
    # quando a API responde com sucesso.
    def test_returns_list_on_success(self):
        client = _make_client()
        parlamentar = {"IdentificacaoParlamentar": {"NomeParlamentar": "Senador A"}}
        client.request.return_value = (
            http.HTTPStatus.OK,
            {
                "ListaParlamentarEmExercicio": {
                    "Parlamentares": {"Parlamentar": [parlamentar]}
                }
            },
        )

        result = client.get_senadores_atuais()

        assert result == [parlamentar]


    # Garante que um único senador vindo como dicionário seja transformado em lista.
    def test_wraps_single_dict_in_list(self):
        client = _make_client()
        parlamentar = {"IdentificacaoParlamentar": {"NomeParlamentar": "Senador Único"}}
        client.request.return_value = (
            http.HTTPStatus.OK,
            {
                "ListaParlamentarEmExercicio": {
                    "Parlamentares": {"Parlamentar": parlamentar}  # dict, not list
                }
            },
        )

        result = client.get_senadores_atuais()

        assert result == [parlamentar]

    # Se a API retornar erro HTTP, o método deve devolver uma lista vazia.
    def test_returns_empty_list_on_http_error(self):
        client = _make_client()
        client.request.return_value = (http.HTTPStatus.INTERNAL_SERVER_ERROR, {})

        result = client.get_senadores_atuais()

        assert result == []

    # Se a resposta não for um dicionário válido, retorna lista vazia.
    def test_returns_empty_list_on_non_dict_response(self):
        client = _make_client()
        client.request.return_value = (http.HTTPStatus.OK, None)

        result = client.get_senadores_atuais()

        assert result == []

    # Verifica se o método lida bem com respostas fora do formato esperado.
    def test_returns_empty_list_on_malformed_json(self):
        client = _make_client()
        client.request.return_value = (http.HTTPStatus.OK, {"UnexpectedKey": {}})

        result = client.get_senadores_atuais()

        assert result == []

    # Confere se o endpoint correto está sendo chamado.
    def test_calls_correct_endpoint(self):
        client = _make_client()
        client.request.return_value = (
    http.HTTPStatus.OK,
    {
        "ListaParlamentarEmExercicio": {
            "Parlamentares": {"Parlamentar": []}
        }
    },
)

        client.get_senadores_atuais()

        args, kwargs = client.request.call_args
        assert args[1] == "/senador/lista/atual"


# ---------------------------------------------------------------------------
# get_senadores_por_legislatura
# ---------------------------------------------------------------------------

class TestGetSenadoresPorLegislatura:
    
    # Verifica se a lista de senadores por legislatura é retornada corretamente.
    def test_returns_list_on_success(self):
        client = _make_client()
        parlamentar = {"IdentificacaoParlamentar": {"NomeParlamentar": "Senador B"}}
        client.request.return_value = (
            http.HTTPStatus.OK,
            {
                "ListaParlamentarLegislatura": {
                    "Parlamentares": {"Parlamentar": [parlamentar]}
                }
            },
        )

        result = client.get_senadores_por_legislatura()

        assert result == [parlamentar]

    # Garante que um único senador retornado como dicionário vire uma lista.
    def test_wraps_single_dict_in_list(self):
        client = _make_client()
        parlamentar = {"IdentificacaoParlamentar": {"NomeParlamentar": "Senador Único"}}
        client.request.return_value = (
            http.HTTPStatus.OK,
            {
                "ListaParlamentarLegislatura": {
                    "Parlamentares": {"Parlamentar": parlamentar}
                }
            },
        )

        result = client.get_senadores_por_legislatura()

        assert result == [parlamentar]

    # Deve retornar lista vazia quando a API responder com erro.
    def test_returns_empty_list_on_http_error(self):
        client = _make_client()
        client.request.return_value = (http.HTTPStatus.NOT_FOUND, {})

        result = client.get_senadores_por_legislatura()

        assert result == []

    # Confere se a URL usada na requisição está correta.
    def test_calls_correct_endpoint(self):
        client = _make_client()
        client.request.return_value = (
            http.HTTPStatus.OK,
            {"ListaParlamentarLegislatura": {"Parlamentares": {"Parlamentar": []}}},
        )

        client.get_senadores_por_legislatura()

        args, _ = client.request.call_args
        assert args[1] == "/senador/lista/legislatura/0/100"


# ---------------------------------------------------------------------------
# get_periodo_legislacao
# ---------------------------------------------------------------------------

class TestGetPeriodoLegislacao:

    # Verifica se a lista de legislaturas é retornada corretamente.
    def test_returns_list_on_success(self):
        client = _make_client()
        legislatura = {"NumeroLegislatura": "56", "DataInicio": "2019-02-01"}
        client.request.return_value = (
            http.HTTPStatus.OK,
            {
                "ListaLegislatura": {
                    "Legislaturas": {"Legislatura": [legislatura]}
                }
            },
        )

        result = client.get_periodo_legislacao()

        assert result == [legislatura]

    # Garante que uma única legislatura seja convertida para lista.
    def test_wraps_single_dict_in_list(self):
        client = _make_client()
        legislatura = {"NumeroLegislatura": "56"}
        client.request.return_value = (
            http.HTTPStatus.OK,
            {"ListaLegislatura": {"Legislaturas": {"Legislatura": legislatura}}},
        )

        result = client.get_periodo_legislacao()

        assert result == [legislatura]

    # Deve retornar lista vazia quando houver erro na requisição.
    def test_returns_empty_list_on_http_error(self):
        client = _make_client()
        client.request.return_value = (http.HTTPStatus.SERVICE_UNAVAILABLE, {})

        result = client.get_periodo_legislacao()

        assert result == []

    # Verifica se o endpoint da API está correto.
    def test_calls_correct_endpoint(self):
        client = _make_client()
        client.request.return_value = (
            http.HTTPStatus.OK,
            {"ListaLegislatura": {"Legislaturas": {"Legislatura": []}}},
        )

        client.get_periodo_legislacao()

        args, _ = client.request.call_args
        assert args[1] == "/dados/ListaLegislatura.json"


# ---------------------------------------------------------------------------
# get_filiacoes_senador
# ---------------------------------------------------------------------------

class TestGetFiliacoesSenador:
    SENADOR_ID = 5529

    def _filiacao_payload(self, filiacao):
        return (
            http.HTTPStatus.OK,
            {
                "FiliacaoParlamentar": {
                    "Parlamentar": {
                        "Filiacoes": {"Filiacao": filiacao}
                    }
                }
            },
        )

    # Verifica se a lista de filiações é retornada corretamente.
    def test_returns_list_on_success(self):
        client = _make_client()
        filiacao = {"Partido": {"SiglaPartido": "PT"}, "DataFiliacao": "2002-01-01"}
        client.request.return_value = self._filiacao_payload([filiacao])

        result = client.get_filiacoes_senador(self.SENADOR_ID)

        assert result == [filiacao]

    # Garante que uma única filiação seja convertida para lista.
    def test_wraps_single_dict_in_list(self):
        client = _make_client()
        filiacao = {"Partido": {"SiglaPartido": "MDB"}}
        client.request.return_value = self._filiacao_payload(filiacao)

        result = client.get_filiacoes_senador(self.SENADOR_ID)

        assert result == [filiacao]

    # Deve retornar None quando a lista de filiações estiver vazia.
    def test_returns_none_for_empty_filiacao_list(self):
        client = _make_client()
        client.request.return_value = self._filiacao_payload([])

        result = client.get_filiacoes_senador(self.SENADOR_ID)

        assert result is None

    # Em caso de erro HTTP, também deve retornar None.
    def test_returns_none_on_http_error(self):
        client = _make_client()
        client.request.return_value = (http.HTTPStatus.NOT_FOUND, {})

        result = client.get_filiacoes_senador(self.SENADOR_ID)

        assert result is None

    # Verifica se o método aceita o ID do senador como string.
    def test_accepts_string_senador_id(self):
        client = _make_client()
        filiacao = {"Partido": {"SiglaPartido": "PSDB"}}
        client.request.return_value = self._filiacao_payload([filiacao])

        result = client.get_filiacoes_senador("5529")

        assert result == [filiacao]

    # Confere se a URL utilizada para buscar filiações está correta.
    def test_calls_correct_endpoint(self):
        client = _make_client()
        client.request.return_value = self._filiacao_payload([])

        client.get_filiacoes_senador(self.SENADOR_ID)

        args, _ = client.request.call_args
        assert args[1] == f"/senador/{self.SENADOR_ID}/filiacoes"

    # Algumas respostas usam outra chave principal; o método deve aceitar ambas.
    def test_accepts_alternate_root_key(self):
        """Some responses use ListaFiliacoesParlamentar instead of FiliacaoParlamentar."""
        client = _make_client()
        filiacao = {"Partido": {"SiglaPartido": "PL"}}
        client.request.return_value = (
            http.HTTPStatus.OK,
            {
                "ListaFiliacoesParlamentar": {
                    "Parlamentar": {
                        "Filiacoes": {"Filiacao": [filiacao]}
                    }
                }
            },
        )

        result = client.get_filiacoes_senador(self.SENADOR_ID)

        assert result == [filiacao]
