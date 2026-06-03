import unittest
from unittest.mock import patch, call
import http

from cliente_deputados import ClienteDeputados


def _make_page_data(qtd: int, start_id: int = 1) -> dict:
    """
    Monta o payload completo que o endpoint /deputados retorna.
    Inclui todas as propriedades relevantes para validação de extração.
    """
    return {
        "dados": [
            {
                "id": i,
                "nome": f"Deputado {i}",
                "siglaPartido": "XYZ",
                "siglaUf": "SP",
                "urlFoto": f"https://example.com/foto/{i}.jpg",
                "email": f"dep{i}@camara.leg.br",
            }
            for i in range(start_id, start_id + qtd)
        ]
    }


def _extract_dados(qtd: int, start_id: int = 1) -> list:
    """
    Extrai apenas a lista 'dados' — equivale ao que get_deputados() retorna
    após parsear a resposta da API.
    """
    return _make_page_data(qtd, start_id)["dados"]


# Conjunto de erros HTTP reutilizado em todos os subtests
HTTP_ERROR_CODES = [
    http.HTTPStatus.BAD_REQUEST,
    http.HTTPStatus.UNAUTHORIZED,
    http.HTTPStatus.FORBIDDEN,
    http.HTTPStatus.NOT_FOUND,
    http.HTTPStatus.INTERNAL_SERVER_ERROR,
    http.HTTPStatus.SERVICE_UNAVAILABLE,
]


# Atributos de Classe e Inicialização
class TestClienteDeputadosAtributos(unittest.TestCase):

    def test_base_url(self):
        """BASE_URL aponta para a versão correta da API."""
        self.assertEqual(
            ClienteDeputados.BASE_URL,
            "https://dadosabertos.camara.leg.br/api/v2",
        )

    def test_base_header_accept_json(self):
        """Header padrão solicita JSON."""
        self.assertIn("accept", ClienteDeputados.BASE_HEADER)
        self.assertEqual(ClienteDeputados.BASE_HEADER["accept"], "application/json")

    def test_page_size(self):
        """PAGE_SIZE está configurado como 100."""
        self.assertEqual(ClienteDeputados.PAGE_SIZE, 100)

    def test_instancia_inicializa_sem_erros(self):
        """Cliente pode ser instanciado sem lançar exceções."""
        with self.assertLogs(level="INFO") as log:
            cliente = ClienteDeputados()
            self.assertIsInstance(cliente, ClienteDeputados)
        self.assertTrue(any("Initialized ClienteDeputados" in msg for msg in log.output))


# get_deputados


class TestGetDeputados(unittest.TestCase):

    def setUp(self):
        self.cliente = ClienteDeputados()

    # caminho feliz

    @patch.object(ClienteDeputados, "request")
    def test_sucesso_com_params_e_extracao_de_propriedades(self, mock_request):
        """
        Retorna lista, valida extração de propriedades (nome, id, partido, UF)
        e verifica que o endpoint e os parâmetros foram enviados corretamente.
        """
        mock_request.return_value = (http.HTTPStatus.OK, _make_page_data(2))

        resultado = self.cliente.get_deputados(siglaUf="SP", siglaPartido="PT")

        self.assertIsInstance(resultado, list)
        self.assertEqual(len(resultado), 2)
        self.assertEqual(resultado[0]["id"], 1)
        self.assertEqual(resultado[0]["nome"], "Deputado 1")
        self.assertEqual(resultado[0]["siglaPartido"], "XYZ")
        self.assertEqual(resultado[0]["siglaUf"], "SP")
        self.assertEqual(resultado[1]["id"], 2)
        mock_request.assert_called_once_with(
            http.HTTPMethod.GET,
            "/deputados",
            headers=self.cliente.BASE_HEADER,
            params={"siglaUf": "SP", "siglaPartido": "PT"},
        )

    @patch.object(ClienteDeputados, "request")
    def test_sucesso_sem_params(self, mock_request):
        """Sem kwargs, params é dict vazio e o endpoint é chamado corretamente."""
        mock_request.return_value = (http.HTTPStatus.OK, _make_page_data(1))

        resultado = self.cliente.get_deputados()

        self.assertEqual(len(resultado), 1)
        mock_request.assert_called_once_with(
            http.HTTPMethod.GET,
            "/deputados",
            headers=self.cliente.BASE_HEADER,
            params={},
        )

    @patch.object(ClienteDeputados, "request")
    def test_sucesso_dados_lista_vazia(self, mock_request):
        """Retorna [] quando 'dados' existe mas está vazio."""
        mock_request.return_value = (http.HTTPStatus.OK, {"dados": []})

        resultado = self.cliente.get_deputados()

        self.assertIsInstance(resultado, list)
        self.assertEqual(resultado, [])

    @patch.object(ClienteDeputados, "request")
    def test_sucesso_sem_chave_dados(self, mock_request):
        """Retorna [] (valor default do .get()) quando o JSON não tem a chave 'dados'."""
        mock_request.return_value = (http.HTTPStatus.OK, {"links": [], "meta": {}})

        resultado = self.cliente.get_deputados()

        self.assertEqual(resultado, [])

    # falhas de HTTP / tipo de dado

    @patch.object(ClienteDeputados, "request")
    def test_retorna_none_para_erro_500_e_gera_log(self, mock_request):
        """Retorna None para erro de servidor e verifica se o log foi gerado."""
        mock_request.return_value = (http.HTTPStatus.INTERNAL_SERVER_ERROR, None)

        with self.assertLogs(level="WARNING") as log:
            resultado = self.cliente.get_deputados()

        self.assertIsNone(resultado)
        self.assertTrue(
            any("Failed to fetch deputados with status" in msg for msg in log.output)
        )

    @patch.object(ClienteDeputados, "request")
    def test_retorna_none_quando_dado_nao_e_dict(self, mock_request):
        """Retorna None quando status 200 mas o corpo é string (ex: HTML de timeout)."""
        mock_request.return_value = (http.HTTPStatus.OK, "<html>Timeout</html>")

        self.assertIsNone(self.cliente.get_deputados())

    @patch.object(ClienteDeputados, "request")
    def test_retorna_none_quando_dado_e_none(self, mock_request):
        """Retorna None quando status 200 mas o corpo da resposta é None."""
        mock_request.return_value = (http.HTTPStatus.OK, None)

        self.assertIsNone(self.cliente.get_deputados())

    def test_multiplos_erros_http_retornam_none(self):
        """Retorna None para todos os códigos de erro HTTP relevantes."""
        for status_code in HTTP_ERROR_CODES:
            with self.subTest(status_code=status_code):
                with patch.object(
                    ClienteDeputados, "request", return_value=(status_code, None)
                ):
                    self.assertIsNone(
                        self.cliente.get_deputados(),
                        msg=f"Esperado None para {status_code}",
                    )


# get_all_deputados
class TestGetAllDeputados(unittest.TestCase):
    """
    Testa a lógica de paginação de get_all_deputados.
    Patches em get_deputados para isolar o comportamento do loop,
    uma vez que get_deputados já é testada em separado.
    """

    def setUp(self):
        self.cliente = ClienteDeputados()

    # quebra imediata do loop

    @patch.object(ClienteDeputados, "get_deputados")
    def test_primeira_pagina_vazia_retorna_lista_vazia(self, mock_get_dep):
        """Para imediatamente e retorna [] quando a 1ª página está vazia."""
        mock_get_dep.return_value = []

        resultado = self.cliente.get_all_deputados()

        self.assertEqual(resultado, [])
        mock_get_dep.assert_called_once()

    @patch.object(ClienteDeputados, "get_deputados")
    def test_primeira_pagina_none_retorna_lista_vazia(self, mock_get_dep):
        """
        None é falsy: o loop quebra e retorna [].
        Comportamento DIFERENTE de get_deputados_atuais, que retorna None.
        """
        mock_get_dep.return_value = None

        resultado = self.cliente.get_all_deputados()

        self.assertEqual(resultado, [])
        mock_get_dep.assert_called_once()

    # página única

    @patch.object(ClienteDeputados, "get_deputados")
    def test_pagina_unica_menor_que_page_size(self, mock_get_dep):
        """Encerra após a 1ª chamada quando len(dados) < PAGE_SIZE."""
        mock_get_dep.return_value = _extract_dados(50)

        resultado = self.cliente.get_all_deputados()

        self.assertEqual(len(resultado), 50)
        mock_get_dep.assert_called_once()

    # múltiplas páginas

    @patch.object(ClienteDeputados, "get_deputados")
    def test_pagina_cheia_seguida_de_vazia(self, mock_get_dep):
        """Itera para a 2ª página e encerra quando ela está vazia."""
        mock_get_dep.side_effect = [
            _extract_dados(100, start_id=1),
            [],
        ]

        resultado = self.cliente.get_all_deputados()

        self.assertEqual(len(resultado), 100)
        self.assertEqual(mock_get_dep.call_count, 2)

    @patch.object(ClienteDeputados, "get_deputados")
    def test_multiplas_paginas_completas_mais_pagina_parcial(self, mock_get_dep):
        """Concatena páginas completas + página parcial final."""
        mock_get_dep.side_effect = [
            _extract_dados(100, start_id=1),
            _extract_dados(100, start_id=101),
            _extract_dados(73, start_id=201),
        ]

        resultado = self.cliente.get_all_deputados()

        self.assertEqual(len(resultado), 273)
        self.assertEqual(mock_get_dep.call_count, 3)

    @patch.object(ClienteDeputados, "get_deputados")
    def test_none_em_pagina_intermediaria_retorna_dados_parciais(self, mock_get_dep):
        """
        None em página intermediária quebra o loop e retorna o que foi acumulado.
        """
        mock_get_dep.side_effect = [
            _extract_dados(100, start_id=1),
            None,
        ]

        resultado = self.cliente.get_all_deputados()

        self.assertEqual(len(resultado), 100)
        self.assertEqual(mock_get_dep.call_count, 2)

    # verificação de parâmetros

    @patch.object(ClienteDeputados, "get_deputados")
    def test_params_pagina_1_inclui_datainicio(self, mock_get_dep):
        """Garante que dataInicio='1823-01-01' e pagina=1 são enviados na 1ª chamada."""
        mock_get_dep.return_value = _extract_dados(30)

        self.cliente.get_all_deputados()

        mock_get_dep.assert_called_once_with(
            pagina=1,
            itens=self.cliente.PAGE_SIZE,
            dataInicio="1823-01-01",
        )

    @patch.object(ClienteDeputados, "get_deputados")
    def test_incremento_de_pagina_em_chamadas_consecutivas(self, mock_get_dep):
        """Verifica que pagina=1, 2, 3 são enviados em cada iteração do loop."""
        mock_get_dep.side_effect = [
            _extract_dados(100, start_id=1),
            _extract_dados(100, start_id=101),
            _extract_dados(1, start_id=201),
        ]

        self.cliente.get_all_deputados()

        mock_get_dep.assert_has_calls(
            [
                call(pagina=1, itens=self.cliente.PAGE_SIZE, dataInicio="1823-01-01"),
                call(pagina=2, itens=self.cliente.PAGE_SIZE, dataInicio="1823-01-01"),
                call(pagina=3, itens=self.cliente.PAGE_SIZE, dataInicio="1823-01-01"),
            ]
        )

    # integridade dos dados

    @patch.object(ClienteDeputados, "get_deputados")
    def test_dados_concatenados_na_ordem_correta(self, mock_get_dep):
        """IDs preservados e concatenados em ordem de paginação."""
        mock_get_dep.side_effect = [
            _extract_dados(100, start_id=1),
            _extract_dados(50, start_id=101),
        ]

        resultado = self.cliente.get_all_deputados()

        self.assertEqual(resultado[0]["id"], 1)
        self.assertEqual(resultado[99]["id"], 100)
        self.assertEqual(resultado[100]["id"], 101)
        self.assertEqual(resultado[-1]["id"], 150)


# get_deputados_atuais
class TestGetDeputadosAtuais(unittest.TestCase):
    """
    get_deputados_atuais distingue None (falha de API) de [] (fim de dados)
    com um 'is None' explícito — comportamento crítico diferente de get_all_deputados.
    """

    def setUp(self):
        self.cliente = ClienteDeputados()

    # quebra por None (falha de API)

    @patch.object(ClienteDeputados, "get_deputados")
    def test_primeira_pagina_none_retorna_none_e_gera_log(self, mock_get_dep):
        """
        Retorna None imediatamente e testa se o log de erro foi gravado garantindo
        comportamento de aborto de snapshot.
        """
        mock_get_dep.return_value = None

        with self.assertLogs(level="ERROR") as log:
            resultado = self.cliente.get_deputados_atuais()

        self.assertIsNone(resultado)
        mock_get_dep.assert_called_once()
        self.assertTrue(
            any(
                "Falha ao buscar deputados atuais na pagina=1" in msg
                for msg in log.output
            )
        )

    @patch.object(ClienteDeputados, "get_deputados")
    def test_none_em_pagina_intermediaria_retorna_none(self, mock_get_dep):
        """Aborta e retorna None se a falha de API ocorrer em página posterior."""
        mock_get_dep.side_effect = [
            _extract_dados(100, start_id=1),
            None,
        ]

        with self.assertLogs(level="ERROR") as log:
            resultado = self.cliente.get_deputados_atuais()

        self.assertIsNone(resultado)
        self.assertEqual(mock_get_dep.call_count, 2)
        self.assertTrue(
            any(
                "Falha ao buscar deputados atuais na pagina=2" in msg
                for msg in log.output
            )
        )

    # quebra por lista vazia (fim de dados)

    @patch.object(ClienteDeputados, "get_deputados")
    def test_primeira_pagina_vazia_retorna_lista_vazia(self, mock_get_dep):
        """Retorna [] quando a API responde com lista vazia (sem deputados)."""
        mock_get_dep.return_value = []

        resultado = self.cliente.get_deputados_atuais()

        self.assertEqual(resultado, [])
        mock_get_dep.assert_called_once()

    @patch.object(ClienteDeputados, "get_deputados")
    def test_pagina_cheia_seguida_de_vazia(self, mock_get_dep):
        """Itera para 2ª página e encerra quando ela retorna []."""
        mock_get_dep.side_effect = [
            _extract_dados(100, start_id=1),
            [],
        ]

        resultado = self.cliente.get_deputados_atuais()

        self.assertEqual(len(resultado), 100)
        self.assertEqual(mock_get_dep.call_count, 2)

    # página única

    @patch.object(ClienteDeputados, "get_deputados")
    def test_pagina_unica_menor_que_page_size(self, mock_get_dep):
        """Encerra após a 1ª chamada quando len(dados) < PAGE_SIZE."""
        mock_get_dep.return_value = _extract_dados(30)

        resultado = self.cliente.get_deputados_atuais()

        self.assertEqual(len(resultado), 30)
        mock_get_dep.assert_called_once()

    # múltiplas páginas

    @patch.object(ClienteDeputados, "get_deputados")
    def test_multiplas_paginas_completas_mais_pagina_parcial(self, mock_get_dep):
        """Concatena múltiplas páginas e para na primeira parcial."""
        mock_get_dep.side_effect = [
            _extract_dados(100, start_id=1),
            _extract_dados(100, start_id=101),
            _extract_dados(30, start_id=201),
        ]

        resultado = self.cliente.get_deputados_atuais()

        self.assertEqual(len(resultado), 230)
        self.assertEqual(mock_get_dep.call_count, 3)

    # verificação de parâmetros

    @patch.object(ClienteDeputados, "get_deputados")
    def test_params_nao_incluem_datainicio(self, mock_get_dep):
        """Garante que dataInicio NÃO está nos parâmetros."""
        mock_get_dep.return_value = _extract_dados(10)

        self.cliente.get_deputados_atuais()

        args, kwargs = mock_get_dep.call_args
        self.assertNotIn("dataInicio", kwargs)
        mock_get_dep.assert_called_once_with(
            pagina=1,
            itens=self.cliente.PAGE_SIZE,
        )

    @patch.object(ClienteDeputados, "get_deputados")
    def test_incremento_de_pagina_em_chamadas_consecutivas(self, mock_get_dep):
        """Verifica que pagina=1, 2, 3 são enviados sem dataInicio."""
        mock_get_dep.side_effect = [
            _extract_dados(100, start_id=1),
            _extract_dados(100, start_id=101),
            _extract_dados(5, start_id=201),
        ]

        self.cliente.get_deputados_atuais()

        mock_get_dep.assert_has_calls(
            [
                call(pagina=1, itens=self.cliente.PAGE_SIZE),
                call(pagina=2, itens=self.cliente.PAGE_SIZE),
                call(pagina=3, itens=self.cliente.PAGE_SIZE),
            ]
        )

    # integridade dos dados

    @patch.object(ClienteDeputados, "get_deputados")
    def test_dados_concatenados_na_ordem_correta(self, mock_get_dep):
        """IDs preservados e concatenados em ordem de paginação."""
        mock_get_dep.side_effect = [
            _extract_dados(100, start_id=1),
            _extract_dados(50, start_id=101),
        ]

        resultado = self.cliente.get_deputados_atuais()

        self.assertEqual(resultado[0]["id"], 1)
        self.assertEqual(resultado[99]["id"], 100)
        self.assertEqual(resultado[100]["id"], 101)
        self.assertEqual(resultado[-1]["id"], 150)


# get_historico_deputado


class TestGetHistoricoDeputado(unittest.TestCase):

    def setUp(self):
        self.cliente = ClienteDeputados()

    # caminho feliz

    @patch.object(ClienteDeputados, "request")
    def test_sucesso_lista_com_extracao_de_props(self, mock_request):
        """
        Retorna lista de registros históricos e verifica extração
        de propriedades além de chamar o endpoint correto.
        """
        mock_data = {
            "dados": [
                {
                    "idLegislatura": 55,
                    "nomeParlamentar": "Deputado X",
                    "siglaPartido": "PT",
                },
                {
                    "idLegislatura": 56,
                    "nomeParlamentar": "Deputado X",
                    "siglaPartido": "PSB",
                },
            ]
        }
        mock_request.return_value = (http.HTTPStatus.OK, mock_data)

        resultado = self.cliente.get_historico_deputado(42)

        self.assertIsInstance(resultado, list)
        self.assertEqual(len(resultado), 2)
        self.assertEqual(resultado[0]["idLegislatura"], 55)
        self.assertEqual(resultado[0]["siglaPartido"], "PT")
        self.assertEqual(resultado[1]["idLegislatura"], 56)
        mock_request.assert_called_once_with(
            http.HTTPMethod.GET,
            "/deputados/42/historico",
            headers=self.cliente.BASE_HEADER,
        )

    @patch.object(ClienteDeputados, "request")
    def test_sucesso_dict_unico_empacotado_em_lista(self, mock_request):
        """Converte dict único em lista de um elemento."""
        mock_data = {"dados": {"idLegislatura": 56, "nomeParlamentar": "Dep Z"}}
        mock_request.return_value = (http.HTTPStatus.OK, mock_data)

        resultado = self.cliente.get_historico_deputado(99)

        self.assertIsInstance(resultado, list)
        self.assertEqual(len(resultado), 1)
        self.assertEqual(resultado[0]["nomeParlamentar"], "Dep Z")

    @patch.object(ClienteDeputados, "request")
    def test_sucesso_lista_vazia(self, mock_request):
        """Retorna [] quando 'dados' existe mas está vazio (deputado sem histórico)."""
        mock_request.return_value = (http.HTTPStatus.OK, {"dados": []})

        resultado = self.cliente.get_historico_deputado(42)

        self.assertIsInstance(resultado, list)
        self.assertEqual(resultado, [])

    @patch.object(ClienteDeputados, "request")
    def test_sucesso_sem_chave_dados_retorna_lista_vazia(self, mock_request):
        """Retorna [] (default do .get()) quando o JSON não tem a chave 'dados'."""
        mock_request.return_value = (http.HTTPStatus.OK, {"links": []})

        resultado = self.cliente.get_historico_deputado(42)

        self.assertEqual(resultado, [])

    # tipo de dado inesperado

    @patch.object(ClienteDeputados, "request")
    def test_dados_tipo_invalido_retorna_none_e_log(self, mock_request):
        """
        Retorna None quando 'dados' não é nem list nem dict —
        nenhum branch é satisfeito, aciona o warning e cai no return None final.
        """
        mock_request.return_value = (http.HTTPStatus.OK, {"dados": "string_inesperada"})

        with self.assertLogs(level="WARNING") as log:
            resultado = self.cliente.get_historico_deputado(42)

        self.assertIsNone(resultado)
        self.assertTrue(any("Failed to fetch historico" in msg for msg in log.output))

    # id como string

    @patch.object(ClienteDeputados, "request")
    def test_deputado_id_como_string_constroi_endpoint_correto(self, mock_request):
        """Aceita deputado_id como string e insere no endpoint corretamente."""
        mock_request.return_value = (http.HTTPStatus.OK, {"dados": []})

        self.cliente.get_historico_deputado("5952")

        mock_request.assert_called_once_with(
            http.HTTPMethod.GET,
            "/deputados/5952/historico",
            headers=self.cliente.BASE_HEADER,
        )

    # falhas de HTTP / tipo de dado

    @patch.object(ClienteDeputados, "request")
    def test_retorna_none_quando_dado_nao_e_dict(self, mock_request):
        """Retorna None quando status 200 mas o corpo é string (ex: HTML de timeout)."""
        mock_request.return_value = (http.HTTPStatus.OK, "<html>Timeout</html>")

        self.assertIsNone(self.cliente.get_historico_deputado(42))

    @patch.object(ClienteDeputados, "request")
    def test_retorna_none_para_erro_404(self, mock_request):
        """Retorna None quando o deputado não é encontrado."""
        mock_request.return_value = (http.HTTPStatus.NOT_FOUND, None)

        self.assertIsNone(self.cliente.get_historico_deputado(10))

    def test_multiplos_erros_http_retornam_none(self):
        """Retorna None para todos os códigos de erro HTTP relevantes."""
        for status_code in HTTP_ERROR_CODES:
            with self.subTest(status_code=status_code):
                with patch.object(
                    ClienteDeputados, "request", return_value=(status_code, None)
                ):
                    self.assertIsNone(
                        self.cliente.get_historico_deputado(42),
                        msg=f"Esperado None para {status_code}",
                    )


if __name__ == "__main__":
    unittest.main(verbosity=2)
