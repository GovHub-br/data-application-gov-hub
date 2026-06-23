import http
import typing
import pytest
from unittest.mock import patch, MagicMock
from cliente_pncp import ClientePNCP


class TestClientePNCP:
    # ------------------------------------------------------------------
    # get_contratacoes_publicacao()
    # ------------------------------------------------------------------

    def test_recusa_sem_data_inicial(self) -> None:
        cliente = ClientePNCP()

        with pytest.raises(TypeError):
            cliente.get_contratacoes_publicacao(  # type: ignore[call-arg]
                data_final="20230202",
                codigo_modalidade_contratacao=8,
            )

    def test_recusa_sem_data_final(self) -> None:
        cliente = ClientePNCP()

        with pytest.raises(TypeError):
            cliente.get_contratacoes_publicacao(  # type: ignore[call-arg]
                data_inicial="20230201",
                codigo_modalidade_contratacao=8,
            )

    def test_recusa_sem_codigo_modalidade_contratacao(self) -> None:
        cliente = ClientePNCP()

        with pytest.raises(TypeError):
            cliente.get_contratacoes_publicacao(  # type: ignore[call-arg]
                data_inicial="20230201",
                data_final="20230202",
            )

    @pytest.mark.parametrize(
        "status,body",
        [
            (http.HTTPStatus.NO_CONTENT, None),
            (
                http.HTTPStatus.BAD_REQUEST,
                {"erro": "Bad Request"},
            ),
            (
                http.HTTPStatus.UNAUTHORIZED,
                "Unauthorized",
            ),
            (
                http.HTTPStatus.UNPROCESSABLE_ENTITY,
                {"message": ("Data Inicial deve ser anterior ou igual à Data Final")},
            ),
            (
                http.HTTPStatus.INTERNAL_SERVER_ERROR,
                "Erro na comunicação com o banco de dados.",
            ),
        ],
    )
    @patch("cliente_pncp.request_safe")
    def test_status_diferente_de_200_retorna_tupla_vazia(
        self,
        mock_request_safe: MagicMock,
        status: int,
        body: str,
    ) -> None:
        mock_request_safe.return_value = (status, body)

        cliente = ClientePNCP()

        itens, total_paginas = cliente.get_contratacoes_publicacao(
            data_inicial="20230201",
            data_final="20230202",
            codigo_modalidade_contratacao=8,
        )

        assert itens == []
        assert total_paginas == 0

        mock_request_safe.assert_called_once_with(
            cliente,
            http.HTTPMethod.GET,
            "/consulta/v1/contratacoes/publicacao",
            headers={"accept": "application/json"},
            params={
                "dataInicial": "20230201",
                "dataFinal": "20230202",
                "pagina": 1,
                "codigoModalidadeContratacao": 8,
                "cnpj": None,
            },
        )

    @patch("cliente_pncp.request_safe")
    def test_retorna_lista_e_total_paginas(
        self,
        mock_request_safe: MagicMock,
    ) -> None:
        resposta = {
            "data": [
                {
                    "numeroControlePNCP": "88117718000103-1-000001/2023",
                    "objetoCompra": "Objeto 1",
                },
                {
                    "numeroControlePNCP": "34164319000174-1-000013/2023",
                    "objetoCompra": "Objeto 2",
                },
            ],
            "totalPaginas": 43,
            "totalRegistros": 429,
            "numeroPagina": 1,
        }

        mock_request_safe.return_value = (
            http.HTTPStatus.OK,
            resposta,
        )

        cliente = ClientePNCP()

        itens, total_paginas = cliente.get_contratacoes_publicacao(
            data_inicial="20230201",
            data_final="20230202",
            codigo_modalidade_contratacao=8,
        )

        assert isinstance(itens, list)
        assert isinstance(total_paginas, int)

        assert len(itens) == 2
        assert total_paginas == 43

        assert itens[0]["numeroControlePNCP"] == ("88117718000103-1-000001/2023")

        mock_request_safe.assert_called_once_with(
            cliente,
            http.HTTPMethod.GET,
            "/consulta/v1/contratacoes/publicacao",
            headers={"accept": "application/json"},
            params={
                "dataInicial": "20230201",
                "dataFinal": "20230202",
                "pagina": 1,
                "codigoModalidadeContratacao": 8,
                "cnpj": None,
            },
        )

    @patch("cliente_pncp.request_safe")
    def test_retorna_lista_quando_api_devolve_lista(
        self,
        mock_request_safe: MagicMock,
    ) -> None:
        resposta = [
            {"id": 1},
            {"id": 2},
        ]

        mock_request_safe.return_value = (
            http.HTTPStatus.OK,
            resposta,
        )

        cliente = ClientePNCP()

        itens, total_paginas = cliente.get_contratacoes_publicacao(
            data_inicial="20230201",
            data_final="20230202",
            codigo_modalidade_contratacao=8,
        )

        assert itens == resposta
        assert total_paginas == 0

    @patch("cliente_pncp.request_safe")
    def test_200_sem_data_retorna_lista_vazia(
        self,
        mock_request_safe: MagicMock,
    ) -> None:
        mock_request_safe.return_value = (
            http.HTTPStatus.OK,
            {
                "foo": "bar",
                "totalPaginas": 10,
            },
        )

        cliente = ClientePNCP()

        itens, total_paginas = cliente.get_contratacoes_publicacao(
            data_inicial="20230201",
            data_final="20230202",
            codigo_modalidade_contratacao=8,
        )

        assert itens == []
        assert total_paginas == 10

    @patch("cliente_pncp.request_safe")
    def test_200_com_corpo_invalido_retorna_vazio(
        self,
        mock_request_safe: MagicMock,
    ) -> None:
        mock_request_safe.return_value = (
            http.HTTPStatus.OK,
            "texto qualquer",
        )

        cliente = ClientePNCP()

        itens, total_paginas = cliente.get_contratacoes_publicacao(
            data_inicial="20230201",
            data_final="20230202",
            codigo_modalidade_contratacao=8,
        )

        assert itens == []
        assert total_paginas == 0

    @patch("cliente_pncp.request_safe")
    def test_resposta_lista_direta(self, mock_request_safe: MagicMock) -> None:
        resposta = [
            {"id": 1},
            {"id": 2},
        ]

        mock_request_safe.return_value = (
            http.HTTPStatus.OK,
            resposta,
        )

        cliente = ClientePNCP()

        itens, total_paginas = cliente.get_contratacoes_publicacao(
            "20230201",
            "20230202",
            8,
        )

        assert itens == resposta
        assert total_paginas == 0

    @patch("cliente_pncp.request_safe")
    def test_dict_sem_data(self, mock_request_safe: MagicMock) -> None:

        mock_request_safe.return_value = (
            http.HTTPStatus.OK,
            {
                "foo": "bar",
                "totalPaginas": 10,
            },
        )

        cliente = ClientePNCP()

        itens, paginas = cliente.get_contratacoes_publicacao(
            "20230201",
            "20230202",
            8,
        )

        assert itens == []
        assert paginas == 10

    @patch("cliente_pncp.request_safe")
    def test_data_nao_lista(self, mock_request_safe: MagicMock) -> None:

        mock_request_safe.return_value = (
            http.HTTPStatus.OK,
            {
                "data": {},
                "totalPaginas": 7,
            },
        )

        cliente = ClientePNCP()

        itens, paginas = cliente.get_contratacoes_publicacao(
            "20230201",
            "20230202",
            8,
        )

        assert itens == []
        assert paginas == 7

    @patch("cliente_pncp.request_safe")
    def test_resposta_string(self, mock_request_safe: MagicMock) -> None:

        mock_request_safe.return_value = (http.HTTPStatus.OK, "erro")

        cliente = ClientePNCP()

        itens, paginas = cliente.get_contratacoes_publicacao(
            "20230201",
            "20230202",
            8,
        )

        assert itens == []
        assert paginas == 0

    # ------------------------------------------------------------------
    # Paginação: get_contratacoes_publicacao_paginado()
    # ------------------------------------------------------------------

    @patch.object(ClientePNCP, "get_contratacoes_publicacao")
    def test_get_contratacoes_publicacao_paginado_retorna_lista_agregada(
        self,
        mock_get_contratacoes_publicacao: MagicMock,
    ) -> None:
        """Agrega registros de múltiplas páginas."""

        mock_get_contratacoes_publicacao.side_effect = [
            ([{"id": 1}, {"id": 2}], 3),
            ([{"id": 3}], 3),
            ([], 3),
        ]

        cliente = ClientePNCP()

        resultado = cliente.get_contratacoes_publicacao_paginado(
            data_inicial="20230201",
            data_final="20230202",
            codigo_modalidade_contratacao=8,
        )

        assert isinstance(resultado, list)
        assert resultado == [
            {"id": 1},
            {"id": 2},
            {"id": 3},
        ]

        assert mock_get_contratacoes_publicacao.call_count == 3

    @patch.object(ClientePNCP, "get_contratacoes_publicacao")
    def test_get_contratacoes_publicacao_paginado_respeita_max_paginas_argumento(
        self,
        mock_get_contratacoes_publicacao: MagicMock,
    ) -> None:
        mock_get_contratacoes_publicacao.side_effect = [
            ([{"id": 1}], 50),
            ([{"id": 2}], 50),
        ]

        cliente = ClientePNCP()

        resultado = cliente.get_contratacoes_publicacao_paginado(
            data_inicial="20230201",
            data_final="20230202",
            codigo_modalidade_contratacao=8,
            max_paginas=2,
        )

        assert resultado == [
            {"id": 1},
            {"id": 2},
        ]

        assert mock_get_contratacoes_publicacao.call_count == 2

    @patch.object(ClientePNCP, "get_contratacoes_publicacao")
    def test_get_contratacoes_publicacao_paginado_respeita_total_paginas_retorno(
        self,
        mock_get_contratacoes_publicacao: MagicMock,
    ) -> None:
        mock_get_contratacoes_publicacao.side_effect = [
            ([{"id": 1}], 2),
            ([{"id": 2}], 2),
        ]

        cliente = ClientePNCP()

        resultado = cliente.get_contratacoes_publicacao_paginado(
            data_inicial="20230201",
            data_final="20230202",
            codigo_modalidade_contratacao=8,
        )

        assert resultado == [
            {"id": 1},
            {"id": 2},
        ]

        assert mock_get_contratacoes_publicacao.call_count == 2

    @patch.object(ClientePNCP, "get_contratacoes_publicacao")
    def test_get_contratacoes_publicacao_paginado_para_quando_recebe_lista_vazia(
        self,
        mock_get_contratacoes_publicacao: MagicMock,
    ) -> None:
        """Encerra a paginação quando uma página não possui registros."""

        mock_get_contratacoes_publicacao.side_effect = [
            ([], 10),
        ]

        cliente = ClientePNCP()

        resultado = cliente.get_contratacoes_publicacao_paginado(
            data_inicial="20230201",
            data_final="20230202",
            codigo_modalidade_contratacao=8,
        )

        assert resultado == []

        mock_get_contratacoes_publicacao.assert_called_once()

    @patch.object(ClientePNCP, "get_contratacoes_publicacao")
    def test_get_contratacoes_publicacao_paginado_max_paginas_zero(
        self,
        mock_get_contratacoes_publicacao: MagicMock,
    ) -> None:
        """Não consulta nenhuma página quando max_paginas é zero."""

        cliente = ClientePNCP()

        resultado = cliente.get_contratacoes_publicacao_paginado(
            data_inicial="20230201",
            data_final="20230202",
            codigo_modalidade_contratacao=8,
            max_paginas=0,
        )

        assert resultado == []

        mock_get_contratacoes_publicacao.assert_not_called()

    @patch.object(ClientePNCP, "get_contratacoes_publicacao")
    def test_get_contratacoes_publicacao_paginado_propaga_excecao(
        self,
        mock_get_contratacoes_publicacao: MagicMock,
    ) -> None:
        """Propaga exceções levantadas durante a busca de páginas."""

        mock_get_contratacoes_publicacao.side_effect = RuntimeError("Erro na API")

        cliente = ClientePNCP()

        with pytest.raises(RuntimeError):
            cliente.get_contratacoes_publicacao_paginado(
                data_inicial="20230201",
                data_final="20230202",
                codigo_modalidade_contratacao=8,
            )

    @patch.object(ClientePNCP, "get_contratacoes_publicacao")
    def test_get_contratacoes_publicacao_paginado_respeita_pagina_inicial(
        self,
        mock_get_contratacoes_publicacao: MagicMock,
    ) -> None:
        """Inicia a coleta na página informada."""

        mock_get_contratacoes_publicacao.side_effect = [
            ([{"id": 10}], 5),
            ([], 5),
        ]

        cliente = ClientePNCP()

        cliente.get_contratacoes_publicacao_paginado(
            data_inicial="20230201",
            data_final="20230202",
            codigo_modalidade_contratacao=8,
            pagina_inicial=5,
        )

        primeira_chamada = mock_get_contratacoes_publicacao.call_args_list[0]

        assert primeira_chamada.kwargs["pagina"] == 5

    # ------------------------------------------------------------------
    # Semestral: get_contratacoes_publicacao_semestral
    # ------------------------------------------------------------------

    @patch.object(ClientePNCP, "get_contratacoes_publicacao_paginado")
    def test_get_contratacoes_publicacao_semestral_retorna_lista_agregada(
        self,
        mock_paginado: MagicMock,
    ) -> None:
        """
        Caminho feliz:
        agrega resultados de H1 e H2.
        """

        mock_paginado.side_effect = [
            [{"id": 1}],
            [{"id": 2}],
        ]

        cliente = ClientePNCP()

        resultado = cliente.get_contratacoes_publicacao_semestral(
            data_inicial="20230101",
            data_final="20231231",
            codigo_modalidade_contratacao=8,
        )

        assert isinstance(resultado, list)

        assert resultado == [
            {"id": 1},
            {"id": 2},
        ]

        assert mock_paginado.call_count == 2

    def test_get_contratacoes_publicacao_semestral_erro_parse_anos(self) -> None:
        """
        Exercita o bloco:

            except Exception as e:
                ...
                raise
        """

        cliente = ClientePNCP()

        with pytest.raises(ValueError):
            cliente.get_contratacoes_publicacao_semestral(
                data_inicial="xxxx",
                data_final="20231231",
                codigo_modalidade_contratacao=8,
            )

    @patch.object(ClientePNCP, "get_contratacoes_publicacao_paginado")
    def test_get_contratacoes_publicacao_semestral_trata_erro_h1(
        self,
        mock_paginado: MagicMock,
    ) -> None:
        """
        Se H1 falhar, o método continua e executa H2.
        """

        mock_paginado.side_effect = [
            RuntimeError("falha H1"),
            [{"id": 2}],
        ]

        cliente = ClientePNCP()

        resultado = cliente.get_contratacoes_publicacao_semestral(
            data_inicial="20230101",
            data_final="20231231",
            codigo_modalidade_contratacao=8,
        )

        assert resultado == [{"id": 2}]
        assert mock_paginado.call_count == 2

    @patch.object(ClientePNCP, "get_contratacoes_publicacao_paginado")
    def test_get_contratacoes_publicacao_semestral_trata_erro_h2(
        self,
        mock_paginado: MagicMock,
    ) -> None:
        """
        Se H2 falhar, mantém dados coletados em H1.
        """

        mock_paginado.side_effect = [
            [{"id": 1}],
            RuntimeError("falha H2"),
        ]

        cliente = ClientePNCP()

        resultado = cliente.get_contratacoes_publicacao_semestral(
            data_inicial="20230101",
            data_final="20231231",
            codigo_modalidade_contratacao=8,
        )

        assert resultado == [{"id": 1}]
        assert mock_paginado.call_count == 2

    @patch.object(ClientePNCP, "get_contratacoes_publicacao_paginado")
    def test_get_contratacoes_publicacao_semestral_h1_ignorado_h2_executado(
        self,
        mock_paginado: MagicMock,
    ) -> None:
        """
        Exercita:

            if s1_ini_clip < s1_fim_clip:
            else:
        """

        mock_paginado.return_value = [{"id": 1}]

        cliente = ClientePNCP()

        resultado = cliente.get_contratacoes_publicacao_semestral(
            data_inicial="20230701",
            data_final="20231231",
            codigo_modalidade_contratacao=8,
        )

        assert resultado == [{"id": 1}]

        mock_paginado.assert_called_once()

    @patch.object(ClientePNCP, "get_contratacoes_publicacao_paginado")
    def test_get_contratacoes_publicacao_semestral_h2_ignorado_h1_executado(
        self,
        mock_paginado: MagicMock,
    ) -> None:
        """
        Exercita:

            if s2_ini_clip < s2_fim_clip:
            else:
        """

        mock_paginado.return_value = [{"id": 1}]

        cliente = ClientePNCP()

        resultado = cliente.get_contratacoes_publicacao_semestral(
            data_inicial="20230101",
            data_final="20230630",
            codigo_modalidade_contratacao=8,
        )

        assert resultado == [{"id": 1}]

        mock_paginado.assert_called_once()

    @patch.object(ClientePNCP, "get_contratacoes_publicacao_paginado")
    def test_get_contratacoes_publicacao_semestral_h1_e_h2_ignorados(
        self,
        mock_paginado: MagicMock,
    ) -> None:
        """
        Nenhuma janela válida.
        """

        cliente = ClientePNCP()

        resultado = cliente.get_contratacoes_publicacao_semestral(
            data_inicial="20231231",
            data_final="20230101",
            codigo_modalidade_contratacao=8,
        )

        assert resultado == []

        mock_paginado.assert_not_called()

    @patch.object(ClientePNCP, "get_contratacoes_publicacao_paginado")
    def test_get_contratacoes_publicacao_semestral_nao_agrega_lista_vazia(
        self,
        mock_paginado: MagicMock,
    ) -> None:
        """
        Exercita o ramo em que page_data é vazio.
        """

        mock_paginado.side_effect = [
            [],
            [],
        ]

        cliente = ClientePNCP()

        resultado = cliente.get_contratacoes_publicacao_semestral(
            data_inicial="20230101",
            data_final="20231231",
            codigo_modalidade_contratacao=8,
        )

        assert resultado == []
        assert mock_paginado.call_count == 2

    # ------------------------------------------------------------------
    # get_itens_e_resultados()
    # ------------------------------------------------------------------

    @patch("cliente_pncp.request_safe")
    def test_get_itens_e_resultados_caminho_feliz(
        self,
        mock_request_safe: MagicMock,
    ) -> None:
        numero_controle = "24855058000185-1-000042/2023"

        itens_api = [
            {
                "numeroItem": 1,
                "descricao": "Servico",
            }
        ]

        resultados_api = [
            {
                "numeroItem": 1,
                "valorTotalHomologado": 6000,
            }
        ]

        mock_request_safe.side_effect = [
            (http.HTTPStatus.OK, itens_api),
            (http.HTTPStatus.OK, 1),
            (http.HTTPStatus.OK, resultados_api),
        ]

        cliente = ClientePNCP()

        itens, resultados = cliente.get_itens_e_resultados([numero_controle])

        assert isinstance(itens, list)
        assert isinstance(resultados, list)

        assert len(itens) == 1
        assert len(resultados) == 1

        assert itens[0]["numeroControlePNCP"] == numero_controle

    @patch("cliente_pncp.request_safe")
    @pytest.mark.parametrize(
        "status,body",
        [
            (http.HTTPStatus.BAD_REQUEST, []),
            (http.HTTPStatus.INTERNAL_SERVER_ERROR, []),
            (http.HTTPStatus.NOT_FOUND, None),
        ],
    )
    def test_get_itens_e_resultados_falha_busca_itens(
        self,
        mock_request_safe: MagicMock,
        status: int,
        body: str,
    ) -> None:
        mock_request_safe.return_value = (
            status,
            body,
        )

        cliente = ClientePNCP()

        itens, resultados = cliente.get_itens_e_resultados(
            ["24855058000185-1-000042/2023"]
        )

        assert itens == []
        assert resultados == []

    @patch("cliente_pncp.request_safe")
    @pytest.mark.parametrize(
        "body",
        [
            {},
            "erro",
            123,
            None,
        ],
    )
    def test_get_itens_e_resultados_itens_tipo_invalido(
        self,
        mock_request_safe: MagicMock,
        body: str,
    ) -> None:
        mock_request_safe.return_value = (
            http.HTTPStatus.OK,
            body,
        )

        cliente = ClientePNCP()

        itens, resultados = cliente.get_itens_e_resultados(
            ["24855058000185-1-000042/2023"]
        )

        assert itens == []
        assert resultados == []

    @patch("cliente_pncp.request_safe")
    def test_get_itens_e_resultados_falha_quantidade(
        self,
        mock_request_safe: MagicMock,
    ) -> None:
        mock_request_safe.side_effect = [
            (
                http.HTTPStatus.OK,
                [{"numeroItem": 1}],
            ),
            (
                http.HTTPStatus.INTERNAL_SERVER_ERROR,
                None,
            ),
        ]

        cliente = ClientePNCP()

        itens, resultados = cliente.get_itens_e_resultados(
            ["24855058000185-1-000042/2023"]
        )

        assert len(itens) == 1
        assert resultados == []

    @patch("cliente_pncp.request_safe")
    @pytest.mark.parametrize(
        "qtd",
        [
            "1",
            {},
            [],
            None,
        ],
    )
    def test_get_itens_e_resultados_quantidade_tipo_invalido(
        self,
        mock_request_safe: MagicMock,
        qtd: typing.Any,
    ) -> None:
        mock_request_safe.side_effect = [
            (
                http.HTTPStatus.OK,
                [{"numeroItem": 1}],
            ),
            (
                http.HTTPStatus.OK,
                qtd,
            ),
        ]

        cliente = ClientePNCP()

        itens, resultados = cliente.get_itens_e_resultados(
            ["24855058000185-1-000042/2023"]
        )

        assert len(itens) == 1
        assert resultados == []

    @patch("cliente_pncp.request_safe")
    def test_get_itens_e_resultados_quantidade_zero(
        self,
        mock_request_safe: MagicMock,
    ) -> None:
        mock_request_safe.side_effect = [
            (
                http.HTTPStatus.OK,
                [{"numeroItem": 1}],
            ),
            (
                http.HTTPStatus.OK,
                0,
            ),
        ]

        cliente = ClientePNCP()

        itens, resultados = cliente.get_itens_e_resultados(
            ["24855058000185-1-000042/2023"]
        )

        assert len(itens) == 1
        assert resultados == []

    @patch("cliente_pncp.request_safe")
    def test_get_itens_e_resultados_falha_resultado_item(
        self,
        mock_request_safe: MagicMock,
    ) -> None:
        mock_request_safe.side_effect = [
            (
                http.HTTPStatus.OK,
                [{"numeroItem": 1}],
            ),
            (
                http.HTTPStatus.OK,
                1,
            ),
            (
                http.HTTPStatus.INTERNAL_SERVER_ERROR,
                None,
            ),
        ]

        cliente = ClientePNCP()

        itens, resultados = cliente.get_itens_e_resultados(
            ["24855058000185-1-000042/2023"]
        )

        assert len(itens) == 1
        assert resultados == []

    @patch("cliente_pncp.request_safe")
    @pytest.mark.parametrize(
        "body",
        [
            {},
            "erro",
            1,
            None,
        ],
    )
    def test_get_itens_e_resultados_resultado_tipo_invalido(
        self,
        mock_request_safe: MagicMock,
        body: str,
    ) -> None:
        mock_request_safe.side_effect = [
            (
                http.HTTPStatus.OK,
                [{"numeroItem": 1}],
            ),
            (
                http.HTTPStatus.OK,
                1,
            ),
            (
                http.HTTPStatus.OK,
                body,
            ),
        ]

        cliente = ClientePNCP()

        itens, resultados = cliente.get_itens_e_resultados(
            ["24855058000185-1-000042/2023"]
        )

        assert len(itens) == 1
        assert resultados == []

    @patch("cliente_pncp.request_safe")
    def test_get_itens_e_resultados_varios_resultados(
        self,
        mock_request_safe: MagicMock,
    ) -> None:
        mock_request_safe.side_effect = [
            (
                http.HTTPStatus.OK,
                [{"numeroItem": 1}],
            ),
            (
                http.HTTPStatus.OK,
                2,
            ),
            (
                http.HTTPStatus.OK,
                [{"resultado": 1}],
            ),
            (
                http.HTTPStatus.OK,
                [{"resultado": 2}],
            ),
        ]

        cliente = ClientePNCP()

        itens, resultados = cliente.get_itens_e_resultados(
            ["24855058000185-1-000042/2023"]
        )

        assert len(itens) == 1
        assert len(resultados) == 2

    @patch("cliente_pncp.request_safe")
    def test_get_itens_e_resultados_propaga_excecao_request_safe(
        self,
        mock_request_safe: MagicMock,
    ) -> None:
        mock_request_safe.side_effect = RuntimeError("Falha de comunicação")

        cliente = ClientePNCP()

        with pytest.raises(RuntimeError):
            cliente.get_itens_e_resultados(["24855058000185-1-000042/2023"])
