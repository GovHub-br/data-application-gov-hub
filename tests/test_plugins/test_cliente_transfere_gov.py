import http
import logging
from unittest.mock import MagicMock, patch, call
import pytest

from cliente_transferegov_emendas import ClienteTransfereGov


@pytest.fixture
def cliente():
    """Instancia ClienteTransfereGov com o método `request` mockado."""
    with patch.object(ClienteTransfereGov, "__init__", lambda self: None):
        c = ClienteTransfereGov()
        c.request = MagicMock()
        return c


def _ok(data):
    """Atalho para retorno de sucesso."""
    return (http.HTTPStatus.OK, data)


def _err():
    """Atalho para retorno de erro genérico."""
    return (http.HTTPStatus.INTERNAL_SERVER_ERROR, None)


# Dados de exemplo

PROGRAMAS = [{"id_programa": 1, "nome": "Programa A"}, {"id_programa": 2, "nome": "Programa B"}]
PLANOS_ACAO = [{"id_plano_acao": 10, "id_programa": 1}, {"id_plano_acao": 11, "id_programa": 1}]
EXECUTORES = [{"id_executor": 100, "nome": "Executor X"}, {"id_executor": 101, "nome": "Executor Y"}]
EMPENHOS = [{"id_empenho": 200, "valor": 5000.0}, {"id_empenho": 201, "valor": 3000.0}]
DOCS_HABEIS = [{"id_dh": 300, "id_empenho": 200}]
METAS = [{"id_meta": 400, "descricao": "Meta 1"}]
FINALIDADES = [{"id_executor": 100, "finalidade": "Educação"}]
ORDENS_BANCARIAS = [{"id_op_ob": 500, "valor": 1000.0}]
RELATORIOS = [{"id_relatorio_gestao": 600, "status": "aprovado"}]
RELATORIOS_NOVO = [{"id_relatorio_gestao_novo": 700, "status": "pendente"}]
PLANOS_TRABALHO = [{"id_plano_trabalho": 800, "descricao": "Plano 1"}]
HISTORICO_PAGAMENTOS = [{"id_historico_op_ob": 900, "valor": 2000.0}]


class TestGetProgramasEspeciais:
    def test_retorna_lista_com_sucesso(self, cliente):
        cliente.request.return_value = _ok(PROGRAMAS)
        resultado = cliente.get_programas_especiais()
        assert resultado == PROGRAMAS

    def test_passa_params_corretos(self, cliente):
        cliente.request.return_value = _ok(PROGRAMAS)
        cliente.get_programas_especiais(limit=50, offset=100)
        _, kwargs = cliente.request.call_args
        assert kwargs["params"]["limit"] == 50
        assert kwargs["params"]["offset"] == 100
        assert kwargs["params"]["order"] == "id_programa.asc"

    def test_retorna_none_em_erro(self, cliente):
        cliente.request.return_value = _err()
        assert cliente.get_programas_especiais() is None

    def test_retorna_none_quando_data_nao_e_lista(self, cliente):
        cliente.request.return_value = (http.HTTPStatus.OK, {"erro": "inesperado"})
        assert cliente.get_programas_especiais() is None

    def test_retorna_lista_vazia(self, cliente):
        cliente.request.return_value = _ok([])
        assert cliente.get_programas_especiais() == []

class TestGetAllProgramasEspeciais:
    def test_pagina_unica(self, cliente):
        cliente.request.return_value = _ok(PROGRAMAS)
        resultado = cliente.get_all_programas_especiais(page_size=1000)
        assert resultado == PROGRAMAS
        assert cliente.request.call_count == 1

    def test_multiplas_paginas(self, cliente):
        pagina1 = [{"id_programa": i} for i in range(3)]
        pagina2 = [{"id_programa": i} for i in range(3, 5)]
        cliente.request.side_effect = [_ok(pagina1), _ok(pagina2)]
        resultado = cliente.get_all_programas_especiais(page_size=3)
        assert len(resultado) == 5
        assert cliente.request.call_count == 2

    def test_para_quando_retorna_vazio(self, cliente):
        cliente.request.side_effect = [_ok(PROGRAMAS), _ok([])]
        resultado = cliente.get_all_programas_especiais(page_size=1000)
        assert resultado == PROGRAMAS

    def test_para_quando_retorna_none(self, cliente):
        cliente.request.side_effect = [_ok(PROGRAMAS), _err()]
        resultado = cliente.get_all_programas_especiais(page_size=1000)
        assert resultado == PROGRAMAS

    def test_retorna_lista_vazia_sem_dados(self, cliente):
        cliente.request.return_value = _ok([])
        assert cliente.get_all_programas_especiais() == []

class TestGetPlanosAcaoEspeciaisByPrograma:
    def test_retorna_lista_com_sucesso(self, cliente):
        cliente.request.return_value = _ok(PLANOS_ACAO)
        resultado = cliente.get_planos_acao_especiais_by_programa(id_programa=1)
        assert resultado == PLANOS_ACAO

    def test_endpoint_contem_id_programa(self, cliente):
        cliente.request.return_value = _ok(PLANOS_ACAO)
        cliente.get_planos_acao_especiais_by_programa(id_programa=42)
        args, _ = cliente.request.call_args
        assert "id_programa=eq.42" in args[1]

    def test_retorna_none_em_erro(self, cliente):
        cliente.request.return_value = _err()
        assert cliente.get_planos_acao_especiais_by_programa(id_programa=1) is None

    def test_retorna_none_quando_data_nao_e_lista(self, cliente):
        cliente.request.return_value = (http.HTTPStatus.OK, None)
        assert cliente.get_planos_acao_especiais_by_programa(id_programa=1) is None


class TestGetAllPlanosAcaoEspeciaisByPrograma:
    def test_pagina_unica(self, cliente):
        cliente.request.return_value = _ok(PLANOS_ACAO)
        resultado = cliente.get_all_planos_acao_especiais_by_programa(id_programa=1)
        assert resultado == PLANOS_ACAO

    def test_multiplas_paginas(self, cliente):
        p1 = [{"id_plano_acao": i} for i in range(2)]
        p2 = [{"id_plano_acao": 99}]
        cliente.request.side_effect = [_ok(p1), _ok(p2)]
        resultado = cliente.get_all_planos_acao_especiais_by_programa(
            id_programa=1, page_size=2
        )
        assert len(resultado) == 3

    def test_retorna_lista_vazia_sem_dados(self, cliente):
        cliente.request.return_value = _ok([])
        assert cliente.get_all_planos_acao_especiais_by_programa(id_programa=1) == []


class TestGetExecutoresEspeciais:
    def test_retorna_lista_com_sucesso(self, cliente):
        cliente.request.return_value = _ok(EXECUTORES)
        assert cliente.get_executores_especiais() == EXECUTORES

    def test_params_corretos(self, cliente):
        cliente.request.return_value = _ok(EXECUTORES)
        cliente.get_executores_especiais(limit=200, offset=400)
        _, kwargs = cliente.request.call_args
        assert kwargs["params"]["limit"] == 200
        assert kwargs["params"]["offset"] == 400
        assert kwargs["params"]["order"] == "id_executor.asc"

    def test_retorna_none_em_erro(self, cliente):
        cliente.request.return_value = _err()
        assert cliente.get_executores_especiais() is None

    def test_retorna_none_quando_data_nao_e_lista(self, cliente):
        cliente.request.return_value = (http.HTTPStatus.OK, "string_invalida")
        assert cliente.get_executores_especiais() is None


class TestGetAllExecutoresEspeciais:
    def test_pagina_unica(self, cliente):
        cliente.request.return_value = _ok(EXECUTORES)
        assert cliente.get_all_executores_especiais() == EXECUTORES

    def test_multiplas_paginas(self, cliente):
        p1 = [{"id_executor": i} for i in range(3)]
        p2 = [{"id_executor": 99}]
        cliente.request.side_effect = [_ok(p1), _ok(p2)]
        resultado = cliente.get_all_executores_especiais(limit=3)
        assert len(resultado) == 4

    def test_retorna_lista_vazia(self, cliente):
        cliente.request.return_value = _ok([])
        assert cliente.get_all_executores_especiais() == []

class TestGetEmpenhos:
    def test_retorna_lista_com_sucesso(self, cliente):
        cliente.request.return_value = _ok(EMPENHOS)
        assert cliente.get_empenhos_especiais() == EMPENHOS

    def test_params_corretos(self, cliente):
        cliente.request.return_value = _ok(EMPENHOS)
        cliente.get_empenhos_especiais(limit=500, offset=1000)
        _, kwargs = cliente.request.call_args
        assert kwargs["params"]["order"] == "id_empenho.asc"
        assert kwargs["params"]["limit"] == 500
        assert kwargs["params"]["offset"] == 1000

    def test_retorna_none_em_erro(self, cliente):
        cliente.request.return_value = _err()
        assert cliente.get_empenhos_especiais() is None


class TestGetAllEmpenhos:
    def test_pagina_unica(self, cliente):
        cliente.request.return_value = _ok(EMPENHOS)
        assert cliente.get_all_empenhos_especiais() == EMPENHOS

    def test_multiplas_paginas(self, cliente):
        p1 = [{"id_empenho": i} for i in range(3)]
        p2 = [{"id_empenho": i} for i in range(3, 5)]
        cliente.request.side_effect = [_ok(p1), _ok(p2)]
        resultado = cliente.get_all_empenhos_especiais(page_size=3)
        assert len(resultado) == 5

    def test_retorna_lista_vazia(self, cliente):
        cliente.request.return_value = _ok([])
        assert cliente.get_all_empenhos_especiais() == []

class TestGetEmpenhosByPlanoAcao:
    def test_retorna_lista_com_sucesso(self, cliente):
        cliente.request.return_value = _ok(EMPENHOS)
        assert cliente.get_empenhos_especiais_by_plano_acao(id_plano_acao=10) == EMPENHOS

    def test_endpoint_contem_id_plano_acao(self, cliente):
        cliente.request.return_value = _ok(EMPENHOS)
        cliente.get_empenhos_especiais_by_plano_acao(id_plano_acao=55)
        args, _ = cliente.request.call_args
        assert "id_plano_acao=eq.55" in args[1]

    def test_retorna_none_em_erro(self, cliente):
        cliente.request.return_value = _err()
        assert cliente.get_empenhos_especiais_by_plano_acao(id_plano_acao=10) is None


class TestGetAllEmpenhosByPlanoAcao:
    def test_pagina_unica(self, cliente):
        cliente.request.return_value = _ok(EMPENHOS)
        assert cliente.get_all_empenhos_especiais_by_plano_acao(id_plano_acao=10) == EMPENHOS

    def test_multiplas_paginas(self, cliente):
        p1 = [{"id_empenho": i} for i in range(3)]
        p2 = [{"id_empenho": 99}]
        cliente.request.side_effect = [_ok(p1), _ok(p2)]
        resultado = cliente.get_all_empenhos_especiais_by_plano_acao(
            id_plano_acao=10, page_size=3
        )
        assert len(resultado) == 4


class TestGetDocsHabeis:
    def test_retorna_lista_com_sucesso(self, cliente):
        cliente.request.return_value = _ok(DOCS_HABEIS)
        assert cliente.get_documentos_habeis_especiais() == DOCS_HABEIS

    def test_params_corretos(self, cliente):
        cliente.request.return_value = _ok(DOCS_HABEIS)
        cliente.get_documentos_habeis_especiais(limit=100, offset=200)
        _, kwargs = cliente.request.call_args
        assert kwargs["params"]["order"] == "id_dh.asc"

    def test_retorna_none_em_erro(self, cliente):
        cliente.request.return_value = _err()
        assert cliente.get_documentos_habeis_especiais() is None


class TestGetAllDocsHabeis:
    def test_pagina_unica(self, cliente):
        cliente.request.return_value = _ok(DOCS_HABEIS)
        assert cliente.get_all_documentos_habeis_especiais() == DOCS_HABEIS

    def test_multiplas_paginas(self, cliente):
        p1 = [{"id_dh": i} for i in range(3)]
        p2 = [{"id_dh": 99}]
        cliente.request.side_effect = [_ok(p1), _ok(p2)]
        resultado = cliente.get_all_documentos_habeis_especiais(page_size=3)
        assert len(resultado) == 4

    def test_retorna_lista_vazia(self, cliente):
        cliente.request.return_value = _ok([])
        assert cliente.get_all_documentos_habeis_especiais() == []


class TestGetDocsHabeisByEmpenho:
    def test_retorna_lista_com_sucesso(self, cliente):
        cliente.request.return_value = _ok(DOCS_HABEIS)
        assert cliente.get_documentos_habeis_especiais_by_empenho(id_empenho=200) == DOCS_HABEIS

    def test_endpoint_contem_id_empenho(self, cliente):
        cliente.request.return_value = _ok(DOCS_HABEIS)
        cliente.get_documentos_habeis_especiais_by_empenho(id_empenho=77)
        args, _ = cliente.request.call_args
        assert "id_empenho=eq.77" in args[1]

    def test_retorna_none_em_erro(self, cliente):
        cliente.request.return_value = _err()
        assert cliente.get_documentos_habeis_especiais_by_empenho(id_empenho=200) is None


class TestGetAllDocsHabeisByEmpenho:
    def test_pagina_unica(self, cliente):
        cliente.request.return_value = _ok(DOCS_HABEIS)
        assert (
            cliente.get_all_documentos_habeis_especiais_by_empenho(id_empenho=200)
            == DOCS_HABEIS
        )

    def test_multiplas_paginas(self, cliente):
        p1 = [{"id_dh": i} for i in range(2)]
        p2 = [{"id_dh": 99}]
        cliente.request.side_effect = [_ok(p1), _ok(p2)]
        resultado = cliente.get_all_documentos_habeis_especiais_by_empenho(
            id_empenho=200, page_size=2
        )
        assert len(resultado) == 3



class TestGetMetas:
    def test_retorna_lista_com_sucesso(self, cliente):
        cliente.request.return_value = _ok(METAS)
        assert cliente.get_metas_especiais() == METAS

    def test_params_corretos(self, cliente):
        cliente.request.return_value = _ok(METAS)
        cliente.get_metas_especiais()
        _, kwargs = cliente.request.call_args
        assert kwargs["params"]["order"] == "id_meta.asc"

    def test_retorna_none_em_erro(self, cliente):
        cliente.request.return_value = _err()
        assert cliente.get_metas_especiais() is None


class TestGetAllMetas:
    def test_pagina_unica(self, cliente):
        cliente.request.return_value = _ok(METAS)
        assert cliente.get_all_metas_especiais() == METAS

    def test_multiplas_paginas(self, cliente):
        p1 = [{"id_meta": i} for i in range(3)]
        p2 = [{"id_meta": 99}]
        cliente.request.side_effect = [_ok(p1), _ok(p2)]
        resultado = cliente.get_all_metas_especiais(page_size=3)
        assert len(resultado) == 4

    def test_retorna_lista_vazia(self, cliente):
        cliente.request.return_value = _ok([])
        assert cliente.get_all_metas_especiais() == []


class TestGetFinalidades:
    def test_retorna_lista_com_sucesso(self, cliente):
        cliente.request.return_value = _ok(FINALIDADES)
        assert cliente.get_finalidades_especiais() == FINALIDADES

    def test_params_corretos(self, cliente):
        cliente.request.return_value = _ok(FINALIDADES)
        cliente.get_finalidades_especiais()
        _, kwargs = cliente.request.call_args
        assert kwargs["params"]["order"] == "id_executor.asc"

    def test_retorna_none_em_erro(self, cliente):
        cliente.request.return_value = _err()
        assert cliente.get_finalidades_especiais() is None


class TestGetAllFinalidades:
    def test_pagina_unica(self, cliente):
        cliente.request.return_value = _ok(FINALIDADES)
        assert cliente.get_all_finalidades_especiais() == FINALIDADES

    def test_multiplas_paginas(self, cliente):
        p1 = [{"id_executor": i} for i in range(2)]
        p2 = [{"id_executor": 99}]
        cliente.request.side_effect = [_ok(p1), _ok(p2)]
        resultado = cliente.get_all_finalidades_especiais(page_size=2)
        assert len(resultado) == 3

    def test_retorna_lista_vazia(self, cliente):
        cliente.request.return_value = _ok([])
        assert cliente.get_all_finalidades_especiais() == []


class TestGetOrdensBancarias:
    def test_retorna_lista_com_sucesso(self, cliente):
        cliente.request.return_value = _ok(ORDENS_BANCARIAS)
        assert cliente.get_ordens_bancarias_especiais() == ORDENS_BANCARIAS

    def test_params_corretos(self, cliente):
        cliente.request.return_value = _ok(ORDENS_BANCARIAS)
        cliente.get_ordens_bancarias_especiais()
        _, kwargs = cliente.request.call_args
        assert kwargs["params"]["order"] == "id_op_ob.asc"

    def test_retorna_none_em_erro(self, cliente):
        cliente.request.return_value = _err()
        assert cliente.get_ordens_bancarias_especiais() is None


class TestGetAllOrdensBancarias:
    def test_pagina_unica(self, cliente):
        cliente.request.return_value = _ok(ORDENS_BANCARIAS)
        assert cliente.get_all_ordens_bancarias_especiais() == ORDENS_BANCARIAS

    def test_multiplas_paginas(self, cliente):
        p1 = [{"id_op_ob": i} for i in range(3)]
        p2 = [{"id_op_ob": 99}]
        cliente.request.side_effect = [_ok(p1), _ok(p2)]
        resultado = cliente.get_all_ordens_bancarias_especiais(page_size=3)
        assert len(resultado) == 4

    def test_retorna_lista_vazia(self, cliente):
        cliente.request.return_value = _ok([])
        assert cliente.get_all_ordens_bancarias_especiais() == []


class TestGetRelatorios:
    def test_retorna_lista_com_sucesso(self, cliente):
        cliente.request.return_value = _ok(RELATORIOS)
        assert cliente.get_relatorio_gestao_especial() == RELATORIOS

    def test_params_corretos(self, cliente):
        cliente.request.return_value = _ok(RELATORIOS)
        cliente.get_relatorio_gestao_especial()
        _, kwargs = cliente.request.call_args
        assert kwargs["params"]["order"] == "id_relatorio_gestao.asc"

    def test_retorna_none_em_erro(self, cliente):
        cliente.request.return_value = _err()
        assert cliente.get_relatorio_gestao_especial() is None


class TestGetAllRelatorios:
    def test_pagina_unica(self, cliente):
        cliente.request.return_value = _ok(RELATORIOS)
        assert cliente.get_all_relatorio_gestao_especial() == RELATORIOS

    def test_multiplas_paginas(self, cliente):
        p1 = [{"id_relatorio_gestao": i} for i in range(3)]
        p2 = [{"id_relatorio_gestao": 99}]
        cliente.request.side_effect = [_ok(p1), _ok(p2)]
        resultado = cliente.get_all_relatorio_gestao_especial(page_size=3)
        assert len(resultado) == 4

    def test_retorna_lista_vazia(self, cliente):
        cliente.request.return_value = _ok([])
        assert cliente.get_all_relatorio_gestao_especial() == []


class TestGetRelatoriosNovo:
    def test_retorna_lista_com_sucesso(self, cliente):
        cliente.request.return_value = _ok(RELATORIOS_NOVO)
        assert cliente.get_relatorio_gestao_novo_especial() == RELATORIOS_NOVO

    def test_params_corretos(self, cliente):
        cliente.request.return_value = _ok(RELATORIOS_NOVO)
        cliente.get_relatorio_gestao_novo_especial()
        _, kwargs = cliente.request.call_args
        assert kwargs["params"]["order"] == "id_relatorio_gestao_novo.asc"

    def test_retorna_none_em_erro(self, cliente):
        cliente.request.return_value = _err()
        assert cliente.get_relatorio_gestao_novo_especial() is None


class TestGetAllRelatoriosNovo:
    def test_pagina_unica(self, cliente):
        cliente.request.return_value = _ok(RELATORIOS_NOVO)
        assert cliente.get_all_relatorios_gestao_novo_especial() == RELATORIOS_NOVO

    def test_multiplas_paginas(self, cliente):
        p1 = [{"id_relatorio_gestao_novo": i} for i in range(3)]
        p2 = [{"id_relatorio_gestao_novo": 99}]
        cliente.request.side_effect = [_ok(p1), _ok(p2)]
        resultado = cliente.get_all_relatorios_gestao_novo_especial(page_size=3)
        assert len(resultado) == 4

    def test_retorna_lista_vazia(self, cliente):
        cliente.request.return_value = _ok([])
        assert cliente.get_all_relatorios_gestao_novo_especial() == []

class TestGetPlanoTrabalho:
    def test_retorna_lista_com_sucesso(self, cliente):
        cliente.request.return_value = _ok(PLANOS_TRABALHO)
        assert cliente.get_plano_trabalho_especial() == PLANOS_TRABALHO

    def test_params_corretos(self, cliente):
        cliente.request.return_value = _ok(PLANOS_TRABALHO)
        cliente.get_plano_trabalho_especial()
        _, kwargs = cliente.request.call_args
        assert kwargs["params"]["order"] == "id_plano_trabalho.asc"

    def test_retorna_none_em_erro(self, cliente):
        cliente.request.return_value = _err()
        assert cliente.get_plano_trabalho_especial() is None


class TestGetAllPlanoTrabalho:
    def test_pagina_unica(self, cliente):
        cliente.request.return_value = _ok(PLANOS_TRABALHO)
        assert cliente.get_all_plano_trabalho_especial() == PLANOS_TRABALHO

    def test_multiplas_paginas(self, cliente):
        p1 = [{"id_plano_trabalho": i} for i in range(3)]
        p2 = [{"id_plano_trabalho": 99}]
        cliente.request.side_effect = [_ok(p1), _ok(p2)]
        resultado = cliente.get_all_plano_trabalho_especial(page_size=3)
        assert len(resultado) == 4

    def test_retorna_lista_vazia(self, cliente):
        cliente.request.return_value = _ok([])
        assert cliente.get_all_plano_trabalho_especial() == []


class TestGetHistoricoPagamentos:
    def test_retorna_lista_com_sucesso(self, cliente):
        cliente.request.return_value = _ok(HISTORICO_PAGAMENTOS)
        assert cliente.get_historico_pagamentos_especiais() == HISTORICO_PAGAMENTOS

    def test_params_corretos(self, cliente):
        cliente.request.return_value = _ok(HISTORICO_PAGAMENTOS)
        cliente.get_historico_pagamentos_especiais()
        _, kwargs = cliente.request.call_args
        assert kwargs["params"]["order"] == "id_historico_op_ob.asc"

    def test_retorna_none_em_erro(self, cliente):
        cliente.request.return_value = _err()
        assert cliente.get_historico_pagamentos_especiais() is None


class TestGetAllHistoricoPagamentos:
    def test_pagina_unica(self, cliente):
        cliente.request.return_value = _ok(HISTORICO_PAGAMENTOS)
        assert cliente.get_all_historico_pagamentos_especiais() == HISTORICO_PAGAMENTOS

    def test_multiplas_paginas(self, cliente):
        p1 = [{"id_historico_op_ob": i} for i in range(3)]
        p2 = [{"id_historico_op_ob": 99}]
        cliente.request.side_effect = [_ok(p1), _ok(p2)]
        resultado = cliente.get_all_historico_pagamentos_especiais(page_size=3)
        assert len(resultado) == 4

    def test_retorna_lista_vazia(self, cliente):
        cliente.request.return_value = _ok([])
        assert cliente.get_all_historico_pagamentos_especiais() == []


# Testes de integração - fluxo completo de paginação

class TestPaginacaoCompleta:
    """Valida o mecanismo de paginação de forma genérica."""

    def _gerar_paginas(self, total: int, page_size: int):
        """Cria side_effect simulando N páginas completas + 1 parcial."""
        paginas = []
        offset = 0
        while offset < total:
            fatia = min(page_size, total - offset)
            paginas.append(_ok([{"id": offset + i} for i in range(fatia)]))
            offset += fatia
        return paginas

    def test_tres_paginas_completas_mais_parcial(self, cliente):
        side = self._gerar_paginas(total=7, page_size=3)
        cliente.request.side_effect = side
        resultado = cliente.get_all_programas_especiais(page_size=3)
        assert len(resultado) == 7
        assert cliente.request.call_count == 3  # 3 + 3 + 1

    def test_offset_incrementado_corretamente(self, cliente):
        p1 = [{"id": i} for i in range(3)]
        p2 = [{"id": i} for i in range(3, 5)]
        cliente.request.side_effect = [_ok(p1), _ok(p2)]
        cliente.get_all_programas_especiais(page_size=3)

        calls = cliente.request.call_args_list
        assert calls[0][1]["params"]["offset"] == 0
        assert calls[1][1]["params"]["offset"] == 3

class TestInit:
    def test_init_real(self):
        """Cobre linhas 12-13: instancia real do __init__."""
        with patch("cliente_transferegov_emendas.ClienteBase.__init__", return_value=None):
            c = ClienteTransfereGov()
            assert c.BASE_URL == "https://api.transferegov.gestao.gov.br/transferenciasespeciais/"


class TestExtendCobertos:
    def test_all_empenhos_by_plano_acao_pagina_exata(self, cliente):
        """Cobre linha 416: página exatamente cheia seguida de vazia."""
        p1 = [{"id_empenho": i} for i in range(3)]
        p2 = [{"id_empenho": i} for i in range(3, 6)]
        p3 = []
        cliente.request.side_effect = [_ok(p1), _ok(p2), _ok(p3)]
        resultado = cliente.get_all_empenhos_especiais_by_plano_acao(
            id_plano_acao=10, page_size=3
        )
        assert len(resultado) == 6

    def test_all_docs_habeis_by_empenho_pagina_exata(self, cliente):
        """Cobre linha 653: página exatamente cheia seguida de vazia."""
        p1 = [{"id_dh": i} for i in range(3)]
        p2 = [{"id_dh": i} for i in range(3, 6)]
        p3 = []
        cliente.request.side_effect = [_ok(p1), _ok(p2), _ok(p3)]
        resultado = cliente.get_all_documentos_habeis_especiais_by_empenho(
            id_empenho=200, page_size=3
        )
        assert len(resultado) == 6
