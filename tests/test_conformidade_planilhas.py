"""Testes do roteamento das planilhas para as seis tabelas do documento e do
transbordo de colunas para ``payload_origem``."""

import json

from cliente_postgres import ClientPostgresDB
from extracao_planilhas import resolver_tabela_planilha


def test_roteia_contemplados_e_editais_por_politica() -> None:
    assert resolver_tabela_planilha("lpg_contemplados") == "planilha_contemplados_lpg"
    assert resolver_tabela_planilha("lpg_editais") == "planilha_editais_lpg"
    assert (
        resolver_tabela_planilha("raw_pnab_lista_contemplados_pncv")
        == "planilha_contemplados_pnab_ciclo_1"
    )
    assert (
        resolver_tabela_planilha("raw_pnab_acoes_cultura_viva")
        == "planilha_editais_pnab_ciclo_1"
    )


def test_abas_de_proponente_da_pnab_viram_planilha_dados() -> None:
    assert resolver_tabela_planilha("pnab_pessoas") == "planilha_dados_pnab_ciclo_1"
    assert resolver_tabela_planilha("pnab_organizacoes") == "planilha_dados_pnab_ciclo_1"


def test_template_de_dados_lpg_cai_em_planilha_dados_pelo_prefixo() -> None:
    # O nome sai do nome da aba do Excel, então o conjunto é aberto: o que
    # identifica a categoria é o prefixo.
    assert resolver_tabela_planilha("lpg_dados_pessoa_fisica") == "planilha_dados_lpg"
    assert resolver_tabela_planilha("lpg_dados_instrumentos_2_2") == "planilha_dados_lpg"
    assert resolver_tabela_planilha("lpg_dados_aba_nova_qualquer") == "planilha_dados_lpg"


def test_origem_desconhecida_nao_inventa_tabela() -> None:
    assert resolver_tabela_planilha("tabela_de_outro_programa") is None
    assert resolver_tabela_planilha("") is None


def test_colunas_excedentes_vao_para_payload_origem_sem_perder_dado() -> None:
    linhas = [
        {"id_anexo": "123", "nome": "Fulano", "coluna_rara": "x", "outra_rara": "y"},
        {"id_anexo": "123", "nome": "Beltrano", "coluna_rara": "z", "outra_rara": None},
    ]

    movidas = ClientPostgresDB._mover_para_payload_origem(
        linhas, {"coluna_rara", "outra_rara"}
    )

    assert [linha["nome"] for linha in movidas] == ["Fulano", "Beltrano"]
    assert "coluna_rara" not in movidas[0]

    payload = json.loads(movidas[0]["payload_origem"])
    assert payload == {"coluna_rara": "x", "outra_rara": "y"}
    assert json.loads(movidas[1]["payload_origem"])["coluna_rara"] == "z"
